import typing
import abc
import bisect
import threading
import time
import asyncio
import logging


# ── Class-aware logger ────────────────────────────────────────────────────────
# Usage inside any class:
#   log = class_logger(__name__, 'ClassName')
# Produces lines like:  2024-01-01 [INFO] core.shared.PacketPool — ...
def class_logger(module: str, cls: str) -> logging.Logger:
    return logging.getLogger(f"{module}.{cls}")


logger = logging.getLogger(__name__)


# ── IDGenerator ───────────────────────────────────────────────────────────────
class IDGenerator:
    _log  = class_logger(__name__, 'IDGenerator')
    _seed = 0
    _lock = threading.Lock()

    @classmethod
    def generate(cls, timestamp: int) -> int:
        with cls._lock:
            cls._seed = (cls._seed + 1) & 0xFFF
            pid       = cls._seed | (timestamp << 12)
        cls._log.debug(f"seed={cls._seed:#05x} ts={timestamp} → pid={pid}")
        return pid


# ── PacketType ────────────────────────────────────────────────────────────────
class PacketType:
    REQUEST  = 1
    RESPONSE = 2
    STREAM   = 4
    CLOSE    = 8    # sender is closing the tunnel
    EOF      = 16   # upstream TCP reached EOF (server → client)

    @staticmethod
    def name(ptype: int) -> str:
        parts = []
        if ptype & PacketType.REQUEST:   parts.append('REQUEST')
        if ptype & PacketType.RESPONSE:  parts.append('RESPONSE')
        if ptype & PacketType.STREAM:    parts.append('STREAM')
        if ptype & PacketType.CLOSE:     parts.append('CLOSE')
        if ptype & PacketType.EOF:       parts.append('EOF')
        return '|'.join(parts) if parts else f'UNKNOWN({ptype})'


# ── Packet ────────────────────────────────────────────────────────────────────
class Packet:
    _log = class_logger(__name__, 'Packet')

    pid       : int
    ptype     : int
    timestamp : int   # ms epoch; used for stream ordering (replaces seq)
    context   : typing.Dict[str, typing.Any]

    def __init__(self, ptype: int):
        self.pid       = None
        self.ptype     = ptype
        self.timestamp = None
        self.context   = {}

    def set(self, k: str, v: typing.Any):
        self.context[k] = v

    def get(self, k: str) -> typing.Any:
        return self.context.get(k)

    def serialize(self) -> dict:
        body = self.get('body')
        if isinstance(body, (bytes, bytearray)):
            body = body.decode('latin-1')
        d = {
            **self.context,
            'pid':       self.pid,
            'ptype':     self.ptype,
            'timestamp': self.timestamp,
            'body':      body,
        }
        self._log.debug(
            f"pid={self.pid} ptype={PacketType.name(self.ptype)} "
            f"ts={self.timestamp} "
            f"dst={self.get('destination')} "
            f"body={len(body) if body else 0}B"
        )
        return d

    @classmethod
    def deserialize(cls, raw: dict) -> 'Packet':
        packet           = cls(ptype=raw.get('ptype', PacketType.REQUEST))
        packet.pid       = raw.get('pid')
        packet.timestamp = raw.get('timestamp')
        for k, v in raw.items():
            if k not in ('pid', 'ptype', 'timestamp'):
                packet.set(k, v)
        cls._log.debug(
            f"pid={packet.pid} ptype={PacketType.name(packet.ptype)} "
            f"ts={packet.timestamp} "
            f"dst={packet.get('destination')} "
            f"body={len(packet.get('body')) if packet.get('body') else 0}B"
        )
        return packet

    def __repr__(self) -> str:
        body = self.get('body')
        return (
            f"Packet(pid={self.pid} ptype={PacketType.name(self.ptype)} "
            f"ts={self.timestamp} body={len(body) if body else 0}B "
            f"dst={self.get('destination')})"
        )


# ── PacketQueue ───────────────────────────────────────────────────────────────
# Batches outbound plain packets within LIMIT_TIMEOUT seconds.
# STREAM packets bypass this — sent immediately to keep tunnel latency low.
class PacketQueue:
    _log          = class_logger(__name__, 'PacketQueue')
    LIMIT_TIMEOUT = 0.05  # seconds

    def __init__(self):
        self._lock     = asyncio.Lock()
        self._loop     = asyncio.get_running_loop()
        self._queue    : list[Packet]                          = []
        self._finished : typing.Optional[asyncio.Event]       = None
        self._timer    : typing.Optional[asyncio.TimerHandle] = None

    async def wait(self):
        async with self._lock:
            if self._finished is None:
                raise RuntimeError("wait() called before enqueue()")
        await self._finished.wait()

    async def enqueue(self, packet: Packet):
        self._log.debug(f"pid={packet.pid} depth={len(self._queue)}")
        async with self._lock:
            if self._finished is None:
                self._finished = asyncio.Event()
                self._timer    = self._loop.call_later(
                    self.LIMIT_TIMEOUT,
                    lambda: asyncio.ensure_future(self._flush())
                )
            elif self._finished.is_set():
                self._log.warning(f"already flushed — dropping pid={packet.pid}")
                return
            self._queue.append(packet)

    async def _flush(self):
        async with self._lock:
            self._log.debug(f"flushing {len(self._queue)} packet(s)")
            if self._finished:
                self._finished.set()


# ── PacketPool ────────────────────────────────────────────────────────────────
# Stores plain (non-stream) response packets.
# wait_for(pid) suspends with zero polling until the exact packet arrives.
class PacketPool:
    _log = class_logger(__name__, 'PacketPool')

    def __init__(self):
        self._lock    = asyncio.Lock()
        self._pool    : list[Packet]             = []
        self._waiters : dict[int, asyncio.Event] = {}

    async def put(self, packet: Packet):
        event = None
        async with self._lock:
            self._pool.append(packet)
            event = self._waiters.get(packet.pid)
        if event:
            self._log.debug(f"waking waiter pid={packet.pid}")
            event.set()

    async def put_many(self, packets: list[Packet]):
        to_wake = []
        async with self._lock:
            self._pool.extend(packets)
            for p in packets:
                ev = self._waiters.get(p.pid)
                if ev:
                    to_wake.append((p.pid, ev))
        for pid, ev in to_wake:
            self._log.debug(f"waking waiter pid={pid}")
            ev.set()

    async def wait_for(self, pid: int, timeout: float = 30.0) -> typing.Optional[Packet]:
        self._log.debug(f"pid={pid} timeout={timeout}s")
        event = asyncio.Event()

        async with self._lock:
            for p in self._pool:
                if p.pid == pid:
                    self._pool.remove(p)
                    self._log.debug(f"pid={pid} found immediately")
                    return p
            self._waiters[pid] = event

        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            self._log.warning(f"pid={pid} timed out after {timeout}s")
            return None
        finally:
            async with self._lock:
                self._waiters.pop(pid, None)

        async with self._lock:
            for p in self._pool:
                if p.pid == pid:
                    self._pool.remove(p)
                    self._log.debug(f"pid={pid} retrieved after event")
                    return p

        self._log.error(f"pid={pid} event fired but packet missing from pool")
        return None

    async def find(self, condition: typing.Callable[[Packet], bool]) -> typing.Optional[Packet]:
        async with self._lock:
            for p in self._pool:
                if condition(p):
                    self._pool.remove(p)
                    return p
        return None


# ── StreamState ───────────────────────────────────────────────────────────────
# Owns the asyncio TCP connection for one CONNECT tunnel on the server side.
class StreamState:
    _log = class_logger(__name__, 'StreamState')

    class Status:
        OPEN    = 'open'
        CLOSING = 'closing'
        CLOSED  = 'closed'

    def __init__(self, pid: int, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.pid    = pid
        self.reader = reader
        self.writer = writer
        self.status = StreamState.Status.OPEN
        self.pump   : typing.Optional[asyncio.Task] = None

    def is_open(self) -> bool:
        return self.status == StreamState.Status.OPEN

    async def close(self):
        if self.status == StreamState.Status.CLOSED:
            return
        self.status = StreamState.Status.CLOSING
        if self.pump and not self.pump.done():
            self.pump.cancel()
            self._log.debug(f"cancelled pump pid={self.pid}")
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except OSError:
            pass
        self.status = StreamState.Status.CLOSED
        self._log.info(f"closed pid={self.pid}")

    def __repr__(self) -> str:
        return f"StreamState(pid={self.pid} status={self.status})"


# ── StreamBuffer ──────────────────────────────────────────────────────────────
# Receives response chunks from the relay, delivers them in timestamp order.
# Uses bisect for O(log n) insert instead of sort-on-every-put.
# Fixed race: event is set after lock release, clear() only inside lock when
# no matching packet exists — no wakeup can be lost.
class StreamBuffer:
    _log = class_logger(__name__, 'StreamBuffer')

    def __init__(self, pid: int):
        self.pid       = pid
        self._lock     = asyncio.Lock()
        self._heap     : list[Packet]  = []   # sorted by timestamp ascending
        self._next_ts  : int           = 0    # minimum acceptable timestamp
        self._event    : asyncio.Event = asyncio.Event()
        self._closed   : bool          = False

    async def put(self, packet: Packet):
        ts   = packet.timestamp or 0
        body = packet.get('body') or b''
        self._log.debug(
            f"pid={self.pid} ts={ts} "
            f"ptype={PacketType.name(packet.ptype)} "
            f"body={len(body) if body else 0}B"
        )
        async with self._lock:
            if self._closed:
                self._log.warning(f"pid={self.pid} closed — dropping ts={ts}")
                return
            # bisect insert keeps heap sorted by timestamp
            keys = [p.timestamp or 0 for p in self._heap]
            idx  = bisect.bisect_right(keys, ts)
            self._heap.insert(idx, packet)
        # Set event AFTER releasing lock — no wakeup can be lost
        self._event.set()

    async def get(self, timeout: float = 30.0) -> typing.Optional[Packet]:
        deadline = time.monotonic() + timeout
        while True:
            async with self._lock:
                # Deliver oldest packet whose timestamp >= _next_ts
                if self._heap:
                    candidate = self._heap[0]
                    ts        = candidate.timestamp or 0
                    if ts >= self._next_ts:
                        self._heap.pop(0)
                        self._next_ts = ts + 1
                        self._log.debug(
                            f"pid={self.pid} delivering ts={ts} "
                            f"ptype={PacketType.name(candidate.ptype)}"
                        )
                        return candidate
                if self._closed:
                    self._log.debug(f"pid={self.pid} closed — returning None")
                    return None
                # Clear only when we know no packet is ready
                self._event.clear()

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._log.warning(f"pid={self.pid} timeout waiting for next chunk")
                return None
            try:
                await asyncio.wait_for(self._event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                self._log.warning(f"pid={self.pid} timeout")
                return None

    async def close(self):
        async with self._lock:
            self._closed = True
        self._event.set()
        self._log.debug(f"pid={self.pid} closed")


# ── StreamRegistry ────────────────────────────────────────────────────────────
# Shared between the relay (puts chunks) and _handle_stream (gets chunks).
class StreamRegistry:
    _log = class_logger(__name__, 'StreamRegistry')

    def __init__(self):
        self._lock    = asyncio.Lock()
        self._buffers : dict[int, StreamBuffer] = {}

    async def get_or_create(self, pid: int) -> StreamBuffer:
        async with self._lock:
            if pid not in self._buffers:
                self._buffers[pid] = StreamBuffer(pid)
                self._log.debug(f"created buffer pid={pid}")
            return self._buffers[pid]

    async def get(self, pid: int) -> typing.Optional[StreamBuffer]:
        async with self._lock:
            return self._buffers.get(pid)

    async def remove(self, pid: int):
        async with self._lock:
            buf = self._buffers.pop(pid, None)
        if buf:
            await buf.close()
            self._log.debug(f"removed and closed pid={pid}")


# ── RelayRequest ──────────────────────────────────────────────────────────────
class RelayRequest:
    def __init__(self, packets: list[Packet], source: str):
        self.packets   = packets
        self.source    = source
        self.timestamp = int(time.time() * 1000)

    def __repr__(self) -> str:
        return (
            f"RelayRequest(source={self.source!r} "
            f"packets={len(self.packets)} "
            f"pids={[p.pid for p in self.packets]})"
        )


# ── HandlerBase ───────────────────────────────────────────────────────────────
# Abstract base shared by client.Handler and server.Handler.
# Concrete classes only differ in: source string, pool/streams exposure,
# and whether they have a StreamRegistry.
class HandlerBase(abc.ABC):
    _log = class_logger(__name__, 'HandlerBase')

    def __init__(self, relay_cls: typing.Type['RelayBase'], source: str):
        self._relay  = relay_cls(self)
        self._source = source   # 'client' or 'server'
        self._lock   = asyncio.Lock()
        self._pool   = PacketPool()
        self._queue  : typing.Optional[PacketQueue] = None

    async def init(self):
        self._log.info(f"starting {self._source} relay")
        await self._relay.start()
        self._log.info(f"{self._source} relay started")

    async def receive(self, raw_packets: list[dict]):
        """Inbound: relay deposits raw dicts → Packet objects."""
        packets = [Packet.deserialize(raw) for raw in raw_packets]
        self._log.info(
            f"receive: {len(packets)} packet(s) "
            f"pids={[p.pid for p in packets]}"
        )
        await self._inbound(packets)

    @abc.abstractmethod
    async def _inbound(self, packets: list[Packet]):
        """Route inbound packets to the right store (pool or stream registry)."""

    async def handle(self, packet: Packet):
        """Outbound: stamp, (optionally batch), and send."""
        packet.timestamp = int(time.time() * 1000)
        if packet.pid is None:
            packet.pid = IDGenerator.generate(packet.timestamp)

        self._log.debug(f"handle: {packet!r}")

        # Stream packets skip the batching window — tunnel latency matters
        if packet.ptype & PacketType.STREAM:
            await self._submit([packet])
            return

        async with self._lock:
            if self._queue is None:
                self._queue = PacketQueue()
            queue = self._queue

        await queue.enqueue(packet)
        await queue.wait()

        batch = None
        async with self._lock:
            if self._queue is queue:
                batch        = queue._queue[:]
                self._queue  = None
                self._log.debug(
                    f"handle: flush winner {len(batch)} packet(s) "
                    f"pids={[p.pid for p in batch]}"
                )
            else:
                self._log.debug(f"handle: flush loser pid={packet.pid} already batched")

        if batch is not None:
            await self._submit(batch)

    async def pool(self) -> PacketPool:
        return self._pool

    async def _submit(self, batch: list[Packet]):
        request = RelayRequest(packets=batch, source=self._source)
        self._log.info(f"submit: {request!r}")
        # Fire-and-forget — never blocks the caller
        asyncio.ensure_future(self._relay.send(request))


# ── RelayBase ─────────────────────────────────────────────────────────────────
class RelayBase(abc.ABC):
    @abc.abstractmethod
    async def start(self): ...

    @abc.abstractmethod
    async def send(self, request: RelayRequest): ...