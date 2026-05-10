import typing
import abc
import heapq
import bisect
import threading
import time
import asyncio
import logging

# Class-aware logger
def class_logger(name: str, cls: str) -> logging.Logger:
    return logging.getLogger(f"{name}.{cls}")

logger = logging.getLogger(__name__)

# ID Generator (same as original)
class IDGenerator:
    _log = class_logger(__name__, 'IDGenerator')
    _seed = 0
    _lock = threading.Lock()

    @classmethod
    def generate(cls, timestamp: int) -> int:
        with cls._lock:
            cls._seed = (cls._seed + 1) & 0xFFF
            pid = cls._seed | (timestamp << 12)
        cls._log.debug(f"seed={cls._seed:#05x} ts={timestamp} → pid={pid}")
        return pid

# PacketType flags
class PacketType:
    REQUEST  = 1
    RESPONSE = 2
    STREAM   = 4
    CLOSE    = 8
    EOF      = 16

    @staticmethod
    def name(ptype: int) -> str:
        parts = []
        if ptype & PacketType.REQUEST:   parts.append('REQUEST')
        if ptype & PacketType.RESPONSE:  parts.append('RESPONSE')
        if ptype & PacketType.STREAM:    parts.append('STREAM')
        if ptype & PacketType.CLOSE:     parts.append('CLOSE')
        if ptype & PacketType.EOF:       parts.append('EOF')
        return '|'.join(parts) if parts else f'UNKNOWN({ptype})'

# Packet (unchanged, but ensure seq field is present for streams)
class Packet:
    _log = class_logger(__name__, 'Packet')

    def __init__(self, ptype: int):
        self.pid = None
        self.ptype = ptype
        self.timestamp = None
        self.seq = None          # for stream chunks ordering
        self.context = {}

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
            'pid': self.pid,
            'ptype': self.ptype,
            'timestamp': self.timestamp,
            'seq': self.seq,
            'body': body,
        }
        self._log.debug(f"serialize: pid={self.pid} ptype={PacketType.name(self.ptype)} "
                        f"seq={self.seq} body={len(body) if body else 0}B")
        return d

    @classmethod
    def deserialize(cls, raw: dict) -> 'Packet':
        packet = cls(ptype=raw.get('ptype', PacketType.REQUEST))
        packet.pid = raw.get('pid')
        packet.timestamp = raw.get('timestamp')
        packet.seq = raw.get('seq')
        for k, v in raw.items():
            if k not in ('pid', 'ptype', 'timestamp', 'seq'):
                packet.set(k, v)
        return packet

    def __repr__(self) -> str:
        body = self.get('body')
        return (f"Packet(pid={self.pid} ptype={PacketType.name(self.ptype)} "
                f"ts={self.timestamp} seq={self.seq} body={len(body) if body else 0}B)")

# PacketQueue (unchanged, only for non‑stream)
class PacketQueue:
    _log = class_logger(__name__, 'PacketQueue')
    LIMIT_TIMEOUT = 0.05

    def __init__(self):
        self._lock = asyncio.Lock()
        self._loop = asyncio.get_running_loop()
        self._queue: list[Packet] = []
        self._finished: typing.Optional[asyncio.Event] = None
        self._timer: typing.Optional[asyncio.TimerHandle] = None

    async def wait(self):
        async with self._lock:
            if self._finished is None:
                raise RuntimeError("wait() called before enqueue()")
        await self._finished.wait()

    async def enqueue(self, packet: Packet):
        async with self._lock:
            if self._finished is None:
                self._finished = asyncio.Event()
                self._timer = self._loop.call_later(
                    self.LIMIT_TIMEOUT,
                    lambda: asyncio.ensure_future(self._flush())
                )
            elif self._finished.is_set():
                self._log.warning(f"already flushed – dropping pid={packet.pid}")
                return
            self._queue.append(packet)

    async def _flush(self):
        async with self._lock:
            if self._finished:
                self._finished.set()

# PacketPool (unchanged)
class PacketPool:
    _log = class_logger(__name__, 'PacketPool')

    def __init__(self):
        self._lock = asyncio.Lock()
        self._pool: list[Packet] = []
        self._waiters: dict[int, asyncio.Event] = {}

    async def put(self, packet: Packet):
        event = None
        async with self._lock:
            self._pool.append(packet)
            event = self._waiters.get(packet.pid)
        if event:
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
            ev.set()

    async def wait_for(self, pid: int, timeout: float = 30.0) -> typing.Optional[Packet]:
        event = asyncio.Event()
        async with self._lock:
            for i, p in enumerate(self._pool):
                if p.pid == pid:
                    self._pool.pop(i)
                    return p
            self._waiters[pid] = event
        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            self._log.warning(f"pid={pid} timeout after {timeout}s")
            return None
        finally:
            async with self._lock:
                self._waiters.pop(pid, None)
        async with self._lock:
            for i, p in enumerate(self._pool):
                if p.pid == pid:
                    self._pool.pop(i)
                    return p
        return None

    async def find(self, condition: typing.Callable[[Packet], bool]) -> typing.Optional[Packet]:
        async with self._lock:
            for i, p in enumerate(self._pool):
                if condition(p):
                    self._pool.pop(i)
                    return p
        return None

# StreamState (server side)
class StreamState:
    _log = class_logger(__name__, 'StreamState')

    class Status:
        OPEN = 'open'
        CLOSING = 'closing'
        CLOSED = 'closed'

    def __init__(self, pid: int, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.pid = pid
        self.reader = reader
        self.writer = writer
        self.status = StreamState.Status.OPEN
        self.pump: typing.Optional[asyncio.Task] = None

    def is_open(self) -> bool:
        return self.status == StreamState.Status.OPEN

    async def close(self):
        if self.status == StreamState.Status.CLOSED:
            return
        self.status = StreamState.Status.CLOSING
        if self.pump and not self.pump.done():
            self.pump.cancel()
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except OSError:
            pass
        self.status = StreamState.Status.CLOSED
        self._log.info(f"closed pid={self.pid}")

# StreamBuffer (client side, fixed get with timeout=0)
class StreamBuffer:
    _log = class_logger(__name__, 'StreamBuffer')

    def __init__(self, pid: int):
        self.pid = pid
        self._lock = asyncio.Lock()
        self._heap = []               # min-heap of (key, counter, packet)
        self._counter = 0             # unique insertion order to break ties
        self._next_seq = 0
        self._event = asyncio.Event()
        self._closed = False

    async def put(self, packet: Packet):
        # Use seq for stream packets, otherwise timestamp
        if packet.ptype & PacketType.STREAM:
            key = packet.seq if packet.seq is not None else 0
        else:
            key = packet.timestamp if packet.timestamp is not None else 0
        async with self._lock:
            if self._closed:
                self._log.warning(f"pid={self.pid} closed – dropping seq={key}")
                return
            # Push with unique counter to avoid comparing Packet objects
            heapq.heappush(self._heap, (key, self._counter, packet))
            self._counter += 1
        self._event.set()

    async def get(self, timeout: float = 30.0) -> typing.Optional[Packet]:
        # Non‑blocking path
        if timeout == 0:
            async with self._lock:
                if self._heap and self._heap[0][0] == self._next_seq:
                    key, _, packet = heapq.heappop(self._heap)
                    self._next_seq = key + 1
                    return packet
                return None

        # Blocking path with timeout
        deadline = time.monotonic() + timeout
        while True:
            async with self._lock:
                if self._heap and self._heap[0][0] == self._next_seq:
                    key, _, packet = heapq.heappop(self._heap)
                    self._next_seq = key + 1
                    return packet
                if self._closed:
                    return None
                self._event.clear()

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._log.warning(f"pid={self.pid} timeout waiting for seq={self._next_seq}")
                return None
            try:
                await asyncio.wait_for(self._event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                self._log.warning(f"pid={self.pid} timeout waiting for seq={self._next_seq}")
                return None

    async def close(self):
        async with self._lock:
            self._closed = True
        self._event.set()

# StreamRegistry
class StreamRegistry:
    _log = class_logger(__name__, 'StreamRegistry')

    def __init__(self):
        self._lock = asyncio.Lock()
        self._buffers: dict[int, StreamBuffer] = {}

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
            self._log.debug(f"removed pid={pid}")

# RelayRequest
class RelayRequest:
    def __init__(self, packets: list[Packet], source: str):
        self.packets = packets
        self.source = source
        self.timestamp = int(time.time() * 1000)

    def __repr__(self) -> str:
        return f"RelayRequest(source={self.source!r} packets={len(self.packets)} pids={[p.pid for p in self.packets]})"

# HandlerBase (abstract)
class HandlerBase(abc.ABC):
    _log = class_logger(__name__, 'HandlerBase')

    def __init__(self, relay_cls: typing.Type['RelayBase'], source: str):
        self._relay = relay_cls(self)
        self._source = source
        self._lock = asyncio.Lock()
        self._pool = PacketPool()
        self._queue: typing.Optional[PacketQueue] = None

    async def init(self):
        await self._relay.start()
        self._log.info(f"{self._source} relay started")

    async def receive(self, raw_packets: list[dict]):
        packets = [Packet.deserialize(raw) for raw in raw_packets]
        self._log.info(f"receive: {len(packets)} packet(s) pids={[p.pid for p in packets]}")
        await self._inbound(packets)

    @abc.abstractmethod
    async def _inbound(self, packets: list[Packet]):
        pass

    async def handle(self, packet: Packet):
        packet.timestamp = int(time.time() * 1000)
        if packet.pid is None:
            packet.pid = IDGenerator.generate(packet.timestamp)

        # Stream packets bypass batching
        # if packet.ptype & PacketType.STREAM:
        #     await self._submit([packet])
        #     return

        async with self._lock:
            if self._queue is None:
                self._queue = PacketQueue()
            queue = self._queue

        await queue.enqueue(packet)
        await queue.wait()

        batch = None
        async with self._lock:
            if self._queue is queue:
                batch = queue._queue[:]
                self._queue = None
        if batch:
            await self._submit(batch)

    async def pool(self) -> PacketPool:
        return self._pool

    async def _submit(self, batch: list[Packet]):
        request = RelayRequest(batch, self._source)
        asyncio.ensure_future(self._relay.send(request))

# RelayBase
class RelayBase(abc.ABC):
    @abc.abstractmethod
    async def start(self): ...
    @abc.abstractmethod
    async def send(self, request: RelayRequest): ...