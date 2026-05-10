import typing
import abc
import threading
import time
import asyncio
import logging


def class_logger(name: str, cls: str) -> logging.Logger:
    return logging.getLogger(f"{name}.{cls}")


class IDGenerator:
    _log  = class_logger(__name__, 'IDGenerator')
    _seed = 0
    _lock = threading.Lock()

    @classmethod
    def generate(cls, timestamp: int) -> int:
        with cls._lock:
            cls._seed = (cls._seed + 1) & 0xFFF
            pid = cls._seed | (timestamp << 12)
        return pid


class PacketType:
    REQUEST  = 1
    RESPONSE = 2
    STREAM   = 4
    CLOSE    = 8
    EOF      = 16

    @staticmethod
    def name(ptype: int) -> str:
        parts = []
        if ptype & PacketType.REQUEST:  parts.append('REQUEST')
        if ptype & PacketType.RESPONSE: parts.append('RESPONSE')
        if ptype & PacketType.STREAM:   parts.append('STREAM')
        if ptype & PacketType.CLOSE:    parts.append('CLOSE')
        if ptype & PacketType.EOF:      parts.append('EOF')
        return '|'.join(parts) if parts else f'UNKNOWN({ptype})'


class Packet:
    _log = class_logger(__name__, 'Packet')

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
        return {
            **self.context,
            'pid':       self.pid,
            'ptype':     self.ptype,
            'timestamp': self.timestamp,
            'body':      body,
        }

    @classmethod
    def deserialize(cls, raw: dict) -> 'Packet':
        p           = cls(ptype=raw.get('ptype', PacketType.REQUEST))
        p.pid       = raw.get('pid')
        p.timestamp = raw.get('timestamp')
        for k, v in raw.items():
            if k not in ('pid', 'ptype', 'timestamp'):
                p.set(k, v)
        return p

    def __repr__(self) -> str:
        body = self.get('body')
        return (f"Packet(pid={self.pid} {PacketType.name(self.ptype)} "
                f"ts={self.timestamp} body={len(body) if body else 0}B)")


# Batches plain (non-stream) outbound packets within LIMIT_TIMEOUT.
# Stream packets bypass this — see HandlerBase.handle().
class PacketQueue:
    _log          = class_logger(__name__, 'PacketQueue')
    LIMIT_TIMEOUT = 0.05  # 50ms — low latency over GAS

    def __init__(self):
        self._lock     = asyncio.Lock()
        self._loop     = asyncio.get_running_loop()
        self._queue    : list[Packet]                          = []
        self._finished : typing.Optional[asyncio.Event]       = None
        self._timer    : typing.Optional[asyncio.TimerHandle] = None

    async def wait(self):
        async with self._lock:
            if self._finished is None:
                raise RuntimeError("wait() before enqueue()")
        await self._finished.wait()

    async def enqueue(self, packet: Packet):
        async with self._lock:
            if self._finished is None:
                self._finished = asyncio.Event()
                self._timer    = self._loop.call_later(
                    self.LIMIT_TIMEOUT,
                    lambda: asyncio.ensure_future(self._flush())
                )
            elif self._finished.is_set():
                self._log.warning(f"already flushed, dropping pid={packet.pid}")
                return
            self._queue.append(packet)

    async def _flush(self):
        async with self._lock:
            if self._finished:
                self._finished.set()


class PacketPool:
    _log = class_logger(__name__, 'PacketPool')

    def __init__(self):
        self._lock    = asyncio.Lock()
        self._pool    : list[Packet]             = []
        self._waiters : dict[int, asyncio.Event] = {}

    async def put_many(self, packets: list[Packet]):
        to_wake = []
        async with self._lock:
            self._pool.extend(packets)
            for p in packets:
                ev = self._waiters.get(p.pid)
                if ev:
                    to_wake.append(ev)
        for ev in to_wake:
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


# Server-side TCP connection for one CONNECT tunnel.
class StreamState:
    _log = class_logger(__name__, 'StreamState')

    def __init__(self, pid: int, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.pid    = pid
        self.reader = reader
        self.writer = writer
        self._open  = True
        self.pump   : typing.Optional[asyncio.Task] = None

    def is_open(self) -> bool:
        return self._open

    async def close(self):
        if not self._open:
            return
        self._open = False
        if self.pump and not self.pump.done():
            self.pump.cancel()
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except OSError:
            pass
        self._log.debug(f"closed pid={self.pid}")


# Client-side buffer for one CONNECT tunnel.
# Delivers chunks ordered by timestamp.
# Does NOT stall waiting for a specific sequence number — delivers whatever
# is oldest in the buffer, preventing hangs from GAS reordering.
class StreamBuffer:
    _log = class_logger(__name__, 'StreamBuffer')

    def __init__(self, pid: int):
        self.pid     = pid
        self._lock   = asyncio.Lock()
        self._chunks : list[Packet] = []   # sorted by timestamp ascending
        self._event  = asyncio.Event()
        self._closed = False

    async def put(self, packet: Packet):
        ts = packet.timestamp or 0
        async with self._lock:
            if self._closed:
                return
            # Insert sorted by timestamp
            idx = len(self._chunks)
            for i, c in enumerate(self._chunks):
                if (c.timestamp or 0) > ts:
                    idx = i
                    break
            self._chunks.insert(idx, packet)
        self._event.set()

    async def get(self, timeout: float = 30.0) -> typing.Optional[Packet]:
        if timeout == 0:
            async with self._lock:
                return self._chunks.pop(0) if self._chunks else None

        deadline = time.monotonic() + timeout
        while True:
            async with self._lock:
                if self._chunks:
                    return self._chunks.pop(0)
                if self._closed:
                    return None
                self._event.clear()

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            try:
                await asyncio.wait_for(self._event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                return None

    async def close(self):
        async with self._lock:
            self._closed = True
        self._event.set()


class StreamRegistry:
    _log = class_logger(__name__, 'StreamRegistry')

    def __init__(self):
        self._lock    = asyncio.Lock()
        self._buffers : dict[int, StreamBuffer] = {}

    async def get_or_create(self, pid: int) -> StreamBuffer:
        async with self._lock:
            if pid not in self._buffers:
                self._buffers[pid] = StreamBuffer(pid)
            return self._buffers[pid]

    async def get(self, pid: int) -> typing.Optional[StreamBuffer]:
        async with self._lock:
            return self._buffers.get(pid)

    async def remove(self, pid: int):
        async with self._lock:
            buf = self._buffers.pop(pid, None)
        if buf:
            await buf.close()


class RelayRequest:
    def __init__(self, packets: list[Packet], source: str):
        self.packets   = packets
        self.source    = source
        self.timestamp = int(time.time() * 1000)

    def __repr__(self) -> str:
        return (f"RelayRequest(source={self.source!r} "
                f"n={len(self.packets)} "
                f"pids={[p.pid for p in self.packets]})")


class HandlerBase(abc.ABC):
    _log = class_logger(__name__, 'HandlerBase')

    def __init__(self, relay_cls: typing.Type['RelayBase'], source: str):
        self._relay  = relay_cls(self)
        self._source = source
        self._lock   = asyncio.Lock()
        self._pool   = PacketPool()
        self._queue  : typing.Optional[PacketQueue] = None

    async def init(self):
        await self._relay.start()
        self._log.info(f"{self._source} relay started")

    async def receive(self, raw_packets: list[dict]):
        packets = [Packet.deserialize(raw) for raw in raw_packets]
        self._log.debug(f"receive {len(packets)} pids={[p.pid for p in packets]}")
        await self._inbound(packets)

    @abc.abstractmethod
    async def _inbound(self, packets: list[Packet]): ...

    async def handle(self, packet: Packet):
        packet.timestamp = int(time.time() * 1000)
        if packet.pid is None:
            packet.pid = IDGenerator.generate(packet.timestamp)

        # Stream packets skip batching — every ms of delay counts in TLS handshake
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
                batch       = queue._queue[:]
                self._queue = None
        if batch:
            await self._submit(batch)

    async def pool(self) -> PacketPool:
        return self._pool

    async def _submit(self, batch: list[Packet]):
        # Fire-and-forget — never blocks the caller
        asyncio.ensure_future(self._relay.send(RelayRequest(batch, self._source)))


class RelayBase(abc.ABC):
    @abc.abstractmethod
    async def start(self): ...

    @abc.abstractmethod
    async def send(self, request: RelayRequest): ...
