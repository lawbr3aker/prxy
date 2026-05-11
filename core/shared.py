import abc
import threading
import time
import asyncio
import typing
import logging


def _logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


class IDGenerator:
    _seed = 0
    _lock = threading.Lock()

    @classmethod
    def generate(cls, timestamp: int) -> int:
        with cls._lock:
            cls._seed = (cls._seed + 1) & 0xFFF
            return cls._seed | (timestamp << 12)


class PacketType:
    REQUEST  = 1
    RESPONSE = 2
    STREAM   = 4
    CLOSE    = 8
    EOF      = 16

    @staticmethod
    def name(t: int) -> str:
        parts = []
        if t & PacketType.REQUEST:  parts.append('REQUEST')
        if t & PacketType.RESPONSE: parts.append('RESPONSE')
        if t & PacketType.STREAM:   parts.append('STREAM')
        if t & PacketType.CLOSE:    parts.append('CLOSE')
        if t & PacketType.EOF:      parts.append('EOF')
        return '|'.join(parts) if parts else f'UNKNOWN({t})'


class Packet:
    def __init__(self, ptype: int):
        self.pid       : typing.Optional[int] = None
        self.ptype     : int                  = ptype
        self.timestamp : typing.Optional[int] = None
        self.seq       : int                  = 0   # per-tunnel monotonic counter for ordering
        self._ctx      : dict                 = {}

    def set(self, k: str, v: typing.Any):
        self._ctx[k] = v

    def get(self, k: str) -> typing.Any:
        return self._ctx.get(k)

    def serialize(self) -> dict:
        body = self._ctx.get('body')
        if isinstance(body, (bytes, bytearray)):
            body = body.decode('latin-1')
        return {
            **self._ctx,
            'pid':       self.pid,
            'ptype':     self.ptype,
            'timestamp': self.timestamp,
            'seq':       self.seq,
            'body':      body,
        }

    @classmethod
    def deserialize(cls, raw: dict) -> 'Packet':
        p           = cls(ptype=raw.get('ptype', PacketType.REQUEST))
        p.pid       = raw.get('pid')
        p.timestamp = raw.get('timestamp')
        p.seq       = raw.get('seq') or 0
        for k, v in raw.items():
            if k not in ('pid', 'ptype', 'timestamp', 'seq'):
                p.set(k, v)
        return p

    def __repr__(self) -> str:
        body = self._ctx.get('body')
        return (f"Packet(pid={self.pid} {PacketType.name(self.ptype)} "
                f"seq={self.seq} body={len(body) if body else 0}B)")


# Batches plain (non-STREAM) outbound packets within LIMIT_TIMEOUT to reduce
# GAS round-trips. STREAM packets bypass this entirely in HandlerBase.handle().
class PacketQueue:
    LIMIT_TIMEOUT = 0.005   # 5 ms — reduced from 20 ms for lower latency

    def __init__(self):
        self._lock     = asyncio.Lock()
        self._loop     = asyncio.get_running_loop()
        self._packets  : list[Packet]                          = []
        self._done     : typing.Optional[asyncio.Event]       = None
        self._timer    : typing.Optional[asyncio.TimerHandle] = None

    async def enqueue(self, packet: Packet):
        async with self._lock:
            if self._done is None:
                self._done  = asyncio.Event()
                self._timer = self._loop.call_later(
                    self.LIMIT_TIMEOUT,
                    lambda: asyncio.ensure_future(self._flush())
                )
            elif self._done.is_set():
                return  # window already closed; caller will retry
            self._packets.append(packet)

    async def wait(self):
        async with self._lock:
            if self._done is None:
                raise RuntimeError("wait() before enqueue()")
        await self._done.wait()

    async def _flush(self):
        async with self._lock:
            if self._done:
                self._done.set()


class PacketPool:
    def __init__(self):
        self._lock    = asyncio.Lock()
        self._packets : list[Packet]             = []
        self._waiters : dict[int, asyncio.Event] = {}

    async def put_many(self, packets: list[Packet]):
        wake = []
        async with self._lock:
            self._packets.extend(packets)
            for p in packets:
                ev = self._waiters.get(p.pid)
                if ev:
                    wake.append(ev)
        for ev in wake:
            ev.set()

    async def wait_for(self, pid: int, timeout: float = 30.0) -> typing.Optional[Packet]:
        ev = asyncio.Event()
        async with self._lock:
            for i, p in enumerate(self._packets):
                if p.pid == pid:
                    self._packets.pop(i)
                    return p
            self._waiters[pid] = ev
        try:
            await asyncio.wait_for(ev.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            async with self._lock:
                self._waiters.pop(pid, None)
        async with self._lock:
            for i, p in enumerate(self._packets):
                if p.pid == pid:
                    self._packets.pop(i)
                    return p
        return None

    async def find(self, cond: typing.Callable[[Packet], bool]) -> typing.Optional[Packet]:
        async with self._lock:
            for i, p in enumerate(self._packets):
                if cond(p):
                    self._packets.pop(i)
                    return p
        return None


# Server-side TCP connection state for one CONNECT tunnel.
class StreamState:
    def __init__(self, pid: int, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.pid    = pid
        self.reader = reader
        self.writer = writer
        self._open  = True
        self.pump   : typing.Optional[asyncio.Task] = None
        self._seq   = 0

    def is_open(self) -> bool:
        return self._open

    def next_seq(self) -> int:
        s       = self._seq
        self._seq += 1
        return s

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


# Client-side ordered receive buffer for one CONNECT tunnel.
# Chunks are delivered in seq order; if the next seq hasn't arrived yet,
# get() waits rather than delivering out-of-order data.
class StreamBuffer:
    def __init__(self, pid: int):
        self.pid       = pid
        self._lock     = asyncio.Lock()
        self._chunks   : list[Packet]  = []   # kept sorted by seq
        self._next_seq : int           = 0
        self._event    : asyncio.Event = asyncio.Event()
        self._closed   : bool          = False

    async def put(self, packet: Packet):
        async with self._lock:
            if self._closed:
                return
            # Insert keeping sort by seq
            lo, hi = 0, len(self._chunks)
            while lo < hi:
                mid = (lo + hi) // 2
                if self._chunks[mid].seq < packet.seq:
                    lo = mid + 1
                else:
                    hi = mid
            self._chunks.insert(lo, packet)
        self._event.set()

    async def get(self, timeout: float = 30.0) -> typing.Optional[Packet]:
        # Non-blocking fast path
        async with self._lock:
            if self._chunks and self._chunks[0].seq == self._next_seq:
                p              = self._chunks.pop(0)
                self._next_seq += 1
                return p
            if self._closed:
                return None
            if timeout == 0:
                return None
            self._event.clear()

        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            try:
                await asyncio.wait_for(self._event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                return None
            async with self._lock:
                if self._chunks and self._chunks[0].seq == self._next_seq:
                    p              = self._chunks.pop(0)
                    self._next_seq += 1
                    return p
                if self._closed:
                    return None
                self._event.clear()

    async def close(self):
        async with self._lock:
            self._closed = True
        self._event.set()


class StreamRegistry:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._bufs : dict[int, StreamBuffer] = {}

    async def get_or_create(self, pid: int) -> StreamBuffer:
        async with self._lock:
            if pid not in self._bufs:
                self._bufs[pid] = StreamBuffer(pid)
            return self._bufs[pid]

    async def remove(self, pid: int):
        async with self._lock:
            buf = self._bufs.pop(pid, None)
        if buf:
            await buf.close()


class RelayRequest:
    def __init__(self, packets: list[Packet], source: str):
        self.packets   = packets
        self.source    = source
        self.timestamp = int(time.time() * 1000)


class HandlerBase(abc.ABC):
    def __init__(self, relay_cls: typing.Type['RelayBase'], source: str):
        self._log    = _logger(f"{__name__}.{self.__class__.__name__}")
        self._relay  = relay_cls(self)
        self._source = source
        self._lock   = asyncio.Lock()
        self._pool   = PacketPool()
        self._queue  : typing.Optional[PacketQueue] = None

    async def init(self):
        await self._relay.start()
        self._log.info(f"{self._source} relay started")

    async def receive(self, raw_packets: list[dict]):
        packets = [Packet.deserialize(r) for r in raw_packets]
        await self._inbound(packets)

    @abc.abstractmethod
    async def _inbound(self, packets: list[Packet]): ...

    async def handle(self, packet: Packet):
        packet.timestamp = int(time.time() * 1000)
        if packet.pid is None:
            packet.pid = IDGenerator.generate(packet.timestamp)

        # STREAM packets must never wait in the batch window —
        # every ms of added latency is felt in the TLS handshake and data flow
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
                batch       = queue._packets[:]
                self._queue = None
        if batch:
            await self._submit(batch)

    async def pool(self) -> PacketPool:
        return self._pool

    async def _submit(self, batch: list[Packet]):
        asyncio.ensure_future(self._relay.send(RelayRequest(batch, self._source)))


class RelayBase(abc.ABC):
    @abc.abstractmethod
    async def start(self): ...

    @abc.abstractmethod
    async def send(self, request: RelayRequest): ...