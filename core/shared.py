import typing
import abc
import threading

import time
import asyncio

import logging
#
logger = logging.getLogger(__name__)


class IDGenerator:
    _seed = 0
    _lock = threading.Lock()

    @classmethod
    def generate(cls, timestamp: int) -> int:
        with cls._lock:
            cls._seed = (cls._seed + 1) & 0xFFF
            pid = cls._seed | (timestamp << 12)
        logger.debug(
            f"IDGenerator.generate: seed={cls._seed:#05x} "
            f"timestamp={timestamp} → pid={pid}"
        )
        return pid


class PacketType:
    # Bits carried in Packet.ptype
    REQUEST     = 1   # client → server request
    RESPONSE    = 2   # server → client response
    STREAM      = 4   # chunk belongs to a CONNECT tunnel

    # Signal bits — combined with STREAM to carry lifecycle events.
    # Kept in ptype (not a separate field) so GAS and serialisation don't
    # need extra keys and the existing bitmask checks still work.
    CLOSE       = 8   # sender is closing the tunnel (either side)
    EOF         = 16  # upstream TCP reached EOF (server → client only)

    @staticmethod
    def name(ptype: int) -> str:
        parts = []
        if ptype & PacketType.REQUEST:   parts.append('REQUEST')
        if ptype & PacketType.RESPONSE:  parts.append('RESPONSE')
        if ptype & PacketType.STREAM:    parts.append('STREAM')
        if ptype & PacketType.CLOSE:     parts.append('CLOSE')
        if ptype & PacketType.EOF:       parts.append('EOF')
        return '|'.join(parts) if parts else f'UNKNOWN({ptype})'


class Packet:
    pid         : int
    ptype       : int
    timestamp   : int
    seq         : int   # per-stream monotonic sequence number for ordering

    context     : typing.Dict[str, typing.Any]

    def __init__(self, ptype: int):
        self.pid        = None
        self.ptype      = ptype
        self.timestamp  = None
        self.seq        = 0
        self.context    = {}

    def set(self,
            k: str,
            v: typing.Any
        ):
        self.context.update({k: v})

    def get(self,
            k: str
        ):
        return self.context.get(k)

    def serialize(self
        ) -> dict:
        b = self.get('b')
        if isinstance(b, bytes):
            b = b.decode('latin-1')

        d = {
            **self.context,
            'pid':       self.pid,
            'ptype':     self.ptype,
            'timestamp': self.timestamp,
            'seq':       self.seq,
            'b':         b,
        }
        logger.debug(
            f"Packet.serialize: pid={self.pid} seq={self.seq} "
            f"ptype={PacketType.name(self.ptype)} "
            f"dst={self.get('d') or self.get('destination')} "
            f"body={len(b) if b else 0}B"
        )
        return d

    @classmethod
    def deserialize(cls, raw: dict
        ) -> 'Packet':
        packet           = cls(ptype=raw.get('ptype', PacketType.REQUEST))
        packet.pid       = raw.get('pid')
        packet.timestamp = raw.get('timestamp')
        packet.seq       = raw.get('seq', 0)
        for k, v in raw.items():
            if k not in ('pid', 'ptype', 'timestamp', 'seq'):
                packet.set(k, v)
        logger.debug(
            f"Packet.deserialize: pid={packet.pid} seq={packet.seq} "
            f"ptype={PacketType.name(packet.ptype)} "
            f"dst={packet.get('d') or packet.get('destination')} "
            f"body={len(packet.get('b')) if packet.get('b') else 0}B"
        )
        return packet

    def __repr__(self):
        b    = self.get('b')
        blen = len(b) if b else 0
        return (
            f"Packet(pid={self.pid} seq={self.seq} "
            f"ptype={PacketType.name(self.ptype)} "
            f"ts={self.timestamp} body={blen}B "
            f"dst={self.get('d') or self.get('destination')})"
        )


# ── PacketQueue ───────────────────────────────────────────────────────────────
# Batches outbound packets into a single relay call within LIMIT_TIMEOUT.
# STREAM packets bypass this entirely — they are sent immediately to avoid
# adding 500 ms latency to every tunnel chunk.
class PacketQueue:
    LIMIT_TIMEOUT   = 0.05  # seconds — short window; reduces relay round trips
                             # without adding noticeable latency to streams

    def __init__(self):
        self._lock      = asyncio.Lock()
        self._loop      = asyncio.get_running_loop()
        self._queue     : list[Packet]                          = []
        self._finished  : typing.Optional[asyncio.Event]       = None
        self._timer     : typing.Optional[asyncio.TimerHandle] = None

    async def wait(self
        ):
        async with self._lock:
            if self._finished is None:
                raise RuntimeError("PacketQueue.wait() called before any enqueue()")
        await self._finished.wait()

    async def enqueue(self,
            packet: Packet
        ):
        logger.debug(f"PacketQueue.enqueue: {packet!r} depth={len(self._queue)}")
        async with self._lock:
            if self._finished is None:
                self._finished = asyncio.Event()
                self._timer    = self._loop.call_later(
                    PacketQueue.LIMIT_TIMEOUT,
                    lambda: asyncio.ensure_future(self._on_timeout())
                )
            elif self._finished.is_set():
                logger.warning(
                    f"PacketQueue.enqueue: already flushed — dropping {packet!r}"
                )
                return
            self._queue.append(packet)

    async def _on_timeout(self
        ):
        async with self._lock:
            if self._finished is None:
                raise RuntimeError("PacketQueue._on_timeout() with no _finished event")
            logger.debug(f"PacketQueue._on_timeout: flushing {len(self._queue)} packet(s)")
            self._finished.set()


# ── PacketPool ────────────────────────────────────────────────────────────────
# Plain REQUEST/RESPONSE: one waiter per pid, consumed exactly once.
# STREAM chunks are routed to StreamRegistry instead.
class PacketPool:
    def __init__(self):
        self._lock    = asyncio.Lock()
        self._pool    : list[Packet]             = []
        self._waiters : dict[int, asyncio.Event] = {}

    async def put(self,
            packet: Packet
        ):
        logger.debug(f"PacketPool.put: {packet!r} pool_size={len(self._pool)}")
        event = None
        async with self._lock:
            self._pool.append(packet)
            event = self._waiters.get(packet.pid)
        if event:
            logger.debug(f"PacketPool.put: waking waiter pid={packet.pid}")
            event.set()

    async def put_many(self,
            packets: list[Packet]
        ):
        logger.debug(
            f"PacketPool.put_many: {len(packets)} packet(s) "
            f"pids={[p.pid for p in packets]}"
        )
        events = []
        async with self._lock:
            self._pool.extend(packets)
            for packet in packets:
                event = self._waiters.get(packet.pid)
                if event:
                    events.append((packet.pid, event))
        for pid, event in events:
            logger.debug(f"PacketPool.put_many: waking waiter pid={pid}")
            event.set()

    async def wait_for(self,
            pid     : int,
            timeout : float = 30.0
        ) -> typing.Optional[Packet]:
        logger.debug(f"PacketPool.wait_for: pid={pid} timeout={timeout}s")
        event = asyncio.Event()

        async with self._lock:
            for packet in self._pool:
                if packet.pid == pid:
                    self._pool.remove(packet)
                    logger.debug(f"PacketPool.wait_for: pid={pid} found immediately")
                    return packet
            self._waiters[pid] = event

        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f"PacketPool.wait_for: pid={pid} timed out after {timeout}s")
            return None
        finally:
            async with self._lock:
                self._waiters.pop(pid, None)

        async with self._lock:
            for packet in self._pool:
                if packet.pid == pid:
                    self._pool.remove(packet)
                    logger.debug(f"PacketPool.wait_for: pid={pid} retrieved after event")
                    return packet

        logger.error(f"PacketPool.wait_for: pid={pid} event fired but packet missing")
        return None

    async def find(self,
            condition: typing.Callable[[Packet], bool]
        ) -> typing.Optional[Packet]:
        async with self._lock:
            for packet in self._pool:
                if condition(packet):
                    self._pool.remove(packet)
                    logger.debug(f"PacketPool.find: matched {packet!r}")
                    return packet
        return None


# ── StreamState ───────────────────────────────────────────────────────────────
# Owns the asyncio TCP connection for one CONNECT tunnel.
# Held in Dispatcher._streams keyed by pid.
class StreamState:
    class Status:
        OPEN    = 'open'
        CLOSING = 'closing'
        CLOSED  = 'closed'

    def __init__(self,
            pid    : int,
            reader : asyncio.StreamReader,
            writer : asyncio.StreamWriter
        ):
        self.pid     = pid
        self.reader  = reader
        self.writer  = writer
        self.status  = StreamState.Status.OPEN
        self.pump    : typing.Optional[asyncio.Task] = None
        # monotonic seq counter — stamped on every outgoing response chunk
        self._seq_lock = asyncio.Lock()
        self._seq      = 0

    async def next_seq(self) -> int:
        async with self._seq_lock:
            s        = self._seq
            self._seq += 1
        return s

    def is_open(self) -> bool:
        return self.status == StreamState.Status.OPEN

    async def close(self
        ):
        if self.status == StreamState.Status.CLOSED:
            return
        self.status = StreamState.Status.CLOSING

        if self.pump and not self.pump.done():
            self.pump.cancel()
            logger.debug(f"StreamState.close: cancelled pump pid={self.pid}")

        try:
            self.writer.close()
            await self.writer.wait_closed()
        except OSError:
            pass

        self.status = StreamState.Status.CLOSED
        logger.info(f"StreamState.close: closed pid={self.pid}")

    def __repr__(self):
        return f"StreamState(pid={self.pid} status={self.status})"


# ── StreamBuffer ──────────────────────────────────────────────────────────────
# One per CONNECT tunnel (keyed by pid in StreamRegistry).
# Holds response chunks pushed by the relay and delivers them in seq order.
# _handle_stream calls get() which suspends until the next in-order chunk
# is available.
class StreamBuffer:
    def __init__(self, pid: int):
        self.pid        = pid
        self._lock      = asyncio.Lock()
        self._heap      : list[Packet]  = []
        self._next_seq  : int           = 0
        self._event     : asyncio.Event = asyncio.Event()
        self._closed    : bool          = False

    async def put(self,
            packet: Packet
        ):
        logger.debug(
            f"StreamBuffer.put: pid={self.pid} seq={packet.seq} "
            f"ptype={PacketType.name(packet.ptype)} "
            f"body={len(packet.get('b') or b'') if packet.get('b') else 0}B"
        )
        async with self._lock:
            if self._closed:
                logger.warning(
                    f"StreamBuffer.put: pid={self.pid} closed — dropping seq={packet.seq}"
                )
                return
            self._heap.append(packet)
            self._heap.sort(key=lambda p: p.seq)
        self._event.set()

    # Returns the next in-order packet, waiting if it hasn't arrived yet.
    # Returns None on timeout or if the buffer is closed.
    async def get(self,
            timeout: float = 30.0
        ) -> typing.Optional[Packet]:
        deadline = time.monotonic() + timeout
        while True:
            async with self._lock:
                if self._heap and self._heap[0].seq == self._next_seq:
                    packet          = self._heap.pop(0)
                    self._next_seq += 1
                    logger.debug(
                        f"StreamBuffer.get: pid={self.pid} "
                        f"delivering seq={packet.seq} "
                        f"ptype={PacketType.name(packet.ptype)}"
                    )
                    return packet
                if self._closed:
                    logger.debug(f"StreamBuffer.get: pid={self.pid} closed — None")
                    return None
                self._event.clear()

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                logger.warning(
                    f"StreamBuffer.get: pid={self.pid} "
                    f"timeout waiting for seq={self._next_seq}"
                )
                return None
            try:
                await asyncio.wait_for(self._event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                logger.warning(
                    f"StreamBuffer.get: pid={self.pid} "
                    f"timeout waiting for seq={self._next_seq}"
                )
                return None

    async def close(self
        ):
        async with self._lock:
            self._closed = True
        self._event.set()
        logger.debug(f"StreamBuffer.close: pid={self.pid}")


# ── StreamRegistry ────────────────────────────────────────────────────────────
# Shared between the relay (puts chunks) and _handle_stream (gets chunks).
# Held on the client Handler so both sides see the same instance.
class StreamRegistry:
    def __init__(self):
        self._lock    = asyncio.Lock()
        self._buffers : dict[int, StreamBuffer] = {}

    async def get_or_create(self,
            pid: int
        ) -> StreamBuffer:
        async with self._lock:
            if pid not in self._buffers:
                self._buffers[pid] = StreamBuffer(pid)
                logger.debug(f"StreamRegistry: created buffer pid={pid}")
            return self._buffers[pid]

    async def get(self,
            pid: int
        ) -> typing.Optional[StreamBuffer]:
        async with self._lock:
            return self._buffers.get(pid)

    async def remove(self,
            pid: int
        ):
        async with self._lock:
            buf = self._buffers.pop(pid, None)
        if buf:
            await buf.close()
            logger.debug(f"StreamRegistry: removed and closed buffer pid={pid}")


class RelayRequest:
    def __init__(self,
            packets : list[Packet],
            source  : str           # 'client' or 'server'
        ):
        self.packets   = packets
        self.source    = source
        self.timestamp = int(time.time() * 1000)

    def __repr__(self):
        return (
            f"RelayRequest(source={self.source!r} "
            f"packets={len(self.packets)} "
            f"pids={[p.pid for p in self.packets]})"
        )


class RelayBase(abc.ABC):
    @abc.abstractmethod
    async def start(self
        ): ...

    @abc.abstractmethod
    async def send(self,
            request: RelayRequest
        ): ...
