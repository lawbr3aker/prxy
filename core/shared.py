import typing
import abc

import time
import asyncio

import logging
#
logger = logging.getLogger(__name__)


class IDGenerator:
    _seed = 0
    _lock = threading.Lock() if False else None  # noqa — replaced below

    @classmethod
    def generate(cls, timestamp):
        cls._seed = (cls._seed + 1) & 0xFFF
        pid = cls._seed | (timestamp << 12)
        logger.debug(f"IDGenerator.generate: seed={cls._seed:#05x} timestamp={timestamp} → pid={pid}")
        return pid

# Thread-safe seed — IDGenerator.generate() is called from HTTP threads
import threading
IDGenerator._lock = threading.Lock()
_orig_generate    = IDGenerator.generate.__func__

def _safe_generate(cls, timestamp):
    with cls._lock:
        cls._seed = (cls._seed + 1) & 0xFFF
        pid = cls._seed | (timestamp << 12)
    logger.debug(f"IDGenerator.generate: seed={cls._seed:#05x} timestamp={timestamp} → pid={pid}")
    return pid

IDGenerator.generate = classmethod(_safe_generate)


class PacketType:
    REQUEST     = 1
    RESPONSE    = 2
    STREAM      = 4

    @staticmethod
    def name(ptype: int) -> str:
        parts = []
        if ptype & PacketType.REQUEST:   parts.append('REQUEST')
        if ptype & PacketType.RESPONSE:  parts.append('RESPONSE')
        if ptype & PacketType.STREAM:    parts.append('STREAM')
        return '|'.join(parts) if parts else f'UNKNOWN({ptype})'


class Packet:
    pid         : int
    ptype       : int
    timestamp   : int

    context     : typing.Dict[str, typing.Any]

    def __init__(self, ptype: int):
        self.pid        = None
        self.ptype      = ptype
        self.timestamp  = None
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
            'b':         b,
        }
        logger.debug(
            f"Packet.serialize: pid={self.pid} ptype={PacketType.name(self.ptype)} "
            f"destination={self.get('d') or self.get('destination')} "
            f"body_len={len(b) if b else 0}"
        )
        return d

    @classmethod
    def deserialize(cls, raw: dict
        ) -> 'Packet':
        packet           = cls(ptype=raw.get('ptype', PacketType.REQUEST))
        packet.pid       = raw.get('pid')
        packet.timestamp = raw.get('timestamp')
        for k, v in raw.items():
            if k not in ('pid', 'ptype', 'timestamp'):
                packet.set(k, v)
        logger.debug(
            f"Packet.deserialize: pid={packet.pid} ptype={PacketType.name(packet.ptype)} "
            f"destination={packet.get('d') or packet.get('destination')} "
            f"body_len={len(packet.get('b')) if packet.get('b') else 0}"
        )
        return packet

    def __repr__(self):
        b    = self.get('b')
        blen = len(b) if b else 0
        return (
            f"Packet(pid={self.pid} ptype={PacketType.name(self.ptype)} "
            f"ts={self.timestamp} body={blen}B "
            f"dst={self.get('d') or self.get('destination')})"
        )


class PacketQueue:
    LIMIT_TIMEOUT   = 0.5  # seconds — flush window
    LIMIT_SIZE      = 512  # bytes   — unused for now, reserved

    def __init__(self):
        self._lock      = asyncio.Lock()
        self._loop      = asyncio.get_running_loop()
        self._queue     : list[Packet]                  = []
        self._finished  : typing.Optional[asyncio.Event]       = None
        self._timer     : typing.Optional[asyncio.TimerHandle] = None

    async def wait(self
        ):
        async with self._lock:
            if self._finished is None:
                # Queue was never used — nothing to wait for
                raise RuntimeError("PacketQueue.wait() called before any enqueue()")
        await self._finished.wait()

    async def enqueue(self,
            packet: Packet
        ):
        logger.debug(f"PacketQueue.enqueue: {packet!r} queue_depth={len(self._queue)}")
        async with self._lock:
            if self._finished is None:
                self._finished = asyncio.Event()
                self._timer    = self._loop.call_later(
                    PacketQueue.LIMIT_TIMEOUT,
                    lambda: asyncio.ensure_future(self._on_timeout())
                )
            elif self._finished.is_set():
                logger.warning(
                    f"PacketQueue.enqueue: queue already flushed — dropping {packet!r}"
                )
                return

            self._queue.append(packet)

    async def _on_timeout(self
        ):
        async with self._lock:
            if self._finished is None:
                raise RuntimeError("PacketQueue._on_timeout() with no _finished event")
            logger.debug(
                f"PacketQueue._on_timeout: flushing {len(self._queue)} packet(s)"
            )
            self._finished.set()


class PacketPool:
    def __init__(self):
        self._lock    = asyncio.Lock()
        self._pool    : list[Packet]             = []
        self._waiters : dict[int, asyncio.Event] = {}  # pid → event

    async def put(self,
            packet: Packet
        ):
        logger.debug(f"PacketPool.put: {packet!r} pool_size={len(self._pool)}")
        event = None
        async with self._lock:
            self._pool.append(packet)
            event = self._waiters.get(packet.pid)
        if event:
            logger.debug(f"PacketPool.put: waking waiter for pid={packet.pid}")
            event.set()

    async def put_many(self,
            packets: list[Packet]
        ):
        logger.debug(
            f"PacketPool.put_many: depositing {len(packets)} packet(s) "
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
            logger.debug(f"PacketPool.put_many: waking waiter for pid={pid}")
            event.set()

    # Zero-polling wait — suspends until the exact pid arrives or timeout
    async def wait_for(self,
            pid     : int,
            timeout : float = 30.0
        ) -> typing.Optional[Packet]:
        logger.debug(f"PacketPool.wait_for: pid={pid} timeout={timeout}s")
        event = asyncio.Event()

        async with self._lock:
            # Already arrived before we registered
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
