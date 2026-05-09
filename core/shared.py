import typing
import abc

import time
import asyncio

import logging
#
logger = logging.getLogger(__name__)


class IDGenerator:
    _seed = 0

    @classmethod
    def generate(cls, timestamp):
        cls._seed = (cls._seed + 1) & 0xFFF
        return cls._seed | (timestamp << 12)

class PacketType:
    REQUEST     = 1
    RESPONSE    = 2
    STREAM      = 4

class Packet:
    pid         : int
    ptype       : PacketType
    timestamp   : int

    context     : typing.Dict[str, typing.Any]

    def __init__(self, ptype):
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
        return {
            **self.context,
            'pid':       self.pid,
            'ptype':     self.ptype,
            'timestamp': self.timestamp,
            'b': self.get('b').decode('latin-1') if isinstance(self.get('b'), bytes) else self.get('b'),
        }

    @classmethod
    def deserialize(cls, raw: dict
        ) -> 'Packet':
        packet           = cls(ptype=raw.get('ptype', PacketType.REQUEST))
        packet.pid       = raw.get('pid')
        packet.timestamp = raw.get('timestamp')
        for k, v in raw.items():
            if k not in ('pid', 'ptype', 'timestamp'):
                packet.set(k, v)
        return packet

class PacketQueue:
    LIMIT_TIMEOUT   = 0.5 # second
    LIMIT_SIZE      = 512 # bytes

    def __init__(self):
        self._lock = asyncio.Lock()
        self._loop = asyncio.get_running_loop()

        self._queue: list[Packet] = []

        self._finished: typing.Optional[asyncio.Event] = None
        self._timer   : typing.Optional[asyncio.TimerHandle] = None

    async def wait(self
        ):
        async with self._lock:
            if self._finished is None:
                raise Exception
        await self._finished.wait()

    async def enqueue(self,
            packet: Packet
        ):
        async with self._lock:
            if self._finished is None:
                self._finished = asyncio.Event()
                self._timer = asyncio.get_running_loop().call_later(
                    PacketQueue.LIMIT_TIMEOUT,
                    lambda: asyncio.ensure_future(self._on_timeout())
                )
            elif self._finished.is_set():
                return

            self._queue.append(packet)

    async def _on_timeout(self
        ):
        async with self._lock:
            if self._finished is None:
                raise Exception
            self._finished.set()

class PacketPool:
    def __init__(self):
        self._lock    = asyncio.Lock()
        self._pool:    list[Packet]             = []
        self._waiters: dict[int, asyncio.Event] = {}  # pid → event

    async def put(self,
            packet: Packet
        ):
        event = None
        async with self._lock:
            self._pool.append(packet)
            event = self._waiters.get(packet.pid)
        if event:
            event.set()

    async def put_many(self,
            packets: list[Packet]
        ):
        events = []
        async with self._lock:
            self._pool.extend(packets)
            for packet in packets:
                event = self._waiters.get(packet.pid)
                if event:
                    events.append(event)
        for event in events:
            event.set()

    # Zero-polling wait — suspends until the exact pid arrives or timeout
    async def wait_for(self,
            pid     : int,
            timeout : float = 30.0
        ) -> typing.Optional[Packet]:
        event = asyncio.Event()

        async with self._lock:
            # already arrived before we registered
            for packet in self._pool:
                if packet.pid == pid:
                    self._pool.remove(packet)
                    return packet
            self._waiters[pid] = event

        try:
            await asyncio.wait_for(event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            return None
        finally:
            async with self._lock:
                self._waiters.pop(pid, None)

        async with self._lock:
            for packet in self._pool:
                if packet.pid == pid:
                    self._pool.remove(packet)
                    return packet
        return None

    async def find(self,
            condition: typing.Callable[Packet, bool]
        ) -> typing.Optional[Packet]:
        async with self._lock:
            for packet in self._pool:
                if condition(packet):
                    self._pool.remove(packet)
                    return packet

class RelayRequest:
    def __init__(self,
            packets : list[Packet],
            source  : str           # 'client' or 'server'
        ):
        self.packets    = packets
        self.source     = source
        self.timestamp  = int(time.time() * 1000)

class RelayBase(abc.ABC):
    @abc.abstractmethod
    async def start(self
        ): ...

    @abc.abstractmethod
    async def send(self,
            request: RelayRequest
        ): ...