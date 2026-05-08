import typing
import asyncio
import time

import aiohttp

import logging
#
logger = logging.getLogger(__name__)

from .shared import (
    IDGenerator,
    PacketType,
    Packet,
    PacketQueue,
    PacketPool,
    RelayRequest,
    RelayBase,
)


class Handler:
    def __init__(self, relay_cls: typing.Type[RelayBase]):
        self._relay = relay_cls(self)

        self._lock  = asyncio.Lock()
        self._pool  = PacketPool()

        self._queue: typing.Optional[PacketQueue] = None

    async def init(self
        ):
        await self._relay.start()

    # Inbound: relay puller deposits raw dicts from GAS → Packet objects
    async def receive(self,
            raw_packets: list[dict]
        ):
        packets = [Packet.deserialize(raw) for raw in raw_packets]
        await self._pool.put_many(packets)
        logger.info(f"received {len(packets)} packet(s)")

    # Outbound: batch a response packet and send back to GAS
    async def handle(self,
            packet: Packet
        ):
        packet.timestamp = int(time.time() * 1000)
        if packet.pid is None:
            packet.pid = IDGenerator.generate(packet.timestamp)

        async with self._lock:
            if self._queue is None:
                self._queue = PacketQueue()
            queue = self._queue

        await queue.enqueue(packet)
        await queue.wait()

        async with self._lock:
            if self._queue is queue:
                batch = queue._queue[:]
                self._queue = None

        await self.submit(batch)

    async def pool(self
        ):
        return self._pool

    async def submit(self,
            batch: list[Packet]
        ):
        request = RelayRequest(packets=batch, source='server')
        asyncio.ensure_future(self._relay.send(request))


# Dispatcher: watches the pool for inbound REQUEST packets,
# makes the real HTTP call, passes the response back to handle().
# Mirrors what ProxyRequestHandler does on the client side.
class Dispatcher:
    def __init__(self, handler: Handler):
        self._handler = handler
        self._session: typing.Optional[aiohttp.ClientSession] = None

    async def start(self
        ):
        self._session = aiohttp.ClientSession()
        asyncio.ensure_future(self._run())

    async def _run(self
        ):
        while True:
            pool   = await self._handler.pool()
            packet = await pool.find(
                lambda p: p.ptype & PacketType.REQUEST
            )
            if packet is None:
                await asyncio.sleep(0.05)
                continue
            asyncio.ensure_future(self._dispatch(packet))

    async def _dispatch(self,
            packet: Packet
        ):
        destination = packet.get('d') or packet.get('destination') or ''
        method      = (packet.get('m') or packet.get('method') or 'GET').upper()
        body        = packet.get('b') or packet.get('body')

        if not destination.startswith('http'):
            destination = 'http://' + destination

        logger.info(f"dispatch {method} {destination}")

        try:
            async with self._session.request(
                    method,
                    destination,
                    data=body,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as r:
                body_bytes = await r.read()

                response = Packet(ptype=PacketType.RESPONSE)
                response.pid = packet.pid
                response.set('status',  r.status)
                response.set('b',       body_bytes)
                response.set('headers', dict(r.headers))

        except Exception as e:
            logger.error(f"dispatch error: {e}")
            response = Packet(ptype=PacketType.RESPONSE)
            response.pid = packet.pid
            response.set('status', 502)
            response.set('b',      str(e).encode())

        await self._handler.handle(response)
