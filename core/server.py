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


# Dispatcher: makes real HTTP calls on behalf of the server.
#
# Primary path   — relay calls dispatch(raw_dict) directly per packet,
#                  awaits the response Packet, returns it synchronously.
#                  No pool involved.
#
# Fallback path  — relay calls handler.receive() which fills the pool;
#                  _run() drains the pool and calls handler.handle() with
#                  each response so the relay's send() posts them back to GAS.
class Dispatcher:
    def __init__(self, handler: Handler):
        self._handler = handler
        self._session: typing.Optional[aiohttp.ClientSession] = None

    async def start(self
        ):
        self._session = aiohttp.ClientSession()
        # Fallback loop — only active when packets arrive via pool (puller path)
        asyncio.ensure_future(self._run())

    # Primary path entry point — called directly by relay._on_push()
    async def dispatch(self,
            raw: dict
        ) -> typing.Optional[Packet]:
        packet = Packet.deserialize(raw)
        return await self._do_request(packet)

    # Fallback path loop — drains pool filled by handler.receive()
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
            asyncio.ensure_future(self._dispatch_and_handle(packet))

    async def _dispatch_and_handle(self,
            packet: Packet
        ):
        response = await self._do_request(packet)
        if response:
            await self._handler.handle(response)

    # Core HTTP request — shared by both paths
    async def _do_request(self,
            packet: Packet
        ) -> Packet:
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
                    timeout=aiohttp.ClientTimeout(total=25)
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

        return response