import typing
import asyncio
import threading
import time

import http.server

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

    # Outbound: batch a request packet and send to GAS
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
        request = RelayRequest(packets=batch, source='client')
        asyncio.ensure_future(self._relay.send(request))


class ProxyRequestHandler(http.server.SimpleHTTPRequestHandler):
    async def _handle_stream(self
        ):
        self.connection.settimeout(0.1)

        pid = None
        while True:
            try:
                data = self.connection.recv(4096)

                packet = Packet(ptype=PacketType.REQUEST | PacketType.STREAM)
                packet.set('destination', self.path)
                packet.set('b', data)
                packet.pid = pid

                await self.server.handler.handle(packet)
            except TimeoutError:
                pass

            async def poll_for_response():
                pool = await self.server.handler.pool()
                while True:
                    response = await pool.find(
                        lambda p: p.pid == packet.pid and p.ptype & PacketType.RESPONSE
                    )
                    if response:
                        return response
                    await asyncio.sleep(0.05)

            try:
                response = await asyncio.wait_for(
                    poll_for_response(),
                    timeout=1
                )
            except asyncio.TimeoutError:
                pass
            else:
                self.connection.sendall(response.get('b'))

    async def _handle_request(self
        ):
        packet = Packet(ptype=PacketType.REQUEST)
        packet.set('d', self.path)
        packet.set('m', self.command)
        if (l := int(self.headers.get('Content-Length', 0))) > 0:
            packet.set('b', self.rfile.read(l))

        await self.server.handler.handle(packet)

        async def poll_for_response():
            pool = await self.server.handler.pool()
            while True:
                response = await pool.find(
                    lambda p: p.pid == packet.pid and p.ptype & PacketType.RESPONSE
                )
                if response:
                    return response
                await asyncio.sleep(0.05)

        try:
            response = await asyncio.wait_for(
                poll_for_response(),
                timeout=20
            )
        except asyncio.TimeoutError:
            self.send_response(504)
        else:
            self.send_response(response.get('status') or 200)

    @staticmethod
    def _(coroutine):
        def callback(self):
            future = asyncio.run_coroutine_threadsafe(
                coroutine(self),
                self.server.loop
            )
            future.result()
        return callback

    do_GET      = _(_handle_request)
    do_POST     = _(_handle_request)
    do_OPTIONS  = _(_handle_request)
    do_HEAD     = _(_handle_request)

    do_CONNECT  = _(_handle_stream)


class ProxyServer(http.server.ThreadingHTTPServer):
    handler : Handler

    daemon_threads      = True
    allow_reuse_address = True

    def __init__(self, relay_cls: typing.Type[RelayBase], *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.handler = Handler(relay_cls)
        self.loop    = asyncio.get_running_loop()
