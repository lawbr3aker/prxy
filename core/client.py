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

    # Inbound: relay deposits raw dicts (responses from GAS) → Packet objects
    # put_many sets asyncio.Events so wait_for() unblocks immediately
    async def receive(self,
            raw_packets: list[dict]
        ):
        packets = [Packet.deserialize(raw) for raw in raw_packets]
        await self._pool.put_many(packets)
        logger.info(f"received {len(packets)} response(s)")

    # Outbound: stamp, batch, and send a request packet
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


class ProxyRequestHandler(http.server.BaseHTTPRequestHandler):

    # ── CONNECT tunnel ────────────────────────────────────────────────────────
    async def _handle_stream(self
        ):
        self.connection.settimeout(0.1)

        # Assign pid once for the whole tunnel session so all chunks
        # share the same pid and can be matched on the server side
        timestamp = int(time.time() * 1000)
        pid       = IDGenerator.generate(timestamp)

        # Acknowledge the tunnel
        self.send_response(200, 'Connection established')
        self.end_headers()

        while True:
            data = None
            try:
                data = self.connection.recv(4096)
            except TimeoutError:
                pass

            if data:
                packet = Packet(ptype=PacketType.REQUEST | PacketType.STREAM)
                packet.pid = pid
                packet.set('destination', self.path)
                packet.set('b', data)

                await self.server.handler.handle(packet)

            # Wait for a response chunk for this pid
            pool     = await self.server.handler.pool()
            response = await pool.wait_for(pid, timeout=1.0)
            if response:
                body = response.get('b') or b''
                if isinstance(body, str):
                    body = body.encode('latin-1')
                self.connection.sendall(body)

    # ── Plain HTTP ────────────────────────────────────────────────────────────
    async def _handle_request(self
        ):
        content_length = int(self.headers.get('Content-Length', 0))

        packet = Packet(ptype=PacketType.REQUEST)
        packet.set('d', self.path)
        packet.set('m', self.command)
        packet.set('headers', dict(self.headers))
        if content_length > 0:
            packet.set('b', self.rfile.read(content_length))

        await self.server.handler.handle(packet)

        pool     = await self.server.handler.pool()
        response = await pool.wait_for(packet.pid, timeout=30.0)

        if response is None:
            self.send_response(504)
            self.send_header('Content-Length', '0')
            self.end_headers()
            return

        status  = response.get('status') or 200
        headers = response.get('headers') or {}
        body    = response.get('b') or b''
        if isinstance(body, str):
            body = body.encode('latin-1')

        self.send_response(status)
        skip = {'transfer-encoding', 'content-length', 'connection'}
        for k, v in headers.items():
            if k.lower() not in skip:
                self.send_header(k, v)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    # ── Thread→asyncio bridge ─────────────────────────────────────────────────
    # http.server runs each request in a thread; bridge back to the event loop
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
    do_PUT      = _(_handle_request)
    do_DELETE   = _(_handle_request)
    do_PATCH    = _(_handle_request)

    do_CONNECT  = _(_handle_stream)

    def log_message(self, fmt, *args):
        logger.debug(fmt, *args)


class ProxyServer(http.server.ThreadingHTTPServer):
    handler : Handler

    daemon_threads      = True
    allow_reuse_address = True

    def __init__(self,
            relay_cls   : typing.Type[RelayBase],
            addr        : tuple,
            handler_cls : typing.Type[ProxyRequestHandler]
        ):
        super().__init__(addr, handler_cls)
        self.handler = Handler(relay_cls)
        self.loop    = asyncio.get_running_loop()