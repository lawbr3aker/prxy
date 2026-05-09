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
        logger.info("Handler.init: starting relay")
        await self._relay.start()
        logger.info("Handler.init: relay started")

    # Inbound: relay deposits raw dicts (responses from GAS) → Packet objects.
    # put_many sets asyncio.Events so wait_for() unblocks immediately.
    async def receive(self,
            raw_packets: list[dict]
        ):
        packets = [Packet.deserialize(raw) for raw in raw_packets]
        logger.info(
            f"Handler.receive: depositing {len(packets)} packet(s) into pool "
            f"pids={[p.pid for p in packets]}"
        )
        await self._pool.put_many(packets)

    # Outbound: stamp, batch, and send a request packet.
    # Returns True if this coroutine was the one to flush the queue, False otherwise.
    async def handle(self,
            packet: Packet
        ):
        packet.timestamp = int(time.time() * 1000)
        if packet.pid is None:
            packet.pid = IDGenerator.generate(packet.timestamp)

        logger.debug(f"Handler.handle: {packet!r}")

        async with self._lock:
            if self._queue is None:
                logger.debug("Handler.handle: creating new PacketQueue")
                self._queue = PacketQueue()
            queue = self._queue

        await queue.enqueue(packet)
        await queue.wait()

        # Only the coroutine that still sees its own queue flushes.
        # Others lost the race — their packets are already in the batch.
        payload = None
        async with self._lock:
            if self._queue is queue:
                payload      = queue._queue[:]
                self._queue  = None
                logger.debug(
                    f"Handler.handle: flush winner — {len(payload)} packet(s) "
                    f"pids={[p.pid for p in payload]}"
                )
            else:
                logger.debug(
                    f"Handler.handle: flush loser — pid={packet.pid} already batched"
                )

        if payload is not None:
            await self.submit(payload)

    async def pool(self
        ) -> PacketPool:
        return self._pool

    async def submit(self,
            batch: list[Packet]
        ):
        request = RelayRequest(packets=batch, source='client')
        logger.info(f"Handler.submit: dispatching {request!r}")
        asyncio.ensure_future(self._relay.send(request))


class ProxyRequestHandler(http.server.BaseHTTPRequestHandler):

    # ── CONNECT tunnel ────────────────────────────────────────────────────────
    async def _handle_stream(self
        ):
        logger.info(f"_handle_stream: CONNECT {self.path}")
        self.connection.settimeout(0.1)

        # Acknowledge the tunnel before any IO
        try:
            self.send_response(200, 'Connection established')
            self.end_headers()
        except (ConnectionResetError, BrokenPipeError, OSError) as exc:
            logger.warning(
                f"_handle_stream: CONNECT ack failed for {self.path} — {exc}"
            )
            return

        # pid is shared across all chunks so server can reassemble the stream
        pid = None

        while True:
            # ── Read from the browser ─────────────────────────────────────────
            data = None
            import socket
            try:
                data = self.connection.recv(4096)
                logger.debug(
                    f"_handle_stream: recv pid={pid} path={self.path} "
                    f"bytes={len(data)} hash={hash(data)}"
                )
            except (socket.timeout, TimeoutError) as exc:
                # No data yet — fall through to poll for a server response
                pass
            except ConnectionResetError as exc:
                logger.info(
                    f"_handle_stream: client reset pid={pid} path={self.path} — {exc}"
                )
                # Notify server of close if we have an established pid
                if pid is not None:
                    await self._send_close_packet(pid)
                return
            except (BrokenPipeError, OSError) as exc:
                logger.info(
                    f"_handle_stream: socket error pid={pid} path={self.path} — {exc}"
                )
                if pid is not None:
                    await self._send_close_packet(pid)
                return

            if data is not None:
                if not data:
                    # Zero-byte read — browser closed the connection cleanly
                    logger.info(
                        f"_handle_stream: client EOF pid={pid} path={self.path}"
                    )
                    if pid is not None:
                        await self._send_close_packet(pid)
                    return

                packet = Packet(ptype=PacketType.REQUEST | PacketType.STREAM)
                packet.pid = pid  # None on first chunk — IDGenerator fills it
                packet.set('destination', self.path)
                packet.set('b', data)

                logger.debug(
                    f"_handle_stream: → relay pid={pid} path={self.path} "
                    f"bytes={len(data)}"
                )
                await self.server.handler.handle(packet)

                if pid is None:
                    pid = packet.pid
                    logger.info(
                        f"_handle_stream: tunnel established pid={pid} path={self.path}"
                    )

            # ── Poll for a server response chunk ──────────────────────────────
            if pid is None:
                # Haven't sent the first packet yet — skip polling
                continue

            pool     = await self.server.handler.pool()
            response = await pool.wait_for(pid, timeout=1.0)

            if response is None:
                # Timeout — keep looping; nothing to write yet
                continue

            body = response.get('b') or b''
            if isinstance(body, str):
                body = body.encode('latin-1')

            logger.debug(
                f"_handle_stream: ← server pid={pid} ts={response.timestamp} "
                f"bytes={len(body)} hash={hash(body)}"
            )

            if body == b'EOF':
                # Empty body is the server's close signal
                logger.info(
                    f"_handle_stream: server EOF pid={pid} path={self.path}"
                )
                return

            try:
                self.connection.sendall(body)
                logger.debug(
                    f"_handle_stream: sent to client pid={pid} bytes={len(body)}"
                )
            except ConnectionResetError as exc:
                logger.info(
                    f"_handle_stream: client reset on write pid={pid} — {exc}"
                )
                return
            except (BrokenPipeError, OSError) as exc:
                logger.info(
                    f"_handle_stream: write error pid={pid} — {exc}"
                )
                return

    # Send a zero-byte STREAM packet so the server knows to close the tunnel
    async def _send_close_packet(self,
            pid: int
        ):
        logger.info(f"_handle_stream: sending close packet pid={pid}")
        packet = Packet(ptype=PacketType.REQUEST | PacketType.STREAM)
        packet.pid = pid
        packet.set('b', b'EOF')
        await self.server.handler.handle(packet)

    # ── Plain HTTP ────────────────────────────────────────────────────────────
    async def _handle_request(self
        ):
        logger.info(f"_handle_request: {self.command} {self.path}")

        content_length = int(self.headers.get('Content-Length', 0))

        packet = Packet(ptype=PacketType.REQUEST)
        packet.set('d',       self.path)
        packet.set('m',       self.command)
        packet.set('headers', dict(self.headers))
        if content_length > 0:
            body = self.rfile.read(content_length)
            packet.set('b', body)
            logger.debug(
                f"_handle_request: {self.command} {self.path} body={content_length}B"
            )

        await self.server.handler.handle(packet)
        logger.debug(
            f"_handle_request: sent to relay pid={packet.pid} "
            f"{self.command} {self.path}"
        )

        pool     = await self.server.handler.pool()
        response = await pool.wait_for(packet.pid, timeout=30.0)

        if response is None:
            logger.warning(
                f"_handle_request: timeout waiting for response pid={packet.pid} "
                f"{self.command} {self.path}"
            )
            try:
                self.send_response(504)
                self.send_header('Content-Length', '0')
                self.end_headers()
            except (ConnectionResetError, BrokenPipeError, OSError) as exc:
                logger.debug(f"_handle_request: 504 write failed — {exc}")
            return

        status  = response.get('status')  or 200
        headers = response.get('headers') or {}
        body    = response.get('b')       or b''
        if isinstance(body, str):
            body = body.encode('latin-1')

        logger.info(
            f"_handle_request: pid={packet.pid} {self.command} {self.path} "
            f"→ {status} body={len(body)}B"
        )

        try:
            self.send_response(status)
            skip = {'transfer-encoding', 'content-length', 'connection'}
            for k, v in headers.items():
                if k.lower() not in skip:
                    self.send_header(k, v)
            self.send_header('Content-Length', str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        except (ConnectionResetError, BrokenPipeError, OSError) as exc:
            logger.info(
                f"_handle_request: write error pid={packet.pid} — {exc}"
            )

    # ── Thread→asyncio bridge ─────────────────────────────────────────────────
    # http.server runs each request in a thread; bridge back to the event loop.
    # Exceptions are caught here so a crashed handler never kills the thread.
    @staticmethod
    def _(coroutine):
        def callback(self):
            future = asyncio.run_coroutine_threadsafe(
                coroutine(self),
                self.server.loop
            )
            try:
                future.result()
            except Exception as exc:
                logger.error(
                    f"ProxyRequestHandler: unhandled exception in {coroutine.__name__}: {exc}",
                    exc_info=True
                )
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
