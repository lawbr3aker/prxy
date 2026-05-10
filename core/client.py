import typing
import asyncio
import threading
import time
import socket

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
    StreamRegistry,
    RelayRequest,
    RelayBase,
)


class Handler:
    def __init__(self, relay_cls: typing.Type[RelayBase]):
        self._relay    = relay_cls(self)

        self._lock     = asyncio.Lock()
        self._pool     = PacketPool()
        self._streams  = StreamRegistry()

        self._queue: typing.Optional[PacketQueue] = None

    async def init(self
        ):
        logger.info("Handler.init: starting relay")
        await self._relay.start()
        logger.info("Handler.init: relay started")

    # Inbound: relay deposits raw dicts → Packet objects.
    # STREAM packets → StreamRegistry (ordered by seq).
    # Plain packets  → PacketPool (wakes the exact waiter).
    async def receive(self,
            raw_packets: list[dict]
        ):
        packets = [Packet.deserialize(raw) for raw in raw_packets]
        logger.info(
            f"Handler.receive: {len(packets)} packet(s) "
            f"pids={[p.pid for p in packets]}"
        )

        plain = []
        for packet in packets:
            if packet.ptype & PacketType.STREAM:
                logger.debug(
                    f"Handler.receive: → StreamRegistry pid={packet.pid} "
                    f"seq={packet.seq} ptype={PacketType.name(packet.ptype)}"
                )
                buf = await self._streams.get_or_create(packet.pid)
                await buf.put(packet)
            else:
                plain.append(packet)

        if plain:
            await self._pool.put_many(plain)

    # Outbound: stamp and send.
    # STREAM packets skip the batching queue entirely — sent immediately.
    # Plain packets wait in the PacketQueue flush window to batch relay calls.
    async def handle(self,
            packet: Packet
        ):
        packet.timestamp = int(time.time() * 1000)
        if packet.pid is None:
            packet.pid = IDGenerator.generate(packet.timestamp)

        logger.debug(f"Handler.handle: {packet!r}")

        # Stream packets must not wait in the batching window — every extra
        # millisecond here is a millisecond of tunnel latency.
        if packet.ptype & PacketType.STREAM:
            await self.submit([packet])
            return

        async with self._lock:
            if self._queue is None:
                logger.debug("Handler.handle: creating new PacketQueue")
                self._queue = PacketQueue()
            queue = self._queue

        await queue.enqueue(packet)
        await queue.wait()

        payload = None
        async with self._lock:
            if self._queue is queue:
                payload     = queue._queue[:]
                self._queue = None
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

    async def streams(self
        ) -> StreamRegistry:
        return self._streams

    async def submit(self,
            batch: list[Packet]
        ):
        request = RelayRequest(packets=batch, source='client')
        logger.info(f"Handler.submit: {request!r}")
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

        # pid shared across all chunks so server can reassemble the stream.
        # Assigned on first outbound packet.
        pid    : typing.Optional[int]        = None
        buf    : typing.Optional[object]     = None
        reg    = await self.server.handler.streams()

        while True:
            # ── Read from the browser ─────────────────────────────────────────
            data = None
            try:
                data = self.connection.recv(4096)
                logger.debug(
                    f"_handle_stream: recv pid={pid} path={self.path} "
                    f"bytes={len(data)} hash={hash(data)}"
                )
            except (socket.timeout, TimeoutError):
                # No data from browser right now — fall through to drain responses
                pass
            except ConnectionResetError as exc:
                logger.info(
                    f"_handle_stream: client reset pid={pid} path={self.path} — {exc}"
                )
                if pid is not None:
                    await self._send_close(pid)
                    await reg.remove(pid)
                return
            except (BrokenPipeError, OSError) as exc:
                logger.info(
                    f"_handle_stream: socket error pid={pid} path={self.path} — {exc}"
                )
                if pid is not None:
                    await self._send_close(pid)
                    await reg.remove(pid)
                return

            if data is not None:
                if not data:
                    # Zero-byte read — browser closed cleanly
                    logger.info(
                        f"_handle_stream: client EOF pid={pid} path={self.path}"
                    )
                    if pid is not None:
                        await self._send_close(pid)
                        await reg.remove(pid)
                    return

                packet = Packet(ptype=PacketType.REQUEST | PacketType.STREAM)
                packet.pid = pid   # None on first chunk — IDGenerator fills it
                packet.set('destination', self.path)
                packet.set('b', data)

                logger.debug(
                    f"_handle_stream: → relay pid={pid} path={self.path} "
                    f"bytes={len(data)}"
                )
                await self.server.handler.handle(packet)

                if pid is None:
                    pid = packet.pid
                    buf = await reg.get_or_create(pid)
                    logger.info(
                        f"_handle_stream: tunnel established pid={pid} path={self.path}"
                    )

            # ── Drain response chunks from the server ─────────────────────────
            # On each iteration: drain all in-order chunks without extra waiting.
            # If nothing is ready, buf.get() with a short timeout suspends cheaply.
            if buf is None:
                # pid not assigned yet — nothing to drain
                continue

            # Non-blocking check first — avoids even the asyncio event overhead
            # on the common case where data is already queued.
            chunk = await buf.get(timeout=0.05)
            if chunk is None:
                continue

            while chunk is not None:
                ptype = chunk.ptype

                if ptype & PacketType.CLOSE:
                    logger.info(
                        f"_handle_stream: server CLOSE pid={pid} path={self.path}"
                    )
                    await reg.remove(pid)
                    return

                if ptype & PacketType.EOF:
                    logger.info(
                        f"_handle_stream: server EOF pid={pid} path={self.path} "
                        f"— upstream done, tunnel stays open"
                    )
                    # EOF means upstream finished sending; the tunnel is still
                    # open for the client to send more.  Stop draining and loop.
                    break

                body = chunk.get('b') or b''
                if isinstance(body, str):
                    body = body.encode('latin-1')

                if body:
                    logger.debug(
                        f"_handle_stream: ← server pid={pid} seq={chunk.seq} "
                        f"bytes={len(body)} hash={hash(body)}"
                    )
                    try:
                        self.connection.sendall(body)
                    except ConnectionResetError as exc:
                        logger.info(
                            f"_handle_stream: client reset on write pid={pid} — {exc}"
                        )
                        await self._send_close(pid)
                        await reg.remove(pid)
                        return
                    except (BrokenPipeError, OSError) as exc:
                        logger.info(
                            f"_handle_stream: write error pid={pid} — {exc}"
                        )
                        await self._send_close(pid)
                        await reg.remove(pid)
                        return

                # Try to drain the next in-order chunk immediately (non-blocking)
                chunk = await buf.get(timeout=0.0)

    async def _send_close(self,
            pid: int
        ):
        logger.info(f"_handle_stream: sending CLOSE pid={pid}")
        packet = Packet(ptype=PacketType.REQUEST | PacketType.STREAM | PacketType.CLOSE)
        packet.pid = pid
        packet.set('b', b'')
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
                f"_handle_request: timeout pid={packet.pid} "
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
                    f"ProxyRequestHandler: unhandled exception in "
                    f"{coroutine.__name__}: {exc}",
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
