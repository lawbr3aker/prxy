import typing
import asyncio
import threading
import socket

import http.server

import logging

from .shared import (
    class_logger,
    IDGenerator,
    PacketType,
    Packet,
    PacketPool,
    StreamRegistry,
    RelayRequest,
    RelayBase,
    HandlerBase,
)


class Handler(HandlerBase):
    _log = class_logger(__name__, 'Handler')

    def __init__(self, relay_cls: typing.Type[RelayBase]):
        super().__init__(relay_cls, source='client')
        self._streams = StreamRegistry()

    async def _inbound(self, packets: list[Packet]):
        plain = []
        for packet in packets:
            if packet.ptype & PacketType.STREAM:
                self._log.debug(
                    f"_inbound: → StreamRegistry pid={packet.pid} "
                    f"ts={packet.timestamp} ptype={PacketType.name(packet.ptype)}"
                )
                buf = await self._streams.get_or_create(packet.pid)
                await buf.put(packet)
            else:
                plain.append(packet)
        if plain:
            await self._pool.put_many(plain)

    async def streams(self) -> StreamRegistry:
        return self._streams


class ProxyRequestHandler(http.server.BaseHTTPRequestHandler):
    _log = class_logger(__name__, 'ProxyRequestHandler')

    # ── CONNECT tunnel ────────────────────────────────────────────────────────
    async def _handle_stream(self):
        self._log.info(f"CONNECT {self.path}")
        self.connection.settimeout(0.1)

        try:
            self.send_response(200, 'Connection established')
            self.end_headers()
        except (ConnectionResetError, BrokenPipeError, OSError) as exc:
            self._log.warning(f"CONNECT ack failed {self.path} — {exc}")
            return

        pid : typing.Optional[int]    = None
        buf : typing.Optional[object] = None
        reg = await self.server.handler.streams()

        while True:
            # ── Read from browser ─────────────────────────────────────────────
            data = None
            try:
                data = self.connection.recv(4096)
                self._log.debug(
                    f"recv pid={pid} path={self.path} bytes={len(data)}"
                )
            except (socket.timeout, TimeoutError):
                pass
            except ConnectionResetError as exc:
                self._log.info(f"client reset pid={pid} path={self.path} — {exc}")
                if pid is not None:
                    await self._send_close(pid)
                    await reg.remove(pid)
                return
            except (BrokenPipeError, OSError) as exc:
                self._log.info(f"socket error pid={pid} path={self.path} — {exc}")
                if pid is not None:
                    await self._send_close(pid)
                    await reg.remove(pid)
                return

            if data is not None:
                if not data:
                    self._log.info(f"client EOF pid={pid} path={self.path}")
                    if pid is not None:
                        await self._send_close(pid)
                        await reg.remove(pid)
                    return

                packet = Packet(ptype=PacketType.REQUEST | PacketType.STREAM)
                packet.pid = pid
                packet.set('destination', self.path)
                packet.set('body', data)

                await self.server.handler.handle(packet)

                if pid is None:
                    pid = packet.pid
                    buf = await reg.get_or_create(pid)
                    self._log.info(f"tunnel established pid={pid} path={self.path}")

            if buf is None:
                continue

            # ── Drain response chunks in timestamp order ───────────────────────
            chunk = await buf.get(timeout=0.05)
            if chunk is None:
                continue

            while chunk is not None:
                ptype = chunk.ptype

                if ptype & PacketType.CLOSE:
                    self._log.info(f"server CLOSE pid={pid}")
                    await reg.remove(pid)
                    return

                if ptype & PacketType.EOF:
                    self._log.info(f"server EOF pid={pid} — upstream done, tunnel open")
                    break

                body = chunk.get('body') or b''
                if isinstance(body, str):
                    body = body.encode('latin-1')

                if body:
                    self._log.debug(
                        f"← server pid={pid} ts={chunk.timestamp} bytes={len(body)}"
                    )
                    try:
                        self.connection.sendall(body)
                    except (ConnectionResetError, BrokenPipeError, OSError) as exc:
                        self._log.info(f"write error pid={pid} — {exc}")
                        await self._send_close(pid)
                        await reg.remove(pid)
                        return

                chunk = await buf.get(timeout=0.0)

    async def _send_close(self, pid: int):
        self._log.info(f"sending CLOSE pid={pid}")
        packet = Packet(ptype=PacketType.REQUEST | PacketType.STREAM | PacketType.CLOSE)
        packet.pid = pid
        packet.set('body', b'')
        await self.server.handler.handle(packet)

    # ── Plain HTTP ────────────────────────────────────────────────────────────
    async def _handle_request(self):
        self._log.info(f"{self.command} {self.path}")

        content_length = int(self.headers.get('Content-Length', 0))

        packet = Packet(ptype=PacketType.REQUEST)
        packet.set('destination', self.path)
        packet.set('method',      self.command)
        packet.set('headers',     dict(self.headers))
        if content_length > 0:
            packet.set('body', self.rfile.read(content_length))

        await self.server.handler.handle(packet)

        pool     = await self.server.handler.pool()
        response = await pool.wait_for(packet.pid, timeout=30.0)

        if response is None:
            self._log.warning(f"timeout pid={packet.pid} {self.command} {self.path}")
            try:
                self.send_response(504)
                self.send_header('Content-Length', '0')
                self.end_headers()
            except (ConnectionResetError, BrokenPipeError, OSError):
                pass
            return

        status  = response.get('status')  or 200
        headers = response.get('headers') or {}
        body    = response.get('body')    or b''
        if isinstance(body, str):
            body = body.encode('latin-1')

        self._log.info(
            f"pid={packet.pid} {self.command} {self.path} → {status} body={len(body)}B"
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
            self._log.info(f"write error pid={packet.pid} — {exc}")

    # ── Thread → asyncio bridge ───────────────────────────────────────────────
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
                ProxyRequestHandler._log.error(
                    f"unhandled exception in {coroutine.__name__}: {exc}",
                    exc_info=True
                )
        return callback

    do_GET     = _(_handle_request)
    do_POST    = _(_handle_request)
    do_OPTIONS = _(_handle_request)
    do_HEAD    = _(_handle_request)
    do_PUT     = _(_handle_request)
    do_DELETE  = _(_handle_request)
    do_PATCH   = _(_handle_request)

    do_CONNECT = _(_handle_stream)

    def log_message(self, fmt, *args):
        self._log.debug(fmt, *args)


class ProxyServer(http.server.ThreadingHTTPServer):
    _log = class_logger(__name__, 'ProxyServer')

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
        self._log.info(f"listening on {addr[0]}:{addr[1]}")