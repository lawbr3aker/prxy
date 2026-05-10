import typing
import asyncio
import threading
import socket
import http.server
import logging

from .shared import (
    class_logger, IDGenerator, PacketType, Packet,
    StreamRegistry, RelayBase, HandlerBase,
)


class Handler(HandlerBase):
    _log = class_logger(__name__, 'Handler')

    def __init__(self, relay_cls: typing.Type[RelayBase]):
        super().__init__(relay_cls, source='client')
        self._streams     = StreamRegistry()
        self._closed_pids : set[int] = set()

    async def _inbound(self, packets: list[Packet]):
        plain = []
        for p in packets:
            if p.ptype & PacketType.STREAM:
                if p.pid in self._closed_pids:
                    self._log.debug(f"dropping stale stream pid={p.pid}")
                    continue
                buf = await self._streams.get_or_create(p.pid)
                await buf.put(p)
            else:
                plain.append(p)
        if plain:
            await self._pool.put_many(plain)

    async def streams(self) -> StreamRegistry:
        return self._streams

    async def close_stream(self, pid: int):
        self._closed_pids.add(pid)
        await self._streams.remove(pid)


class ProxyRequestHandler(http.server.BaseHTTPRequestHandler):
    _log = class_logger(__name__, 'ProxyRequestHandler')

    async def _handle_stream(self):
        self._log.info(f"CONNECT {self.path}")
        self.connection.settimeout(0.1)

        try:
            self.send_response(200, 'Connection established')
            self.end_headers()
        except (ConnectionResetError, BrokenPipeError, OSError) as exc:
            self._log.warning(f"CONNECT ack failed — {exc}")
            return

        pid : typing.Optional[int]          = None
        buf : typing.Optional[object]       = None
        reg = await self.server.handler.streams()

        while True:
            # ── Read from browser ──────────────────────────────────────────────
            data = None
            try:
                data = self.connection.recv(65536)
            except (socket.timeout, TimeoutError):
                pass
            except (ConnectionResetError, BrokenPipeError, OSError) as exc:
                self._log.debug(f"client socket error pid={pid} — {exc}")
                if pid is not None:
                    await self._send_close(pid)
                    await self.server.handler.close_stream(pid)
                return

            if data is not None:
                if not data:
                    self._log.info(f"client EOF pid={pid}")
                    if pid is not None:
                        await self._send_close(pid)
                        await self.server.handler.close_stream(pid)
                    return

                packet = Packet(ptype=PacketType.REQUEST | PacketType.STREAM)
                packet.pid = pid
                packet.set('destination', self.path)
                packet.set('body', data)

                await self.server.handler.handle(packet)

                if pid is None:
                    pid = packet.pid
                    buf = await reg.get_or_create(pid)
                    self._log.info(f"tunnel pid={pid} path={self.path}")

            if buf is None:
                continue

            # ── Drain all available response chunks (timestamp-ordered) ────────
            chunk = await buf.get(timeout=0.05)
            while chunk is not None:
                if chunk.ptype & PacketType.CLOSE:
                    self._log.info(f"server CLOSE pid={pid}")
                    await self.server.handler.close_stream(pid)
                    return

                if chunk.ptype & PacketType.EOF:
                    self._log.debug(f"server EOF pid={pid}")
                    break

                body = chunk.get('body') or b''
                if isinstance(body, str):
                    body = body.encode('latin-1')
                if body:
                    try:
                        self.connection.sendall(body)
                    except (ConnectionResetError, BrokenPipeError, OSError) as exc:
                        self._log.debug(f"write error pid={pid} — {exc}")
                        await self._send_close(pid)
                        await self.server.handler.close_stream(pid)
                        return

                chunk = await buf.get(timeout=0)

    async def _send_close(self, pid: int):
        packet = Packet(ptype=PacketType.REQUEST | PacketType.STREAM | PacketType.CLOSE)
        packet.pid = pid
        packet.set('body', b'')
        await self.server.handler.handle(packet)

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
            self._log.warning(f"timeout pid={packet.pid}")
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

        self._log.info(f"pid={packet.pid} → {status} body={len(body)}B")
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
            self._log.debug(f"write error pid={packet.pid} — {exc}")

    @staticmethod
    def _run_coro(coro):
        def wrapper(self):
            future = asyncio.run_coroutine_threadsafe(coro(self), self.server.loop)
            try:
                future.result()
            except Exception as exc:
                ProxyRequestHandler._log.error(f"unhandled: {exc}", exc_info=True)
        return wrapper

    do_GET     = _run_coro(_handle_request)
    do_POST    = _run_coro(_handle_request)
    do_OPTIONS = _run_coro(_handle_request)
    do_HEAD    = _run_coro(_handle_request)
    do_PUT     = _run_coro(_handle_request)
    do_DELETE  = _run_coro(_handle_request)
    do_PATCH   = _run_coro(_handle_request)
    do_CONNECT = _run_coro(_handle_stream)

    def log_message(self, fmt, *args):
        self._log.debug(fmt, *args)


class ProxyServer(http.server.ThreadingHTTPServer):
    _log           = class_logger(__name__, 'ProxyServer')
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, relay_cls: typing.Type[RelayBase],
                 addr: tuple, handler_cls: typing.Type[ProxyRequestHandler]):
        super().__init__(addr, handler_cls)
        self.handler = Handler(relay_cls)
        self.loop    = asyncio.get_running_loop()
        self._log.info(f"listening on {addr[0]}:{addr[1]}")
