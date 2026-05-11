import typing
import asyncio
import threading
import socket
import time
import http.server
import logging

from .shared import (
    _logger, IDGenerator, PacketType, Packet,
    StreamBuffer, StreamRegistry, RelayBase, HandlerBase,
)


class Handler(HandlerBase):
    def __init__(self, relay_cls: typing.Type[RelayBase]):
        super().__init__(relay_cls, source='client')
        self._streams = StreamRegistry()

    async def _inbound(self, packets: list[Packet]):
        plain = []
        for p in packets:
            if p.ptype & PacketType.STREAM:
                buf = await self._streams.get_or_create(p.pid)
                await buf.put(p)
            else:
                plain.append(p)
        if plain:
            await self._pool.put_many(plain)

    async def streams(self) -> StreamRegistry:
        return self._streams


class ProxyRequestHandler(http.server.BaseHTTPRequestHandler):
    _log = _logger(__name__ + '.ProxyRequestHandler')

    async def _handle_stream(self):
        self._log.info(f"CONNECT {self.path}")
        self.connection.settimeout(0.1)

        try:
            self.send_response(200, 'Connection established')
            self.end_headers()
        except (ConnectionResetError, BrokenPipeError, OSError) as exc:
            self._log.warning(f"CONNECT ack failed — {exc}")
            return

        pid             : typing.Optional[int]          = None
        buf             : typing.Optional[StreamBuffer] = None
        reg             = await self.server.handler.streams()
        seq             = 0
        IDLE_TIMEOUT    = 30.0   # seconds
        last_data       = time.monotonic()

        while True:
            # Read from the browser
            data = None
            try:
                data = self.connection.recv(65536)
            except (socket.timeout, TimeoutError):
                pass
            except (ConnectionResetError, BrokenPipeError, OSError) as exc:
                self._log.debug(f"client socket error pid={pid} — {exc}")
                if pid is not None:
                    await self._send_close(pid, reg)
                return

            if data is not None:
                if not data:  # browser closed
                    self._log.info(f"client EOF pid={pid}")
                    if pid is not None:
                        await self._send_close(pid, reg)
                    return
                last_data = time.monotonic()

                p       = Packet(ptype=PacketType.REQUEST | PacketType.STREAM)
                p.pid   = pid
                p.seq   = seq
                seq    += 1
                p.set('destination', self.path)
                p.set('body', data)

                await self.server.handler.handle(p)

                if pid is None:
                    pid = p.pid
                    buf = await reg.get_or_create(pid)
                    self._log.info(f"tunnel pid={pid} path={self.path}")

            if buf is None:
                continue

            # Drain all available in-order response chunks
            chunk = await buf.get(timeout=0)
            if chunk is None:
                chunk = await buf.get(timeout=0.05)
            if chunk is None:
                # Check idle timeout
                if time.monotonic() - last_data > IDLE_TIMEOUT:
                    self._log.info(f"tunnel idle timeout pid={pid}")
                    if pid is not None:
                        await self._send_close(pid, reg)
                    return
                continue

            while chunk is not None:
                last_data = time.monotonic()
                if chunk.ptype & PacketType.CLOSE:
                    self._log.info(f"server CLOSE pid={pid}")
                    await reg.remove(pid)
                    return

                if chunk.ptype & PacketType.EOF:
                    self._log.debug(f"server EOF pid={pid} — upstream done")
                    break   # upstream done; client may still send

                body = chunk.get('body') or b''
                if isinstance(body, str):
                    body = body.encode('latin-1')
                if body:
                    try:
                        self.connection.sendall(body)
                    except (ConnectionResetError, BrokenPipeError, OSError) as exc:
                        self._log.debug(f"write error pid={pid} — {exc}")
                        await self._send_close(pid, reg)
                        return

                chunk = await buf.get(timeout=0)

    async def _send_close(self, pid: int, reg: StreamRegistry):
        self._log.info(f"sending CLOSE pid={pid}")
        p     = Packet(ptype=PacketType.REQUEST | PacketType.STREAM | PacketType.CLOSE)
        p.pid = pid
        p.set('body', b'')
        await self.server.handler.handle(p)
        await reg.remove(pid)

    async def _handle_request(self):
        self._log.info(f"{self.command} {self.path}")

        n = int(self.headers.get('Content-Length', 0))
        p = Packet(ptype=PacketType.REQUEST)
        p.set('destination', self.path)
        p.set('method',      self.command)
        p.set('headers',     dict(self.headers))
        if n > 0:
            p.set('body', self.rfile.read(n))

        await self.server.handler.handle(p)

        pool     = await self.server.handler.pool()
        response = await pool.wait_for(p.pid, timeout=30.0)

        if response is None:
            self._log.warning(f"timeout {self.command} {self.path}")
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

        self._log.info(f"pid={p.pid} → {status} {len(body)}B")
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
            self._log.debug(f"response write error — {exc}")

    @staticmethod
    def _run_coro(coro):
        def wrapper(self):
            f = asyncio.run_coroutine_threadsafe(coro(self), self.server.loop)
            try:
                f.result()
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
    daemon_threads      = True
    allow_reuse_address = True

    def __init__(self, relay_cls: typing.Type[RelayBase],
                 addr: tuple, handler_cls: typing.Type[ProxyRequestHandler]):
        super().__init__(addr, handler_cls)
        self.handler = Handler(relay_cls)
        self.loop    = asyncio.get_running_loop()
        _logger(__name__).info(f"proxy on {addr[0]}:{addr[1]}")