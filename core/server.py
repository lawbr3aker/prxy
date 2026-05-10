import typing
import asyncio
import ssl
import aiohttp
import logging

from .shared import (
    class_logger, PacketType, Packet,
    StreamState, RelayBase, HandlerBase,
)


class Handler(HandlerBase):
    _log = class_logger(__name__, 'Handler')

    def __init__(self, relay_cls: typing.Type[RelayBase]):
        super().__init__(relay_cls, source='server')

    async def _inbound(self, packets: list[Packet]):
        await self._pool.put_many(packets)


class Dispatcher:
    _log = class_logger(__name__, 'Dispatcher')

    def __init__(self, handler: Handler):
        self._handler     = handler
        self._session     : typing.Optional[aiohttp.ClientSession] = None
        self._streams     : dict[int, StreamState]                 = {}
        self._stream_lock = asyncio.Lock()

    async def start(self):
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode    = ssl.CERT_NONE
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=ssl_ctx)
        )
        self._log.info("session created")

    async def stop(self):
        async with self._stream_lock:
            states = list(self._streams.values())
            self._streams.clear()
        for s in states:
            await s.close()
        if self._session:
            await self._session.close()

    # Primary path: called directly by relay._on_push()
    async def dispatch(self, raw: dict) -> typing.Optional[Packet]:
        packet = Packet.deserialize(raw)
        if packet.ptype & PacketType.STREAM:
            await self._do_stream(packet)
            return None
        return await self._do_request(packet)

    # Fallback path: drains pool when GAS couldn't reach _on_push
    async def run_fallback(self):
        while True:
            pool   = await self._handler.pool()
            packet = await pool.find(lambda p: bool(p.ptype & PacketType.REQUEST))
            if packet is None:
                await asyncio.sleep(0.01)
                continue
            asyncio.ensure_future(self._fallback_dispatch(packet))

    async def _fallback_dispatch(self, packet: Packet):
        if packet.ptype & PacketType.STREAM:
            await self._do_stream(packet)
        else:
            response = await self._do_request(packet)
            if response:
                await self._handler.handle(response)

    async def _do_request(self, packet: Packet) -> typing.Optional[Packet]:
        destination = packet.get('destination') or ''
        method      = (packet.get('method') or 'GET').upper()
        body        = packet.get('body') or b''
        headers     = packet.get('headers') or {}

        if not destination.startswith(('http://', 'https://')):
            destination = 'http://' + destination
        if isinstance(body, str):
            body = body.encode('latin-1')

        self._log.info(f"{method} {destination}")
        try:
            async with self._session.request(
                method, destination,
                data=body,
                headers={k: v for k, v in headers.items()
                         if k.lower() not in ('host', 'content-length', 'transfer-encoding')},
                timeout=aiohttp.ClientTimeout(total=25),
                allow_redirects=True,
            ) as r:
                body_bytes = await r.read()
                self._log.info(f"pid={packet.pid} → {r.status} body={len(body_bytes)}B")
                resp = Packet(ptype=PacketType.RESPONSE)
                resp.pid = packet.pid
                resp.set('status',  r.status)
                resp.set('body',    body_bytes)
                resp.set('headers', dict(r.headers))
                return resp
        except asyncio.TimeoutError:
            return self._error(packet.pid, 504, b'Gateway Timeout')
        except Exception as exc:
            self._log.error(f"request error pid={packet.pid} — {exc}")
            return self._error(packet.pid, 502, str(exc).encode())

    def _error(self, pid: int, status: int, body: bytes) -> Packet:
        p = Packet(ptype=PacketType.RESPONSE)
        p.pid = pid
        p.set('status', status)
        p.set('body',   body)
        return p

    async def _do_stream(self, packet: Packet):
        pid   = packet.pid
        ptype = packet.ptype
        body  = packet.get('body') or b''
        if isinstance(body, str):
            body = body.encode('latin-1')

        if ptype & PacketType.CLOSE:
            self._log.info(f"CLOSE from client pid={pid}")
            await self._close_stream(pid)
            await self._signal(pid, PacketType.RESPONSE | PacketType.STREAM | PacketType.CLOSE)
            return

        async with self._stream_lock:
            state = self._streams.get(pid)

        if state is None:
            destination = packet.get('destination') or ''
            host, _, port_str = destination.rpartition(':')
            port = int(port_str) if port_str.isdigit() else 443
            if not host:
                host = destination
            self._log.info(f"TCP connect pid={pid} → {host}:{port}")
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port), timeout=10.0
                )
            except Exception as exc:
                self._log.error(f"TCP connect failed pid={pid} — {exc}")
                await self._signal(pid, PacketType.RESPONSE | PacketType.STREAM | PacketType.CLOSE)
                return

            state = StreamState(pid, reader, writer)
            async with self._stream_lock:
                self._streams[pid] = state
            # Store state BEFORE starting pump so pump finds it on first check
            state.pump = asyncio.ensure_future(self._pump(pid))
            self._log.info(f"TCP connected pid={pid} → {host}:{port}")

        if body:
            try:
                state.writer.write(body)
                await state.writer.drain()
            except OSError as exc:
                self._log.info(f"write error pid={pid} — {exc}")
                await self._close_stream(pid)
                await self._signal(pid, PacketType.RESPONSE | PacketType.STREAM | PacketType.CLOSE)

    async def _pump(self, pid: int):
        self._log.info(f"pump start pid={pid}")
        async with self._stream_lock:
            state = self._streams.get(pid)
        if state is None:
            return
        reader = state.reader
        try:
            while state.is_open():
                try:
                    data = await asyncio.wait_for(reader.read(65536), timeout=30.0)
                except asyncio.TimeoutError:
                    continue
                except OSError as exc:
                    self._log.info(f"pump read error pid={pid} — {exc}")
                    break
                if not data:
                    self._log.info(f"pump EOF pid={pid}")
                    await self._close_stream(pid)
                    await self._signal(pid, PacketType.RESPONSE | PacketType.STREAM | PacketType.EOF)
                    return
                resp = Packet(ptype=PacketType.RESPONSE | PacketType.STREAM)
                resp.pid = pid
                resp.set('body', data)
                await self._handler.handle(resp)
        except asyncio.CancelledError:
            pass
        finally:
            await self._close_stream(pid)

    async def _close_stream(self, pid: int):
        async with self._stream_lock:
            state = self._streams.pop(pid, None)
        if state:
            await state.close()

    async def _signal(self, pid: int, ptype: int):
        p = Packet(ptype=ptype)
        p.pid = pid
        p.set('body', b'')
        await self._handler.handle(p)
