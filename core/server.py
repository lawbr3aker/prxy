import typing
import asyncio
import ssl
import aiohttp
import logging

from .shared import (
    _logger, PacketType, Packet,
    PacketPool, StreamState, RelayBase, HandlerBase,
)


class Handler(HandlerBase):
    def __init__(self, relay_cls: typing.Type[RelayBase]):
        super().__init__(relay_cls, source='server')

    async def _inbound(self, packets: list[Packet]):
        await self._pool.put_many(packets)


class Dispatcher:
    def __init__(self, handler: Handler):
        self._log         = _logger(__name__ + '.Dispatcher')
        self._handler     = handler
        self._session     : typing.Optional[aiohttp.ClientSession] = None
        self._streams     : dict[int, StreamState]                 = {}
        self._stream_lock = asyncio.Lock()

    async def start(self):
        ssl_ctx                = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode    = ssl.CERT_NONE
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=ssl_ctx)
        )
        self._log.info("session ready")

    async def stop(self):
        async with self._stream_lock:
            states = list(self._streams.values())
            self._streams.clear()
        for s in states:
            await s.close()
        if self._session:
            await self._session.close()

    async def dispatch(self, raw: dict) -> typing.Optional[Packet]:
        p = Packet.deserialize(raw)
        if p.ptype & PacketType.STREAM:
            await self._do_stream(p)
            return None
        return await self._do_request(p)

    # Fallback: drains PacketPool when GAS used cache path instead of _on_push
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
            resp = await self._do_request(packet)
            if resp:
                await self._handler.handle(resp)

    async def _do_request(self, packet: Packet) -> Packet:
        dst     = packet.get('destination') or ''
        method  = (packet.get('method') or 'GET').upper()
        body    = packet.get('body') or b''
        headers = packet.get('headers') or {}

        if not dst.startswith(('http://', 'https://')):
            dst = 'http://' + dst
        if isinstance(body, str):
            body = body.encode('latin-1')

        self._log.info(f"{method} {dst}")
        try:
            async with self._session.request(
                method, dst, data=body,
                headers={k: v for k, v in headers.items()
                         if k.lower() not in ('host', 'content-length', 'transfer-encoding')},
                timeout=aiohttp.ClientTimeout(total=25),
                allow_redirects=True,
            ) as r:
                body_bytes = await r.read()
                self._log.info(f"pid={packet.pid} → {r.status} {len(body_bytes)}B")
                resp = Packet(ptype=PacketType.RESPONSE)
                resp.pid = packet.pid
                resp.set('status',  r.status)
                resp.set('body',    body_bytes)
                resp.set('headers', dict(r.headers))
                return resp
        except asyncio.TimeoutError:
            return self._err(packet.pid, 504, b'Gateway Timeout')
        except Exception as exc:
            self._log.error(f"request error pid={packet.pid} — {exc}")
            return self._err(packet.pid, 502, str(exc).encode())

    def _err(self, pid: int, status: int, body: bytes) -> Packet:
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
            self._log.info(f"CLOSE pid={pid}")
            await self._close(pid)
            await self._signal(pid, PacketType.RESPONSE | PacketType.STREAM | PacketType.CLOSE)
            return

        async with self._stream_lock:
            state = self._streams.get(pid)

        if state is None:
            dst = packet.get('destination') or ''
            host, _, port_s = dst.rpartition(':')
            port = int(port_s) if port_s.isdigit() else 443
            if not host:
                host = dst
            self._log.info(f"TCP connect pid={pid} → {host}:{port}")
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port), timeout=10.0
                )
            except Exception as exc:
                self._log.error(f"TCP failed pid={pid} {host}:{port} — {exc}")
                await self._signal(pid, PacketType.RESPONSE | PacketType.STREAM | PacketType.CLOSE)
                return

            state      = StreamState(pid, reader, writer)
            state.pump = asyncio.ensure_future(self._pump(pid))
            async with self._stream_lock:
                self._streams[pid] = state
            self._log.info(f"TCP ready pid={pid}")

        if body:
            try:
                state.writer.write(body)
                await state.writer.drain()
            except OSError as exc:
                self._log.info(f"write error pid={pid} — {exc}")
                await self._close(pid)
                await self._signal(pid, PacketType.RESPONSE | PacketType.STREAM | PacketType.CLOSE)

    async def _pump(self, pid: int):
        self._log.info(f"pump start pid={pid}")
        async with self._stream_lock:
            state = self._streams.get(pid)
        if state is None:
            return

        try:
            while state.is_open():
                try:
                    data = await asyncio.wait_for(state.reader.read(65536), timeout=30.0)
                except asyncio.TimeoutError:
                    continue
                except OSError as exc:
                    self._log.info(f"pump read error pid={pid} — {exc}")
                    break

                if not data:
                    self._log.info(f"pump EOF pid={pid}")
                    # Signal EOF before closing so the seq number is correct
                    await self._signal(pid, PacketType.RESPONSE | PacketType.STREAM | PacketType.EOF)
                    return

                resp     = Packet(ptype=PacketType.RESPONSE | PacketType.STREAM)
                resp.pid = pid
                resp.seq = state.next_seq()
                resp.set('body', data)
                await self._handler.handle(resp)

        except asyncio.CancelledError:
            pass
        finally:
            await self._close(pid)

    async def _close(self, pid: int):
        async with self._stream_lock:
            state = self._streams.pop(pid, None)
        if state:
            await state.close()

    async def _signal(self, pid: int, ptype: int):
        self._log.info(f"signal pid={pid} {PacketType.name(ptype)}")
        p     = Packet(ptype=ptype)
        p.pid = pid
        # Signal seq comes from remaining state if still alive, otherwise just 0
        async with self._stream_lock:
            state = self._streams.get(pid)
        if state:
            p.seq = state.next_seq()
        p.set('body', b'')
        await self._handler.handle(p)