import typing
import asyncio
import ssl
import aiohttp
import logging

from .shared import (
    class_logger, PacketType, Packet, PacketPool,
    StreamState, RelayRequest, RelayBase, HandlerBase
)

class Handler(HandlerBase):
    _log = class_logger(__name__, 'Handler')

    def __init__(self, relay_cls: typing.Type[RelayBase]):
        super().__init__(relay_cls, source='server')

    async def _inbound(self, packets: list[Packet]):
        # Only plain REQUEST packets arrive here (stream packets are handled directly)
        await self._pool.put_many(packets)


class Dispatcher:
    _log = class_logger(__name__, 'Dispatcher')

    def __init__(self, handler: Handler):
        self._handler = handler
        self._session: typing.Optional[aiohttp.ClientSession] = None
        self._streams: dict[int, StreamState] = {}
        self._stream_lock = asyncio.Lock()
        self._seq_counter: dict[int, int] = {}   # per‑PID seq for outgoing stream chunks

    async def start(self):
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE
        self._session = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_ctx))
        self._log.info("aiohttp session created")

    async def stop(self):
        async with self._stream_lock:
            states = list(self._streams.values())
            self._streams.clear()
        for state in states:
            await state.close()
        if self._session:
            await self._session.close()
        self._log.info("all streams closed")

    async def dispatch(self, raw: dict) -> typing.Optional[Packet]:
        packet = Packet.deserialize(raw)
        if packet.ptype & PacketType.STREAM:
            await self._do_stream(packet)
            return None
        return await self._do_request(packet)

    async def _do_request(self, packet: Packet) -> typing.Optional[Packet]:
        destination = packet.get('destination') or ''
        method = (packet.get('method') or 'GET').upper()
        body = packet.get('body') or b''
        headers = packet.get('headers') or {}

        if not destination.startswith(('http://', 'https://')):
            destination = 'http://' + destination
        if isinstance(body, str):
            body = body.encode('latin-1')

        self._log.info(f"{method} {destination} body={len(body)}B")
        try:
            async with self._session.request(
                method, destination, data=body,
                headers={k: v for k, v in headers.items()
                         if k.lower() not in ('host', 'content-length', 'transfer-encoding')},
                timeout=aiohttp.ClientTimeout(total=25), allow_redirects=True
            ) as r:
                body_bytes = await r.read()
                self._log.info(f"pid={packet.pid} → {r.status} body={len(body_bytes)}B")
                response = Packet(ptype=PacketType.RESPONSE)
                response.pid = packet.pid
                response.set('status', r.status)
                response.set('body', body_bytes)
                response.set('headers', dict(r.headers))
                return response
        except asyncio.TimeoutError:
            self._log.warning(f"timeout {method} {destination}")
            return self._error_packet(packet.pid, 504, b'Gateway Timeout')
        except aiohttp.ClientConnectionError as exc:
            self._log.error(f"connection error {exc}")
            return self._error_packet(packet.pid, 502, str(exc).encode())
        except Exception as exc:
            self._log.error(f"unexpected error {exc}", exc_info=True)
            return self._error_packet(packet.pid, 502, str(exc).encode())

    def _error_packet(self, pid: int, status: int, body: bytes) -> Packet:
        p = Packet(ptype=PacketType.RESPONSE)
        p.pid = pid
        p.set('status', status)
        p.set('body', body)
        return p

    async def _do_stream(self, packet: Packet):
        pid = packet.pid
        ptype = packet.ptype
        body = packet.get('body') or b''
        if isinstance(body, str):
            body = body.encode('latin-1')

        self._log.debug(f"pid={pid} ptype={PacketType.name(ptype)} bytes={len(body)}")

        if ptype & PacketType.CLOSE:
            self._log.info(f"CLOSE from client pid={pid}")
            await self._close_stream(pid)
            await self._send_signal(pid, PacketType.RESPONSE | PacketType.STREAM | PacketType.CLOSE)
            return

        async with self._stream_lock:
            state = self._streams.get(pid)

        if state is None:
            destination = packet.get('destination') or ''
            host, _, port_str = destination.rpartition(':')
            port = int(port_str) if port_str.isdigit() else 443
            if not host:
                host = destination
            self._log.info(f"opening TCP pid={pid} → {host}:{port}")
            try:
                reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), timeout=10.0)
            except (asyncio.TimeoutError, OSError, ConnectionRefusedError) as exc:
                self._log.error(f"TCP connect failed {host}:{port} — {exc}")
                await self._send_signal(pid, PacketType.RESPONSE | PacketType.STREAM | PacketType.CLOSE)
                return
            state = StreamState(pid, reader, writer)
            async with self._stream_lock:
                self._streams[pid] = state
            self._seq_counter[pid] = 0
            state.pump = asyncio.ensure_future(self._stream_pump(pid))

        if body:
            try:
                state.writer.write(body)
                await state.writer.drain()
            except (ConnectionResetError, BrokenPipeError, OSError) as exc:
                self._log.info(f"write error pid={pid} — {exc}")
                await self._close_stream(pid)
                await self._send_signal(pid, PacketType.RESPONSE | PacketType.STREAM | PacketType.CLOSE)

    async def _stream_pump(self, pid: int):
        self._log.info(f"pump started pid={pid}")
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
                except (ConnectionResetError, OSError) as exc:
                    self._log.info(f"read error pid={pid} — {exc}")
                    break
                if not data:
                    self._log.info(f"upstream EOF pid={pid}")
                    await self._send_signal(pid, PacketType.RESPONSE | PacketType.STREAM | PacketType.EOF)
                    break
                seq = self._seq_counter.get(pid, 0)
                self._seq_counter[pid] = seq + 1
                response = Packet(ptype=PacketType.RESPONSE | PacketType.STREAM)
                response.pid = pid
                response.seq = seq
                response.set('body', data)
                await self._handler.handle(response)
        except asyncio.CancelledError:
            self._log.info(f"pump cancelled pid={pid}")
        finally:
            await self._close_stream(pid)

    async def _close_stream(self, pid: int):
        async with self._stream_lock:
            state = self._streams.pop(pid, None)
            self._seq_counter.pop(pid, None)
        if state:
            await state.close()

    async def _send_signal(self, pid: int, ptype: int):
        self._log.info(f"signal pid={pid} {PacketType.name(ptype)}")
        packet = Packet(ptype=ptype)
        packet.pid = pid
        packet.set('body', b'')
        await self._handler.handle(packet)