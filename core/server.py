import typing
import asyncio
import ssl

import aiohttp

import logging

from .shared import (
    class_logger,
    PacketType,
    Packet,
    PacketPool,
    StreamState,
    RelayRequest,
    RelayBase,
    HandlerBase,
)


class Handler(HandlerBase):
    _log = class_logger(__name__, 'Handler')

    def __init__(self, relay_cls: typing.Type[RelayBase]):
        super().__init__(relay_cls, source='server')

    async def _inbound(self, packets: list[Packet]):
        # Server only receives plain REQUEST packets from the client via GAS.
        # (Stream chunks never go through the pool on the server — they are
        # handled directly by Dispatcher._do_stream via dispatch().)
        await self._pool.put_many(packets)


class Dispatcher:
    """
    Makes real HTTP/TCP calls on behalf of the client.

    Primary path  — relay calls dispatch(raw) directly per packet.
                    Plain HTTP returns a Packet inline.
                    STREAM returns None; pump delivers responses via handle().

    Fallback path — relay deposits packets via handler.receive() → pool;
                    _run() drains and dispatches them.

    STREAM lifecycle
    ────────────────
    First DATA chunk:   open TCP, create StreamState, start pump, write data.
    Subsequent chunks:  write to existing StreamState.writer.
    CLOSE from client:  close StreamState, send CLOSE response.
    Pump reads upstream continuously; stamps each chunk with current timestamp;
    calls handler.handle() → relay.send() → client StreamBuffer (ordered).
    """

    _log = class_logger(__name__, 'Dispatcher')

    def __init__(self, handler: Handler):
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
        self._log.info("aiohttp session created")
        asyncio.ensure_future(self._run())
        self._log.info("fallback _run() loop started")

    async def stop(self):
        async with self._stream_lock:
            states = list(self._streams.values())
            self._streams.clear()
        for state in states:
            await state.close()
        if self._session:
            await self._session.close()
        self._log.info("all streams closed, session closed")

    # ── Primary path ─────────────────────────────────────────────────────────
    async def dispatch(self, raw: dict) -> typing.Optional[Packet]:
        packet = Packet.deserialize(raw)
        self._log.debug(f"dispatch: {packet!r}")

        if packet.ptype & PacketType.STREAM:
            await self._do_stream(packet)
            return None

        return await self._do_request(packet)

    # ── Fallback path ─────────────────────────────────────────────────────────
    async def _run(self):
        self._log.debug("fallback loop started")
        while True:
            pool   = await self._handler.pool()
            packet = await pool.find(lambda p: bool(p.ptype & PacketType.REQUEST))
            if packet is None:
                await asyncio.sleep(0.01)
                continue
            self._log.debug(f"fallback dispatching {packet!r}")
            asyncio.ensure_future(self._fallback_dispatch(packet))

    async def _fallback_dispatch(self, packet: Packet):
        if packet.ptype & PacketType.STREAM:
            await self._do_stream(packet)
        else:
            response = await self._do_request(packet)
            if response:
                await self._handler.handle(response)

    # ── Plain HTTP ────────────────────────────────────────────────────────────
    async def _do_request(self, packet: Packet) -> typing.Optional[Packet]:
        destination = packet.get('destination') or ''
        method      = (packet.get('method') or 'GET').upper()
        body        = packet.get('body')
        headers     = packet.get('headers') or {}

        if not destination.startswith('http'):
            destination = 'http://' + destination
        if isinstance(body, str):
            body = body.encode('latin-1')

        self._log.info(f"{method} {destination} body={len(body) if body else 0}B")

        try:
            async with self._session.request(
                    method,
                    destination,
                    data=body,
                    headers={k: v for k, v in headers.items()
                             if k.lower() not in
                             {'host', 'content-length', 'transfer-encoding'}},
                    timeout=aiohttp.ClientTimeout(total=25),
                    allow_redirects=True
                ) as r:
                body_bytes = await r.read()
                self._log.info(
                    f"pid={packet.pid} {method} {destination} "
                    f"→ {r.status} body={len(body_bytes)}B"
                )
                response = Packet(ptype=PacketType.RESPONSE)
                response.pid = packet.pid
                response.set('status',  r.status)
                response.set('body',    body_bytes)
                response.set('headers', dict(r.headers))

        except asyncio.TimeoutError:
            self._log.warning(f"timeout pid={packet.pid} {method} {destination}")
            response = Packet(ptype=PacketType.RESPONSE)
            response.pid = packet.pid
            response.set('status', 504)
            response.set('body',   b'Gateway Timeout')

        except aiohttp.ClientConnectionError as exc:
            self._log.error(f"connection error pid={packet.pid} {method} {destination} — {exc}")
            response = Packet(ptype=PacketType.RESPONSE)
            response.pid = packet.pid
            response.set('status', 502)
            response.set('body',   str(exc).encode())

        except Exception as exc:
            self._log.error(f"unexpected error pid={packet.pid} — {exc}", exc_info=True)
            response = Packet(ptype=PacketType.RESPONSE)
            response.pid = packet.pid
            response.set('status', 502)
            response.set('body',   str(exc).encode())

        return response

    # ── STREAM chunk handler ──────────────────────────────────────────────────
    async def _do_stream(self, packet: Packet):
        pid   = packet.pid
        ptype = packet.ptype
        body  = packet.get('body') or b''
        if isinstance(body, str):
            body = body.encode('latin-1')

        self._log.debug(
            f"pid={pid} ptype={PacketType.name(ptype)} "
            f"dst={packet.get('destination')} bytes={len(body)}"
        )

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
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=10.0
                )
            except (asyncio.TimeoutError, OSError, ConnectionRefusedError) as exc:
                self._log.error(f"TCP connect failed pid={pid} {host}:{port} — {exc}")
                await self._send_signal(
                    pid,
                    PacketType.RESPONSE | PacketType.STREAM | PacketType.CLOSE
                )
                return

            self._log.info(f"TCP connected pid={pid} → {host}:{port}")

            state = StreamState(pid, reader, writer)
            async with self._stream_lock:
                self._streams[pid] = state

            # Start pump AFTER state is stored so pump finds it on first check
            state.pump = asyncio.ensure_future(self._stream_pump(pid))

        if body:
            try:
                self._log.debug(f"write pid={pid} bytes={len(body)}")
                state.writer.write(body)
                await state.writer.drain()
            except (ConnectionResetError, BrokenPipeError, OSError) as exc:
                self._log.info(f"write error pid={pid} — {exc}")
                await self._close_stream(pid)
                await self._send_signal(
                    pid,
                    PacketType.RESPONSE | PacketType.STREAM | PacketType.CLOSE
                )

    # ── Continuous upstream reader ────────────────────────────────────────────
    async def _stream_pump(self, pid: int):
        self._log.info(f"pump started pid={pid}")

        async with self._stream_lock:
            state = self._streams.get(pid)
        if state is None:
            self._log.warning(f"pump: no state at start pid={pid}")
            return

        reader = state.reader

        try:
            while state.is_open():
                try:
                    data = await asyncio.wait_for(reader.read(65536), timeout=30.0)
                except asyncio.TimeoutError:
                    self._log.debug(f"pump idle 30s pid={pid} — looping")
                    continue
                except (ConnectionResetError, OSError) as exc:
                    self._log.info(f"pump read error pid={pid} — {exc}")
                    await self._close_stream(pid)
                    await self._send_signal(
                        pid,
                        PacketType.RESPONSE | PacketType.STREAM | PacketType.CLOSE
                    )
                    return

                if not data:
                    self._log.info(f"pump upstream EOF pid={pid}")
                    await self._close_stream(pid)
                    await self._send_signal(
                        pid,
                        PacketType.RESPONSE | PacketType.STREAM | PacketType.EOF
                    )
                    return

                self._log.debug(f"pump read pid={pid} bytes={len(data)}")

                response = Packet(ptype=PacketType.RESPONSE | PacketType.STREAM)
                response.pid = pid
                response.set('body', data)
                # timestamp is stamped by handler.handle() — used for ordering
                await self._handler.handle(response)

        except asyncio.CancelledError:
            self._log.info(f"pump cancelled pid={pid}")
            raise

    # ── Helpers ───────────────────────────────────────────────────────────────
    async def _close_stream(self, pid: int):
        async with self._stream_lock:
            state = self._streams.pop(pid, None)
        if state:
            await state.close()

    async def _send_signal(self, pid: int, ptype: int):
        self._log.info(f"signal pid={pid} ptype={PacketType.name(ptype)}")
        packet = Packet(ptype=ptype)
        packet.pid = pid
        packet.set('body', b'')
        # timestamp stamped by handle() — ensures signal is ordered after all
        # data chunks already in flight through the pump
        await self._handler.handle(packet)