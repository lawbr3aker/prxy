import typing
import asyncio
import time
import ssl

import aiohttp

import logging
#
logger = logging.getLogger(__name__)

from .shared import (
    IDGenerator,
    PacketType,
    Packet,
    PacketQueue,
    PacketPool,
    StreamState,
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

    # Inbound: relay deposits raw dicts from GAS/test → Packet objects
    async def receive(self,
            raw_packets: list[dict]
        ):
        packets = [Packet.deserialize(raw) for raw in raw_packets]
        logger.info(
            f"Handler.receive: depositing {len(packets)} packet(s) into pool "
            f"pids={[p.pid for p in packets]}"
        )
        await self._pool.put_many(packets)

    # Outbound: stamp and send.
    # STREAM packets skip the batching queue — sent immediately.
    async def handle(self,
            packet: Packet
        ):
        packet.timestamp = int(time.time() * 1000)
        if packet.pid is None:
            packet.pid = IDGenerator.generate(packet.timestamp)

        logger.debug(f"Handler.handle: {packet!r}")

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

        batch = None
        async with self._lock:
            if self._queue is queue:
                batch       = queue._queue[:]
                self._queue = None
                logger.debug(
                    f"Handler.handle: flush winner — {len(batch)} packet(s) "
                    f"pids={[p.pid for p in batch]}"
                )
            else:
                logger.debug(
                    f"Handler.handle: flush loser — pid={packet.pid} already batched"
                )

        if batch is not None:
            await self.submit(batch)

    async def pool(self
        ) -> PacketPool:
        return self._pool

    async def submit(self,
            batch: list[Packet]
        ):
        request = RelayRequest(packets=batch, source='server')
        logger.info(f"Handler.submit: {request!r}")
        asyncio.ensure_future(self._relay.send(request))


class Dispatcher:
    """
    Makes real HTTP/TCP calls on behalf of the client.

    Primary path — relay calls dispatch(raw) directly; plain HTTP returns a
    Packet inline; STREAM chunks return None (pump pushes responses back).

    Fallback path — relay calls handler.receive() which fills the pool;
    _run() drains and dispatches.

    STREAM lifecycle
    ────────────────
    First DATA chunk for a pid:
      • Opens TCP connection → creates StreamState.
      • Starts _stream_pump task.
      • Writes data chunk.
      • Returns None.

    Subsequent DATA chunks:
      • Writes to existing StreamState.writer.
      • Returns None.

    CLOSE chunk from client:
      • Closes StreamState (cancels pump, closes writer).
      • Sends CLOSE response back to client.
      • Returns None.

    _stream_pump:
      • Reads from StreamState.reader in a tight loop.
      • Stamps each chunk with a monotonically increasing seq.
      • Calls handler.handle() → relay.send() → client StreamRegistry.
      • On EOF or error, sends EOF/CLOSE and exits.
    """

    def __init__(self, handler: Handler):
        self._handler     = handler
        self._session     : typing.Optional[aiohttp.ClientSession] = None
        self._streams     : dict[int, StreamState]                 = {}
        self._stream_lock = asyncio.Lock()

    async def start(self
        ):
        ssl_context                  = ssl.create_default_context()
        ssl_context.check_hostname   = False
        ssl_context.verify_mode      = ssl.CERT_NONE
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=ssl_context)
        )
        logger.info("Dispatcher.start: aiohttp session created")
        asyncio.ensure_future(self._run())
        logger.info("Dispatcher.start: fallback _run() loop started")

    async def stop(self
        ):
        async with self._stream_lock:
            states = list(self._streams.values())
            self._streams.clear()
        for state in states:
            await state.close()
        if self._session:
            await self._session.close()
        logger.info("Dispatcher.stop: all streams closed, session closed")

    # Primary path
    async def dispatch(self,
            raw: dict
        ) -> typing.Optional[Packet]:
        packet = Packet.deserialize(raw)
        logger.debug(f"Dispatcher.dispatch: {packet!r}")

        if packet.ptype & PacketType.STREAM:
            await self._do_stream(packet)
            return None

        return await self._do_request(packet)

    # Fallback path
    async def _run(self
        ):
        logger.debug("Dispatcher._run: fallback loop started")
        while True:
            pool   = await self._handler.pool()
            packet = await pool.find(
                lambda p: bool(p.ptype & PacketType.REQUEST)
            )
            if packet is None:
                await asyncio.sleep(0.01)
                continue
            logger.debug(f"Dispatcher._run: fallback dispatching {packet!r}")
            asyncio.ensure_future(self._fallback_dispatch(packet))

    async def _fallback_dispatch(self,
            packet: Packet
        ):
        if packet.ptype & PacketType.STREAM:
            await self._do_stream(packet)
        else:
            response = await self._do_request(packet)
            if response:
                await self._handler.handle(response)

    # ── Plain HTTP ────────────────────────────────────────────────────────────
    async def _do_request(self,
            packet: Packet
        ) -> typing.Optional[Packet]:
        destination = packet.get('d') or packet.get('destination') or ''
        method      = (packet.get('m') or packet.get('method') or 'GET').upper()
        body        = packet.get('b') or packet.get('body')
        headers     = packet.get('headers') or {}

        if not destination.startswith('http'):
            destination = 'http://' + destination

        if isinstance(body, str):
            body = body.encode('latin-1')

        logger.info(
            f"Dispatcher._do_request: {method} {destination} "
            f"body={len(body) if body else 0}B"
        )

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

                logger.info(
                    f"Dispatcher._do_request: pid={packet.pid} {method} {destination} "
                    f"→ {r.status} body={len(body_bytes)}B"
                )

                response = Packet(ptype=PacketType.RESPONSE)
                response.pid = packet.pid
                response.set('status',  r.status)
                response.set('b',       body_bytes)
                response.set('headers', dict(r.headers))

        except asyncio.TimeoutError:
            logger.warning(
                f"Dispatcher._do_request: timeout pid={packet.pid} "
                f"{method} {destination}"
            )
            response = Packet(ptype=PacketType.RESPONSE)
            response.pid = packet.pid
            response.set('status', 504)
            response.set('b',      b'Gateway Timeout')

        except aiohttp.ClientConnectionError as exc:
            logger.error(
                f"Dispatcher._do_request: connection error pid={packet.pid} "
                f"{method} {destination} — {exc}"
            )
            response = Packet(ptype=PacketType.RESPONSE)
            response.pid = packet.pid
            response.set('status', 502)
            response.set('b',      str(exc).encode())

        except Exception as exc:
            logger.error(
                f"Dispatcher._do_request: unexpected error pid={packet.pid} — {exc}",
                exc_info=True
            )
            response = Packet(ptype=PacketType.RESPONSE)
            response.pid = packet.pid
            response.set('status', 502)
            response.set('b',      str(exc).encode())

        return response

    # ── STREAM chunk handler ──────────────────────────────────────────────────
    async def _do_stream(self,
            packet: Packet
        ):
        pid   = packet.pid
        ptype = packet.ptype
        body  = packet.get('b') or b''
        if isinstance(body, str):
            body = body.encode('latin-1')

        logger.debug(
            f"Dispatcher._do_stream: pid={pid} "
            f"ptype={PacketType.name(ptype)} "
            f"bytes={len(body)} "
            f"dst={packet.get('destination')}"
        )

        # ── CLOSE from client ─────────────────────────────────────────────────
        if ptype & PacketType.CLOSE:
            logger.info(f"Dispatcher._do_stream: CLOSE pid={pid}")
            await self._close_stream(pid)
            await self._send_signal(pid, PacketType.RESPONSE | PacketType.STREAM | PacketType.CLOSE)
            return

        async with self._stream_lock:
            state = self._streams.get(pid)

        # ── First chunk — open TCP and start pump ─────────────────────────────
        if state is None:
            destination = packet.get('destination') or ''
            host, _, port_str = destination.rpartition(':')
            port = int(port_str) if port_str.isdigit() else 443
            if not host:
                host = destination

            logger.info(
                f"Dispatcher._do_stream: opening TCP pid={pid} → {host}:{port}"
            )
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port),
                    timeout=10.0
                )
            except asyncio.TimeoutError:
                logger.warning(
                    f"Dispatcher._do_stream: TCP connect timeout pid={pid} "
                    f"{host}:{port}"
                )
                await self._send_signal(
                    pid,
                    PacketType.RESPONSE | PacketType.STREAM | PacketType.CLOSE
                )
                return
            except (OSError, ConnectionRefusedError) as exc:
                logger.error(
                    f"Dispatcher._do_stream: TCP connect failed pid={pid} "
                    f"{host}:{port} — {exc}"
                )
                await self._send_signal(
                    pid,
                    PacketType.RESPONSE | PacketType.STREAM | PacketType.CLOSE
                )
                return

            logger.info(
                f"Dispatcher._do_stream: TCP connected pid={pid} → {host}:{port}"
            )

            state = StreamState(pid, reader, writer)
            async with self._stream_lock:
                self._streams[pid] = state

            # Start pump before writing — upstream may send data immediately
            # after the first write (e.g. TLS ServerHello)
            state.pump = asyncio.ensure_future(self._stream_pump(pid))

        # ── Write data to upstream ────────────────────────────────────────────
        if body:
            try:
                logger.debug(
                    f"Dispatcher._do_stream: write pid={pid} bytes={len(body)}"
                )
                state.writer.write(body)
                await state.writer.drain()
            except (ConnectionResetError, BrokenPipeError, OSError) as exc:
                logger.info(
                    f"Dispatcher._do_stream: write error pid={pid} — {exc}"
                )
                await self._close_stream(pid)
                await self._send_signal(
                    pid,
                    PacketType.RESPONSE | PacketType.STREAM | PacketType.CLOSE
                )

    # ── Continuous upstream reader ────────────────────────────────────────────
    async def _stream_pump(self,
            pid: int
        ):
        logger.info(f"Dispatcher._stream_pump: started pid={pid}")

        async with self._stream_lock:
            state = self._streams.get(pid)
        if state is None:
            logger.warning(f"Dispatcher._stream_pump: no state at start pid={pid}")
            return

        reader = state.reader

        try:
            while state.is_open():
                try:
                    data = await asyncio.wait_for(reader.read(65536), timeout=30.0)
                except asyncio.TimeoutError:
                    # No upstream data for 30 s — check connection still alive
                    logger.debug(
                        f"Dispatcher._stream_pump: idle 30s pid={pid} — looping"
                    )
                    continue
                except (ConnectionResetError, OSError) as exc:
                    logger.info(
                        f"Dispatcher._stream_pump: read error pid={pid} — {exc}"
                    )
                    await self._close_stream(pid)
                    await self._send_signal(
                        pid,
                        PacketType.RESPONSE | PacketType.STREAM | PacketType.CLOSE
                    )
                    return

                if not data:
                    # TCP EOF — upstream finished sending
                    logger.info(
                        f"Dispatcher._stream_pump: upstream EOF pid={pid}"
                    )
                    await self._close_stream(pid)
                    await self._send_signal(
                        pid,
                        PacketType.RESPONSE | PacketType.STREAM | PacketType.EOF
                    )
                    return

                seq = await state.next_seq()
                logger.debug(
                    f"Dispatcher._stream_pump: read pid={pid} seq={seq} "
                    f"bytes={len(data)}"
                )

                response = Packet(ptype=PacketType.RESPONSE | PacketType.STREAM)
                response.pid = pid
                response.seq = seq
                response.set('b', data)
                await self._handler.handle(response)

        except asyncio.CancelledError:
            logger.info(f"Dispatcher._stream_pump: cancelled pid={pid}")
            raise

    # ── Helpers ───────────────────────────────────────────────────────────────
    async def _close_stream(self,
            pid: int
        ):
        async with self._stream_lock:
            state = self._streams.pop(pid, None)
        if state:
            await state.close()

    async def _send_signal(self,
            pid   : int,
            ptype : int
        ):
        logger.info(
            f"Dispatcher._send_signal: pid={pid} "
            f"ptype={PacketType.name(ptype)}"
        )
        packet = Packet(ptype=ptype)
        packet.pid = pid

        # Signal packets also carry a seq so StreamBuffer can order them
        # correctly relative to data chunks already in flight.
        async with self._stream_lock:
            state = self._streams.get(pid)
        if state:
            packet.seq = await state.next_seq()

        packet.set('b', b'')
        await self._handler.handle(packet)
