import typing
import asyncio
import time

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

    # Inbound: relay puller deposits raw dicts from GAS → Packet objects
    async def receive(self,
            raw_packets: list[dict]
        ):
        packets = [Packet.deserialize(raw) for raw in raw_packets]
        logger.info(
            f"Handler.receive: depositing {len(packets)} packet(s) into pool "
            f"pids={[p.pid for p in packets]}"
        )
        await self._pool.put_many(packets)

    # Outbound: batch a response packet and send back to GAS
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

        # BUG FIX: original code used `batch` without checking if we're the flush winner,
        # meaning every coroutine would call submit() even though only one owns the payload.
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
        logger.info(f"Handler.submit: dispatching {request!r}")
        asyncio.ensure_future(self._relay.send(request))


# Dispatcher: makes real HTTP/TCP calls on behalf of the client.
#
# Primary path  — relay calls dispatch(raw_dict) directly per packet,
#                 awaits the response Packet, and returns it inline so
#                 GAS can forward it back to the client in the same round trip.
#
# Fallback path — relay calls handler.receive() which fills the pool;
#                 _run() drains the pool and calls handler.handle() with
#                 each response so send() posts them back to GAS cache.
#
# STREAM packets — each chunk shares a pid; an open aiohttp connection is
#                  kept alive in _stream_sessions keyed by pid until either
#                  side sends a zero-byte close signal.
class Dispatcher:
    def __init__(self, handler: Handler):
        self._handler = handler
        self._session : typing.Optional[aiohttp.ClientSession]                         = None
        # pid → open streaming connection state
        # { 'writer': asyncio.StreamWriter, 'reader': asyncio.StreamReader }
        self._streams : dict[int, dict]                                                = {}
        self._stream_lock = asyncio.Lock()

    async def start(self
        ):
        self._session = aiohttp.ClientSession()
        logger.info("Dispatcher.start: aiohttp session created")
        # Fallback loop — only active when packets arrive via pool (puller path)
        asyncio.ensure_future(self._run())
        logger.info("Dispatcher.start: fallback _run() loop started")

    async def stop(self
        ):
        if self._session:
            await self._session.close()
            logger.info("Dispatcher.stop: aiohttp session closed")

    # Primary path entry point — called directly by relay._on_push()
    async def dispatch(self,
            raw: dict
        ) -> typing.Optional[Packet]:
        packet = Packet.deserialize(raw)
        logger.debug(f"Dispatcher.dispatch: {packet!r}")

        if packet.ptype & PacketType.STREAM:
            return await self._do_stream(packet)
        return await self._do_request(packet)

    # Fallback path loop — drains pool filled by handler.receive()
    async def _run(self
        ):
        logger.debug("Dispatcher._run: fallback loop started")
        while True:
            pool   = await self._handler.pool()
            packet = await pool.find(
                lambda p: bool(p.ptype & PacketType.REQUEST)
            )
            if packet is None:
                await asyncio.sleep(0.05)
                continue
            logger.debug(f"Dispatcher._run: fallback dispatching {packet!r}")
            asyncio.ensure_future(self._dispatch_and_handle(packet))

    async def _dispatch_and_handle(self,
            packet: Packet
        ):
        if packet.ptype & PacketType.STREAM:
            response = await self._do_stream(packet)
        else:
            response = await self._do_request(packet)
        if response:
            logger.debug(
                f"Dispatcher._dispatch_and_handle: handling response {response!r}"
            )
            await self._handler.handle(response)

    # ── Plain HTTP request ────────────────────────────────────────────────────
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
                             if k.lower() not in {'host', 'content-length', 'transfer-encoding'}},
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
                f"Dispatcher._do_request: timeout pid={packet.pid} {method} {destination}"
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

    # ── STREAM (CONNECT tunnel) chunk ─────────────────────────────────────────
    # Each chunk for a given pid is routed through the same TCP connection.
    # A zero-byte chunk signals close from either side.
    async def _do_stream(self,
            packet: Packet
        ) -> typing.Optional[Packet]:
        pid  = packet.pid
        body = packet.get('b') or b''
        if isinstance(body, str):
            body = body.encode('latin-1')

        logger.debug(
            f"Dispatcher._do_stream: pid={pid} bytes={len(body)} "
            f"destination={packet.get('destination')}"
        )

        async with self._stream_lock:
            state = self._streams.get(pid)

        # ── Close signal from client ──────────────────────────────────────────
        if body == b'EOF':
            if state is not None:
                logger.info(
                    f"Dispatcher._do_stream: client close signal pid={pid} — closing TCP"
                )
                try:
                    state['writer'].close()
                    await state['writer'].wait_closed()
                except OSError:
                    pass
                async with self._stream_lock:
                    self._streams.pop(pid, None)
            # Echo close back to client
            return self._send_close_packet(pid)

        # ── Open new TCP connection for this pid ──────────────────────────────
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
                state = {'reader': reader, 'writer': writer}
                async with self._stream_lock:
                    self._streams[pid] = state
                logger.info(
                    f"Dispatcher._do_stream: TCP connected pid={pid} → {host}:{port}"
                )
            except asyncio.TimeoutError:
                logger.warning(
                    f"Dispatcher._do_stream: TCP connect timeout pid={pid} {host}:{port}"
                )
                return self._send_close_packet(pid)
            except (OSError, ConnectionRefusedError) as exc:
                logger.error(
                    f"Dispatcher._do_stream: TCP connect failed pid={pid} {host}:{port} — {exc}"
                )
                return self._send_close_packet(pid)

        # ── Write chunk to upstream ───────────────────────────────────────────
        writer = state['writer']
        reader = state['reader']

        try:
            logger.debug(
                f"Dispatcher._do_stream: write pid={pid} bytes={len(body)}"
            )
            writer.write(body)
            await writer.drain()
        except (ConnectionResetError, BrokenPipeError, OSError) as exc:
            logger.info(
                f"Dispatcher._do_stream: write error pid={pid} — {exc}"
            )
            async with self._stream_lock:
                self._streams.pop(pid, None)
            return self._send_close_packet(pid)

        # ── Read response chunk ───────────────────────────────────────────────
        try:
            response_data = await asyncio.wait_for(
                reader.read(4096),
                timeout=5.0
            )
            logger.debug(
                f"Dispatcher._do_stream: read pid={pid} bytes={len(response_data)}"
            )
        except asyncio.TimeoutError:
            # No data ready yet — return empty non-close chunk
            logger.debug(
                f"Dispatcher._do_stream: read timeout pid={pid} — no data ready"
            )
            response_data = None
        except (ConnectionResetError, OSError) as exc:
            logger.info(
                f"Dispatcher._do_stream: read error pid={pid} — {exc}"
            )
            async with self._stream_lock:
                self._streams.pop(pid, None)
            return self._send_close_packet(pid)

        if response_data is not None and len(response_data) == 0:
            # TCP EOF from upstream
            logger.info(
                f"Dispatcher._do_stream: upstream EOF pid={pid}"
            )
            async with self._stream_lock:
                self._streams.pop(pid, None)
            return self._send_close_packet(pid)

        response = Packet(ptype=PacketType.RESPONSE | PacketType.STREAM)
        response.pid = pid
        response.set('b', response_data or b'')
        return response

    # Send a zero-byte STREAM packet so the client knows to close the tunnel
    async def _send_close_packet(self,
            pid: int
        ):
        logger.info(f"_do_stream: sending close packet pid={pid}")
        packet = Packet(ptype=PacketType.RESPONSE | PacketType.STREAM)
        packet.pid = pid
        packet.set('b', b'EOF')
        
        return packet
