import typing
import asyncio
import json

import aiohttp
from aiohttp import web

import logging
#
logger = logging.getLogger(__name__)

from core.shared import RelayBase, RelayRequest
from core.server import Dispatcher


class GASRelay(RelayBase):
    GAS_URL     = 'https://script.google.com/macros/s/YOUR_DEPLOYMENT_ID/exec'
    LISTEN_PORT = 8888

    def __init__(self, handler):
        self._handler    = handler
        self._session    : typing.Optional[aiohttp.ClientSession] = None
        self._dispatcher : typing.Optional[Dispatcher]            = None
        # Event set once the session is open — send() waits on this before
        # any POST so there is no race between start() and the first send().
        self._session_ready = asyncio.Event()

    async def start(self
        ):
        self._dispatcher = Dispatcher(self._handler)
        await self._dispatcher.start()
        logger.info("GASRelay.start: dispatcher ready")

        app = web.Application()
        app.router.add_post('/push', self._on_push)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', self.LISTEN_PORT)
        await site.start()
        logger.info(f"GASRelay.start: /push listener on 0.0.0.0:{self.LISTEN_PORT}")

        asyncio.ensure_future(self._run())

    async def _run(self
        ):
        logger.info("GASRelay._run: opening aiohttp session")
        async with aiohttp.ClientSession() as session:
            self._session = session
            self._session_ready.set()
            logger.info("GASRelay._run: session ready — entering fallback puller")
            await self._puller()

    # Primary path: GAS calls POST /push synchronously with a batch of request
    # packets. Dispatch all concurrently, return plain HTTP responses inline.
    # Stream packets return None — pump delivers their responses via send().
    async def _on_push(self,
            request: web.Request
        ) -> web.Response:
        try:
            body    = await request.json()
            packets = body.get('packets', [])

            logger.info(
                f"GASRelay._on_push: {len(packets)} packet(s) "
                f"pids={[p.get('pid') for p in packets]}"
            )
            for p in packets:
                logger.debug(
                    f"GASRelay._on_push: pid={p.get('pid')} "
                    f"ptype={p.get('ptype')} seq={p.get('seq', 0)} "
                    f"dst={p.get('d') or p.get('destination')} "
                    f"method={p.get('m') or p.get('method')} "
                    f"body={len(p.get('b') or '') if p.get('b') else 0}B"
                )

            responses = await asyncio.gather(
                *[self._dispatcher.dispatch(p) for p in packets],
                return_exceptions=False
            )

            valid      = [r for r in responses if r is not None]
            serialized = [r.serialize() for r in valid]

            logger.info(
                f"GASRelay._on_push: returning {len(serialized)} inline response(s) "
                f"pids={[r.pid for r in valid]}"
            )
            for r in valid:
                logger.debug(
                    f"GASRelay._on_push: response pid={r.pid} "
                    f"seq={r.seq} status={r.get('status')} "
                    f"body={len(r.get('b') or b'') if r.get('b') else 0}B"
                )

            return web.Response(
                text=json.dumps({'ok': True, 'responses': serialized}),
                content_type='application/json'
            )

        except Exception as exc:
            logger.error(f"GASRelay._on_push: error — {exc}", exc_info=True)
            return web.Response(
                text=json.dumps({'ok': False, 'error': str(exc)}),
                status=500,
                content_type='application/json'
            )

    # Fallback puller: GAS could not reach /push — packets cached under
    # queue:server. Drain and dispatch them here; responses go back via send().
    async def _puller(self
        ):
        consecutive_errors = 0

        while True:
            logger.debug("GASRelay._puller: GET open (waiting for cached requests)")
            try:
                async with self._session.get(
                        self.GAS_URL,
                        params={'role': 'server'},
                        timeout=aiohttp.ClientTimeout(total=55)
                    ) as r:

                    consecutive_errors = 0
                    logger.debug(f"GASRelay._puller: GET status={r.status}")

                    if r.status == 204:
                        continue

                    text = await r.text()
                    if not text.strip():
                        logger.debug("GASRelay._puller: empty body — reconnecting")
                        continue

                    logger.debug(f"GASRelay._puller: raw body: {text[:500]}")

                    data = json.loads(text)
                    if isinstance(data, dict):
                        data = [data]
                    if not data:
                        continue

                    data.sort(key=lambda p: (p.get('seq', 0), p.get('pid', 0)))
                    logger.info(
                        f"GASRelay._puller: {len(data)} packet(s) from fallback cache "
                        f"pids={[p.get('pid') for p in data]}"
                    )
                    for p in data:
                        logger.debug(
                            f"GASRelay._puller: pid={p.get('pid')} "
                            f"ptype={p.get('ptype')} seq={p.get('seq', 0)} "
                            f"dst={p.get('d') or p.get('destination')} "
                            f"body={len(p.get('b') or '') if p.get('b') else 0}B"
                        )
                    await self._handler.receive(data)

            except asyncio.TimeoutError:
                logger.debug("GASRelay._puller: long-poll timeout — reconnecting")
                consecutive_errors = 0

            except aiohttp.ClientConnectionError as exc:
                consecutive_errors += 1
                wait = min(2 ** consecutive_errors, 30)
                logger.error(
                    f"GASRelay._puller: connection error ({consecutive_errors}) — "
                    f"retry in {wait}s: {exc}"
                )
                await asyncio.sleep(wait)

            except Exception as exc:
                consecutive_errors += 1
                wait = min(2 ** consecutive_errors, 30)
                logger.error(
                    f"GASRelay._puller: unexpected error ({consecutive_errors}) — "
                    f"retry in {wait}s: {exc}",
                    exc_info=True
                )
                await asyncio.sleep(wait)

    # Fallback send: called by Dispatcher._stream_pump (and plain HTTP fallback)
    # via Handler.submit(). POSTs response packets to GAS cache so the client
    # puller can collect them.
    async def send(self,
            request: RelayRequest
        ):
        await self._session_ready.wait()

        packets_info = [
            f"pid={p.pid} ptype={p.ptype} seq={p.seq} "
            f"status={p.get('status')} "
            f"body={len(p.get('b') or b'') if p.get('b') else 0}B"
            for p in request.packets
        ]
        logger.info(
            f"GASRelay.send: POST {len(request.packets)} response(s) "
            f"source={request.source!r} (fallback) | {packets_info}"
        )

        payload = json.dumps({
            'source':  request.source,
            'packets': [p.serialize() for p in request.packets]
        })

        try:
            async with self._session.post(
                    self.GAS_URL,
                    data=payload,
                    headers={'Content-Type': 'application/json'},
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as r:
                text = await r.text()
                logger.info(
                    f"GASRelay.send: POST status={r.status} body: {text[:200]}"
                )

        except asyncio.TimeoutError:
            logger.error("GASRelay.send: POST timeout (30s)")
        except aiohttp.ClientConnectionError as exc:
            logger.error(f"GASRelay.send: connection error — {exc}")
        except Exception as exc:
            logger.error(f"GASRelay.send: unexpected error — {exc}", exc_info=True)