import typing
import asyncio
import json

import aiohttp
from aiohttp import web

import logging
#
logger = logging.getLogger(__name__)

from core.shared import RelayBase, RelayRequest, Packet
from core.server import Dispatcher


class GASRelay(RelayBase):
    GAS_URL     = 'https://script.google.com/macros/s/AKfycbxzDxEe1NyqFU9SVPV2C7sdZpp5XKWMzDAsgdp5Y1grEwut-2SkZ3R4-c0G31dCrjsc/exec'
    LISTEN_PORT = 8888

    def __init__(self, handler):
        self._handler    = handler
        self._session:    typing.Optional[aiohttp.ClientSession] = None
        self._dispatcher: typing.Optional[Dispatcher]            = None

    async def start(self
        ):
        self._dispatcher = Dispatcher(self._handler)
        await self._dispatcher.start()
        logger.info("GASRelay.start: dispatcher ready")

        # HTTP server — GAS calls POST /push synchronously
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
            logger.info(
                "GASRelay._run: session ready — entering fallback puller"
            )
            await self._puller()

    # Primary path: GAS calls this endpoint synchronously with a batch of
    # request packets. We dispatch all of them concurrently, collect responses,
    # and return them all in the HTTP body so GAS forwards them inline.
    async def _on_push(self,
            request: web.Request
        ) -> web.Response:
        try:
            body    = await request.json()
            packets = body.get('packets', [])

            logger.info(
                f"GASRelay._on_push: received {len(packets)} packet(s) "
                f"pids={[p.get('pid') for p in packets]}"
            )
            for p in packets:
                logger.debug(
                    f"GASRelay._on_push: packet pid={p.get('pid')} "
                    f"ptype={p.get('ptype')} "
                    f"destination={p.get('d') or p.get('destination')} "
                    f"method={p.get('m') or p.get('method')} "
                    f"body={len(p.get('b') or '') if p.get('b') else 0}B"
                )

            # Dispatch all packets concurrently and wait for all responses
            responses = await asyncio.gather(
                *[self._dispatcher.dispatch(p) for p in packets],
                return_exceptions=False
            )

            valid     = [r for r in responses if r is not None]
            serialized = [r.serialize() for r in valid]

            logger.info(
                f"GASRelay._on_push: returning {len(serialized)} response(s) inline "
                f"pids={[r.pid for r in valid]}"
            )
            for r in valid:
                logger.debug(
                    f"GASRelay._on_push: response pid={r.pid} "
                    f"status={r.get('status')} "
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

    # Fallback path: GAS could not reach us directly — cached packets instead.
    # Puller drains them and sends responses back via GAS POST.
    async def _puller(self
        ):
        consecutive_errors = 0

        while True:
            logger.debug(
                "GASRelay._puller: GET open (fallback — waiting for cached requests)..."
            )
            try:
                async with self._session.get(
                        self.GAS_URL,
                        params={'role': 'server'},
                        timeout=aiohttp.ClientTimeout(total=55)
                    ) as r:

                    consecutive_errors = 0
                    logger.debug(
                        f"GASRelay._puller: GET returned status={r.status}"
                    )

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
                        logger.debug("GASRelay._puller: empty list — reconnecting")
                        continue

                    data.sort(key=lambda p: p.get('pid', 0))
                    logger.info(
                        f"GASRelay._puller: {len(data)} packet(s) from fallback cache "
                        f"pids={[p.get('pid') for p in data]}"
                    )
                    for p in data:
                        logger.debug(
                            f"GASRelay._puller: packet pid={p.get('pid')} "
                            f"ptype={p.get('ptype')} "
                            f"destination={p.get('d') or p.get('destination')} "
                            f"body={len(p.get('b') or '') if p.get('b') else 0}B"
                        )
                    await self._handler.receive(data)

            except asyncio.TimeoutError:
                logger.debug("GASRelay._puller: GET long-poll timeout — reconnecting")
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

    # Fallback send: only called via the puller path.
    # POSTs response packets to GAS cache so the client puller can collect them.
    async def send(self,
            request: RelayRequest
        ):
        if self._session is None:
            logger.warning("GASRelay.send: session not ready — dropping request")
            return

        packets_info = [
            f"pid={p.pid} status={p.get('status')} "
            f"body={len(p.get('b') or b'') if p.get('b') else 0}B"
            for p in request.packets
        ]
        logger.info(
            f"GASRelay.send: POST {len(request.packets)} response(s) back to GAS cache "
            f"(fallback) | {packets_info}"
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
                    f"GASRelay.send: POST response status={r.status} body: {text[:200]}"
                )

        except asyncio.TimeoutError:
            logger.error("GASRelay.send: POST timeout (30s)")
        except aiohttp.ClientConnectionError as exc:
            logger.error(f"GASRelay.send: connection error — {exc}")
        except Exception as exc:
            logger.error(f"GASRelay.send: unexpected error — {exc}", exc_info=True)
