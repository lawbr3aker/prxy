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
    GAS_URL     = 'https://script.google.com/macros/s/AKfycbyHKhzCtoi6ofiur5d8frZWGRtqhDGskY7EsnnDT5IsJaXVN8n4S2f27-M1F33mrdZv/exec'
    LISTEN_PORT = 8888

    def __init__(self, handler):
        self._handler    = handler
        self._session:    typing.Optional[aiohttp.ClientSession] = None
        self._dispatcher: typing.Optional[Dispatcher] = None

    async def start(self
        ):
        self._dispatcher = Dispatcher(self._handler)
        await self._dispatcher.start()

        # HTTP server — GAS calls POST /push synchronously
        app = web.Application()
        app.router.add_post('/push', self._on_push)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', self.LISTEN_PORT)
        await site.start()
        logger.info(f"server GASRelay listening on 0.0.0.0:{self.LISTEN_PORT}")

        # Session for sending responses back to GAS (fallback path only)
        asyncio.ensure_future(self._run())

    async def _run(self
        ):
        async with aiohttp.ClientSession() as session:
            self._session = session
            # Fallback puller — only used when SERVER_URL is not set in GAS
            # and GAS cached packets under queue:server instead
            await self._puller()

    # Primary path: GAS calls this endpoint synchronously with a batch of
    # request packets. We dispatch all of them, collect responses, return them
    # all in the HTTP response body so GAS can return them to the client inline.
    async def _on_push(self,
            request: web.Request
        ) -> web.Response:
        try:
            body    = await request.json()
            packets = body.get('packets', [])
            logger.info(f"/push received {len(packets)} packet(s)")

            # Dispatch all packets concurrently and wait for all responses
            responses = await asyncio.gather(
                *[self._dispatcher.dispatch(p) for p in packets]
            )

            serialized = [r.serialize() for r in responses if r is not None]
            return web.Response(
                text=json.dumps({ 'ok': True, 'responses': serialized }),
                content_type='application/json'
            )

        except Exception as e:
            logger.error(f"/push error: {e}")
            return web.Response(
                text=json.dumps({ 'ok': False, 'error': str(e) }),
                status=500,
                content_type='application/json'
            )

    # Fallback path: GAS could not reach us directly, cached packets instead.
    # Puller drains them and sends responses back via GAS POST.
    async def _puller(self
        ):
        while True:
            try:
                logger.debug("GAS GET open (fallback, waiting for cached requests)...")
                async with self._session.get(
                        self.GAS_URL,
                        params={'role': 'server'},
                        timeout=aiohttp.ClientTimeout(total=55)
                    ) as r:

                    if r.status == 204:
                        continue

                    text = await r.text()
                    if not text.strip():
                        continue

                    data = json.loads(text)
                    if isinstance(data, dict):
                        data = [data]

                    data.sort(key=lambda p: p.get('pid', 0))
                    logger.info(f"GAS → server fallback: {len(data)} packet(s)")
                    await self._handler.receive(data)

            except asyncio.TimeoutError:
                logger.debug("GAS GET timeout, reconnecting")
            except Exception as e:
                logger.error(f"GAS GET error: {e}")
                await asyncio.sleep(2)

    # Fallback send: only called when server dispatched via puller path,
    # needs to POST responses back to GAS cache for client puller to collect.
    async def send(self,
            request: RelayRequest
        ):
        if self._session is None:
            return

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
                logger.info(f"GAS POST {r.status} (fallback response)")
        except Exception as e:
            logger.error(f"GAS POST error: {e}")