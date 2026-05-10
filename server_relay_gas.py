import typing
import asyncio
import json
import aiohttp
from aiohttp import web
import logging

from core.shared import class_logger, RelayBase, RelayRequest
from core.server import Dispatcher

class GASRelay(RelayBase):
    _log = class_logger(__name__, 'GASRelay')
    GAS_URL = 'https://script.google.com/macros/s/AKfycbwW_z3SuOblecNJz65B1BaKGI4eU7EhMnmpTcA4C488R9slyvhyDGiS1LVyErEkVxw2/exec'
    ROLE = 'server'
    LISTEN_PORT = 8888

    def __init__(self, handler):
        self._handler = handler
        self._session: typing.Optional[aiohttp.ClientSession] = None
        self._dispatcher: typing.Optional[Dispatcher] = None
        self._session_ready = asyncio.Event()

    async def start(self):
        self._dispatcher = Dispatcher(self._handler)
        await self._dispatcher.start()

        app = web.Application()
        app.router.add_post('/', self._on_push)
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, '0.0.0.0', self.LISTEN_PORT).start()
        self._log.info(f"listening on 0.0.0.0:{self.LISTEN_PORT}")

        asyncio.ensure_future(self._run())

    async def _run(self):
        async with aiohttp.ClientSession() as session:
            self._session = session
            self._session_ready.set()
            await self._puller()

    async def _on_push(self, request: web.Request) -> web.Response:
        try:
            body = await request.json()
            packets = body.get('packets', [])
            self._log.info(f"_on_push: {len(packets)} packets")
            responses = await asyncio.gather(
                *[self._dispatcher.dispatch(p) for p in packets],
                return_exceptions=False
            )
            valid = [r.serialize() for r in responses if r is not None]
            return web.Response(
                text=json.dumps({'ok': True, 'responses': valid}),
                content_type='application/json'
            )
        except Exception as exc:
            self._log.error(f"_on_push error: {exc}", exc_info=True)
            return web.Response(
                text=json.dumps({'ok': False, 'error': str(exc)}),
                status=500, content_type='application/json'
            )

    async def send(self, request: RelayRequest):
        await self._session_ready.wait()
        payload = json.dumps({
            'source': request.source,
            'packets': [p.serialize() for p in request.packets]
        })
        try:
            async with self._session.post(
                self.GAS_URL, data=payload,
                headers={'Content-Type': 'application/json'},
                timeout=aiohttp.ClientTimeout(total=30)
            ) as r:
                text = await r.text()
                self._log.info(f"POST status={r.status} response: {text[:200]}")
        except Exception as exc:
            self._log.error(f"send error: {exc}", exc_info=True)

    async def _puller(self):
        consecutive_errors = 0
        while True:
            try:
                async with self._session.get(
                    self.GAS_URL, params={'role': self.ROLE},
                    timeout=aiohttp.ClientTimeout(total=55)
                ) as r:
                    consecutive_errors = 0
                    if r.status == 204:
                        continue
                    text = await r.text()
                    if not text.strip():
                        continue
                    data = json.loads(text)
                    if isinstance(data, dict):
                        data = [data]
                    if data:
                        data.sort(key=lambda p: (p.get('timestamp', 0), p.get('seq', 0)))
                        await self._handler.receive(data)
            except asyncio.TimeoutError:
                consecutive_errors = 0
            except aiohttp.ClientConnectionError as exc:
                consecutive_errors += 1
                wait = min(2 ** consecutive_errors, 30)
                self._log.error(f"connection error, retry in {wait}s: {exc}")
                await asyncio.sleep(wait)
            except Exception as exc:
                consecutive_errors += 1
                wait = min(2 ** consecutive_errors, 30)
                self._log.error(f"unexpected error, retry in {wait}s: {exc}", exc_info=True)
                await asyncio.sleep(wait)