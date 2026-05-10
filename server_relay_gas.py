import typing
import asyncio
import json

import aiohttp
from aiohttp import web

import logging

from core.shared import class_logger, RelayBase, RelayRequest
from core.server import Dispatcher


class GASRelay(RelayBase):
    _log        = class_logger(__name__, 'GASRelay')
    GAS_URL     = 'https://script.google.com/macros/s/AKfycbx2U1r5zYHhPs45LLMVb09mS-9iUbJbYg6DbXfBaC658JXO-NGKbYYMDVsiwyY4sbqJ/exec'
    ROLE        = 'server'
    LISTEN_PORT = 8888

    def __init__(self, handler):
        self._handler    = handler
        self._session    : typing.Optional[aiohttp.ClientSession] = None
        self._dispatcher : typing.Optional[Dispatcher]            = None
        self._session_ready = asyncio.Event()

    async def start(self):
        self._dispatcher = Dispatcher(self._handler)
        await self._dispatcher.start()
        self._log.info("dispatcher ready")

        app = web.Application()
        app.router.add_post('/', self._on_push)

        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, '0.0.0.0', self.LISTEN_PORT).start()
        self._log.info(f"listening on 0.0.0.0:{self.LISTEN_PORT}")

        asyncio.ensure_future(self._run())

    async def _run(self):
        self._log.info("opening aiohttp session")
        async with aiohttp.ClientSession() as session:
            self._session = session
            self._session_ready.set()
            self._log.info("session ready — entering fallback puller")
            await self._puller()

    # Primary path: GAS POSTs a batch here synchronously.
    # Dispatch all packets concurrently; return plain HTTP responses inline.
    # Stream packets return None — pump delivers their responses via send().
    async def _on_push(self, request: web.Request) -> web.Response:
        try:
            body    = await request.json()
            packets = body.get('packets', [])

            self._log.info(
                f"_on_push: {len(packets)} packet(s) "
                f"pids={[p.get('pid') for p in packets]}"
            )
            for p in packets:
                self._log.debug(
                    f"pid={p.get('pid')} ptype={p.get('ptype')} "
                    f"ts={p.get('timestamp')} "
                    f"dst={p.get('destination')} method={p.get('method')} "
                    f"body={len(p.get('body') or '') if p.get('body') else 0}B"
                )

            responses = await asyncio.gather(
                *[self._dispatcher.dispatch(p) for p in packets],
                return_exceptions=False
            )

            valid      = [r for r in responses if r is not None]
            serialized = [r.serialize() for r in valid]

            self._log.info(
                f"_on_push: {len(serialized)} inline response(s) "
                f"pids={[r.pid for r in valid]}"
            )

            return web.Response(
                text=json.dumps({'ok': True, 'responses': serialized}),
                content_type='application/json'
            )

        except Exception as exc:
            self._log.error(f"_on_push error — {exc}", exc_info=True)
            return web.Response(
                text=json.dumps({'ok': False, 'error': str(exc)}),
                status=500,
                content_type='application/json'
            )

    # Fallback send: called by Dispatcher._stream_pump via Handler._submit().
    # POSTs response packets to GAS cache so the client puller can collect them.
    # Fire-and-forget via ensure_future — never blocks the pump.
    async def send(self, request: RelayRequest):
        await self._session_ready.wait()

        self._log.info(
            f"POST {len(request.packets)} response(s) source={request.source!r} "
            f"pids={[p.pid for p in request.packets]}"
        )
        for p in request.packets:
            self._log.debug(
                f"pid={p.pid} ptype={p.ptype} ts={p.timestamp} "
                f"body={len(p.get('body') or b'') if p.get('body') else 0}B"
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
                self._log.info(f"POST status={r.status} response: {text[:200]}")

        except asyncio.TimeoutError:
            self._log.error("POST timeout (30s)")
        except aiohttp.ClientConnectionError as exc:
            self._log.error(f"connection error — {exc}")
        except Exception as exc:
            self._log.error(f"unexpected error — {exc}", exc_info=True)

    # Fallback puller: drains queue:server when GAS cannot reach /
    async def _puller(self):
        consecutive_errors = 0
        while True:
            self._log.debug("GET open (fallback — waiting for cached requests)")
            try:
                async with self._session.get(
                        self.GAS_URL,
                        params={'role': self.ROLE},
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
                    if not data:
                        continue

                    data.sort(key=lambda p: (p.get('timestamp', 0), p.get('pid', 0)))
                    self._log.info(
                        f"puller: {len(data)} packet(s) from fallback cache "
                        f"pids={[p.get('pid') for p in data]}"
                    )
                    await self._handler.receive(data)

            except asyncio.TimeoutError:
                self._log.debug("long-poll timeout — reconnecting")
                consecutive_errors = 0
            except aiohttp.ClientConnectionError as exc:
                consecutive_errors += 1
                wait = min(2 ** consecutive_errors, 30)
                self._log.error(f"connection error ({consecutive_errors}) retry in {wait}s: {exc}")
                await asyncio.sleep(wait)
            except Exception as exc:
                consecutive_errors += 1
                wait = min(2 ** consecutive_errors, 30)
                self._log.error(f"unexpected error ({consecutive_errors}) retry in {wait}s: {exc}", exc_info=True)
                await asyncio.sleep(wait)