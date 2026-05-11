import typing
import asyncio
import ssl
import json
import aiohttp
import logging

from core.shared import class_logger, RelayBase, RelayRequest


class GASRelay(RelayBase):
    _log    = class_logger(__name__, 'GASRelay')
    GAS_URL = 'https://script.google.com/macros/s/AKfycbwW_z3SuOblecNJz65B1BaKGI4eU7EhMnmpTcA4C488R9slyvhyDGiS1LVyErEkVxw2/exec'
    ROLE    = 'client'

    def __init__(self, handler):
        self._handler       = handler
        self._session       : typing.Optional[aiohttp.ClientSession] = None
        self._session_ready = asyncio.Event()

    async def start(self):
        asyncio.ensure_future(self._run())

    async def _run(self):
        ssl_ctx                = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode    = ssl.CERT_NONE
        async with aiohttp.ClientSession() as session:
            self._session = session
            self._session_ready.set()
            await self._puller()

    async def send(self, request: RelayRequest):
        await self._session_ready.wait()

        payload = json.dumps({
            'source':  request.source,
            'packets': [p.serialize() for p in request.packets],
        })
        self._log.debug(
            f"POST n={len(request.packets)} "
            f"pids={[p.pid for p in request.packets]}"
        )
        try:
            async with self._session.post(
                self.GAS_URL, data=payload,
                headers={'Content-Type': 'application/json'},
                timeout=aiohttp.ClientTimeout(total=60),
            ) as r:
                text = await r.text()
                if r.status != 200 or not text.strip():
                    self._log.warning(f"POST status={r.status}")
                    return
                body = json.loads(text)
                if body.get('fallback'):
                    self._log.warning("GAS fallback — puller will collect")
                elif 'responses' in body:
                    responses = body['responses']
                    # Sort by timestamp so stream chunks arrive in order
                    responses.sort(key=lambda p: p.get('timestamp', 0))
                    self._log.debug(f"inline {len(responses)} responses")
                    await self._handler.receive(responses)
        except Exception as exc:
            self._log.error(f"send error: {exc}", exc_info=True)

    async def _puller(self):
        consecutive_errors = 0
        while True:
            try:
                async with self._session.get(
                    self.GAS_URL, params={'role': self.ROLE},
                    timeout=aiohttp.ClientTimeout(total=55),
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
                        data.sort(key=lambda p: p.get('timestamp', 0))
                        self._log.debug(f"puller {len(data)} packets")
                        await self._handler.receive(data)
            except asyncio.TimeoutError:
                consecutive_errors = 0
            except aiohttp.ClientConnectionError as exc:
                consecutive_errors += 1
                await asyncio.sleep(min(2 ** consecutive_errors, 30))
                self._log.error(f"connection error: {exc}")
            except Exception as exc:
                consecutive_errors += 1
                await asyncio.sleep(min(2 ** consecutive_errors, 30))
                self._log.error(f"puller error: {exc}", exc_info=True)
