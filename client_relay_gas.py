import typing
import asyncio
import json

import aiohttp

import logging

from core.shared import class_logger, RelayBase, RelayRequest


class GASRelay(RelayBase):
    _log    = class_logger(__name__, 'GASRelay')
    GAS_URL = 'https://script.google.com/macros/s/AKfycbx2U1r5zYHhPs45LLMVb09mS-9iUbJbYg6DbXfBaC658JXO-NGKbYYMDVsiwyY4sbqJ/exec'
    ROLE    = 'client'

    def __init__(self, handler):
        self._handler       = handler
        self._session       : typing.Optional[aiohttp.ClientSession] = None
        self._session_ready = asyncio.Event()

    async def start(self):
        self._log.info("scheduling _run()")
        asyncio.ensure_future(self._run())

    async def _run(self):
        self._log.info("opening aiohttp session")
        async with aiohttp.ClientSession() as session:
            self._session = session
            self._session_ready.set()
            self._log.info("session ready — entering fallback puller")
            await self._puller()

    # Primary path: POST packets → GAS → server /push → responses inline.
    # Fire-and-forget via ensure_future in HandlerBase._submit — never blocks.
    async def send(self, request: RelayRequest):
        await self._session_ready.wait()

        self._log.info(
            f"POST {len(request.packets)} packet(s) source={request.source!r} "
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
                    timeout=aiohttp.ClientTimeout(total=60)
                ) as r:

                text = await r.text()
                self._log.info(f"POST status={r.status} body_len={len(text)}")
                self._log.debug(f"raw response: {text[:500]}")

                if r.status != 200 or not text.strip():
                    self._log.warning(f"unexpected status={r.status} or empty body")
                    return

                body = json.loads(text)

                if not body.get('ok'):
                    self._log.error(f"GAS error: {body.get('error')}")
                    return

                if 'responses' in body:
                    responses = body['responses']
                    # Sort by timestamp (stream ordering) then pid (plain ordering)
                    responses.sort(key=lambda p: (p.get('timestamp', 0), p.get('pid', 0)))
                    self._log.info(
                        f"{len(responses)} response(s) inline "
                        f"pids={[p.get('pid') for p in responses]}"
                    )
                    await self._handler.receive(responses)

                elif body.get('fallback'):
                    self._log.warning("GAS used fallback cache — puller will collect")

                else:
                    self._log.debug(f"acknowledged queued={body.get('queued')}")

        except asyncio.TimeoutError:
            self._log.error("POST timeout (60s)")
        except aiohttp.ClientConnectionError as exc:
            self._log.error(f"connection error — {exc}")
        except Exception as exc:
            self._log.error(f"unexpected error — {exc}", exc_info=True)

    # Fallback puller: drains queue:client when inline path was unavailable.
    async def _puller(self):
        consecutive_errors = 0
        while True:
            self._log.debug("GET open (fallback — waiting for cached responses)")
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