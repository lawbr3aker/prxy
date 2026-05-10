import typing
import asyncio
import json

import aiohttp

import logging
#
logger = logging.getLogger(__name__)

from core.shared import RelayBase, RelayRequest


class GASRelay(RelayBase):
    GAS_URL = 'https://script.google.com/macros/s/YOUR_DEPLOYMENT_ID/exec'

    def __init__(self, handler):
        self._handler       = handler
        self._session       : typing.Optional[aiohttp.ClientSession] = None
        # Event set once the session is open — send() waits on this before
        # any POST so there is no race between start() and the first send().
        self._session_ready = asyncio.Event()

    async def start(self
        ):
        logger.info("GASRelay.start: scheduling _run()")
        asyncio.ensure_future(self._run())

    async def _run(self
        ):
        logger.info("GASRelay._run: opening aiohttp session")
        async with aiohttp.ClientSession() as session:
            self._session = session
            self._session_ready.set()
            logger.info("GASRelay._run: session ready — entering fallback puller")
            await self._puller()

    # Primary path: POST request packets to GAS.
    # GAS forwards to server synchronously and returns responses inline.
    # On fallback (server unreachable), GAS caches; _puller collects later.
    async def send(self,
            request: RelayRequest
        ):
        await self._session_ready.wait()

        packets_info = [
            f"pid={p.pid} ptype={p.ptype} seq={p.seq} "
            f"body={len(p.get('b') or b'') if p.get('b') else 0}B"
            for p in request.packets
        ]
        logger.info(
            f"GASRelay.send: POST {len(request.packets)} packet(s) "
            f"source={request.source!r} | {packets_info}"
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
                logger.info(
                    f"GASRelay.send: status={r.status} body_len={len(text)}"
                )
                logger.debug(f"GASRelay.send: raw response: {text[:500]}")

                if r.status != 200 or not text.strip():
                    logger.warning(
                        f"GASRelay.send: unexpected status={r.status} or empty body"
                    )
                    return

                body = json.loads(text)

                if not body.get('ok'):
                    logger.error(f"GASRelay.send: GAS error: {body.get('error')}")
                    return

                # Primary path: responses returned inline from GAS→server→GAS
                if 'responses' in body:
                    responses = body['responses']
                    # Sort by seq first (stream chunk ordering),
                    # then pid as tiebreaker (plain packet ordering)
                    responses.sort(key=lambda p: (p.get('seq', 0), p.get('pid', 0)))
                    logger.info(
                        f"GASRelay.send: {len(responses)} response(s) inline "
                        f"pids={[p.get('pid') for p in responses]}"
                    )
                    for p in responses:
                        logger.debug(
                            f"GASRelay.send: inline pid={p.get('pid')} "
                            f"seq={p.get('seq', 0)} "
                            f"ptype={p.get('ptype')} "
                            f"status={p.get('status')} "
                            f"body={len(p.get('b') or '') if p.get('b') else 0}B"
                        )
                    await self._handler.receive(responses)

                # Fallback: GAS cached them; _puller will collect
                elif body.get('fallback'):
                    logger.warning(
                        "GASRelay.send: GAS used fallback cache — "
                        "puller will collect responses"
                    )

                else:
                    logger.debug(
                        f"GASRelay.send: acknowledged queued={body.get('queued')}"
                    )

        except asyncio.TimeoutError:
            logger.error("GASRelay.send: POST timeout (60s)")
        except aiohttp.ClientConnectionError as exc:
            logger.error(f"GASRelay.send: connection error — {exc}")
        except Exception as exc:
            logger.error(f"GASRelay.send: unexpected error — {exc}", exc_info=True)

    # Fallback puller: long-polling GET drains queue:client when the inline
    # primary path was unavailable. Reconnects immediately on every return.
    async def _puller(self
        ):
        consecutive_errors = 0

        while True:
            logger.debug("GASRelay._puller: GET open (waiting for cached responses)")
            try:
                async with self._session.get(
                        self.GAS_URL,
                        params={'role': 'client'},
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

                    # Same sort order as send()
                    data.sort(key=lambda p: (p.get('seq', 0), p.get('pid', 0)))
                    logger.info(
                        f"GASRelay._puller: {len(data)} packet(s) from fallback cache "
                        f"pids={[p.get('pid') for p in data]}"
                    )
                    for p in data:
                        logger.debug(
                            f"GASRelay._puller: pid={p.get('pid')} "
                            f"seq={p.get('seq', 0)} "
                            f"ptype={p.get('ptype')} "
                            f"status={p.get('status')} "
                            f"body={len(p.get('b') or '') if p.get('b') else 0}B"
                        )
                    await self._handler.receive(data)

            except asyncio.TimeoutError:
                # Expected — GAS returned empty after POLL_MAX ms
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