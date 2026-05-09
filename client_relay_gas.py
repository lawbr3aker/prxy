import typing
import asyncio
import json

import aiohttp

import logging
#
logger = logging.getLogger(__name__)

from core.shared import RelayBase, RelayRequest, Packet


class GASRelay(RelayBase):
    GAS_URL = 'https://script.google.com/macros/s/AKfycbyHKhzCtoi6ofiur5d8frZWGRtqhDGskY7EsnnDT5IsJaXVN8n4S2f27-M1F33mrdZv/exec'

    def __init__(self, handler):
        self._handler = handler
        self._session: typing.Optional[aiohttp.ClientSession] = None

    async def start(self
        ):
        logger.info("GASRelay.start: scheduling _run()")
        asyncio.ensure_future(self._run())

    async def _run(self
        ):
        # Session scoped to task lifetime — closes cleanly on cancellation
        logger.info("GASRelay._run: opening aiohttp session")
        async with aiohttp.ClientSession() as session:
            self._session = session
            logger.info("GASRelay._run: session ready — entering fallback puller")
            await self._puller()

    # Primary path: send() posts request packets to GAS and reads responses
    # from the reply body. On the happy path GAS forwards to the server and
    # returns responses inline; no separate polling needed.
    async def send(self,
            request: RelayRequest
        ):
        packets_info = [
            f"pid={p.pid} ptype={p.ptype} body={len(p.get('b') or b'') if p.get('b') else 0}B"
            for p in request.packets
        ]
        logger.info(
            f"GASRelay.send: POST {len(request.packets)} packet(s) "
            f"source={request.source!r} → GAS | {packets_info}"
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
                    f"GASRelay.send: POST response status={r.status} "
                    f"body_len={len(text)}"
                )
                logger.debug(f"GASRelay.send: POST raw response: {text[:500]}")

                if r.status != 200 or not text.strip():
                    logger.warning(
                        f"GASRelay.send: unexpected status={r.status} or empty body"
                    )
                    return

                body = json.loads(text)

                if not body.get('ok'):
                    logger.error(f"GASRelay.send: GAS error: {body.get('error')}")
                    return

                # Primary path: responses came back inline from GAS → server → GAS
                if 'responses' in body:
                    responses = body['responses']
                    responses.sort(key=lambda p: p.get('pid', 0))
                    logger.info(
                        f"GASRelay.send: {len(responses)} response(s) inline "
                        f"pids={[p.get('pid') for p in responses]}"
                    )
                    for p in responses:
                        logger.debug(
                            f"GASRelay.send: inline response pid={p.get('pid')} "
                            f"status={p.get('status')} "
                            f"body={len(p.get('b') or '') if p.get('b') else 0}B"
                        )
                    await self._handler.receive(responses)

                # Fallback path: GAS cached them; puller will collect
                elif body.get('fallback'):
                    logger.warning(
                        "GASRelay.send: GAS used fallback cache — "
                        "puller will collect responses"
                    )

                else:
                    logger.debug(
                        f"GASRelay.send: POST acknowledged queued={body.get('queued')}"
                    )

        except asyncio.TimeoutError:
            logger.error("GASRelay.send: POST timeout (60s)")
        except aiohttp.ClientConnectionError as exc:
            logger.error(f"GASRelay.send: connection error — {exc}")
        except Exception as exc:
            logger.error(f"GASRelay.send: unexpected error — {exc}", exc_info=True)

    # Fallback puller — long-polling GET to drain queue:client when the primary
    # inline path was unavailable. Reconnects immediately on every return.
    async def _puller(self
        ):
        consecutive_errors = 0

        while True:
            logger.debug(
                "GASRelay._puller: GET open (fallback — waiting for cached responses)..."
            )
            try:
                async with self._session.get(
                        self.GAS_URL,
                        params={'role': 'client'},
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
                        logger.debug("GASRelay._puller: empty packet list — reconnecting")
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
                            f"status={p.get('status')} "
                            f"body={len(p.get('b') or '') if p.get('b') else 0}B"
                        )
                    await self._handler.receive(data)

            except asyncio.TimeoutError:
                # Expected — GAS held the connection for POLL_MAX then returned empty
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
