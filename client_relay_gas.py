import typing
import asyncio
import json

import aiohttp

import logging
#
logger = logging.getLogger(__name__)

from core.shared import RelayBase, RelayRequest


class GASRelay(RelayBase):
    GAS_URL = 'https://script.google.com/macros/s/AKfycbyHKhzCtoi6ofiur5d8frZWGRtqhDGskY7EsnnDT5IsJaXVN8n4S2f27-M1F33mrdZv/exec'

    def __init__(self, handler):
        self._handler = handler
        self._session: typing.Optional[aiohttp.ClientSession] = None

    async def start(self
        ):
        asyncio.ensure_future(self._run())

    async def _run(self
        ):
        # Session scoped to task lifetime — closes cleanly on cancellation
        async with aiohttp.ClientSession() as session:
            self._session = session
            # Fallback puller — only used when GAS could not reach the server
            # directly and cached responses under queue:client instead
            await self._puller()

    # Primary path: send() posts to GAS and reads responses from the reply.
    # GAS forwards synchronously to the server and returns responses inline.
    # No separate polling needed on the happy path.
    async def send(self,
            request: RelayRequest
        ):
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
                logger.info(f"GAS POST {r.status}")

                if r.status != 200 or not text.strip():
                    return

                body = json.loads(text)

                if not body.get('ok'):
                    logger.error(f"GAS error: {body.get('error')}")
                    return

                # Primary path: responses came back inline
                if 'responses' in body:
                    responses = body['responses']
                    responses.sort(key=lambda p: p.get('pid', 0))
                    logger.info(f"GAS → client: {len(responses)} response(s) inline")
                    await self._handler.receive(responses)

                # Fallback path: GAS cached them, puller will pick up
                elif body.get('fallback'):
                    logger.warning("GAS used fallback cache path, puller will collect responses")

        except Exception as e:
            logger.error(f"GAS POST error: {e}")

    # Fallback puller — hanging GET to drain queue:client when the primary
    # path was unavailable. On every return reconnect immediately.
    async def _puller(self
        ):
        while True:
            try:
                logger.debug("GAS GET open (fallback, waiting for cached responses)...")
                async with self._session.get(
                        self.GAS_URL,
                        params={'role': 'client'},
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
                    logger.info(f"GAS → client fallback: {len(data)} packet(s)")
                    await self._handler.receive(data)

            except asyncio.TimeoutError:
                logger.debug("GAS GET timeout, reconnecting")
            except Exception as e:
                logger.error(f"GAS GET error: {e}")
                await asyncio.sleep(2)