import typing
import asyncio
import json

import aiohttp

import logging
#
logger = logging.getLogger(__name__)

from core.shared import RelayBase, RelayRequest


class GASRelay(RelayBase):
    GAS_URL = 'https://script.google.com/macros/s/AKfycbyNiZLWWac3GaXRJVTInzZ6tQQXfB8bD3YwGLvCt4HW9xTuK1mugOLo0X9KvpT44BeK/exec'

    def __init__(self, handler):
        self._handler = handler
        self._session: typing.Optional[aiohttp.ClientSession] = None

    async def start(self
        ):
        self._session = aiohttp.ClientSession()
        asyncio.ensure_future(self._puller())
        logger.info("client GASRelay started")

    # Persistent hanging GET — GAS holds it open until response packets
    # from the server are ready in the cache.
    # On every return (data or timeout) reconnect immediately.
    async def _puller(self
        ):
        while True:
            try:
                logger.debug("GAS GET open (waiting for server responses)...")
                async with self._session.get(
                        self.GAS_URL,
                        params={'role': 'client'},
                        proxy='socks5://172.20.10.1:1082',
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
                    logger.info(f"GAS → client: {len(data)} packet(s)")
                    await self._handler.receive(data)

            except asyncio.TimeoutError:
                logger.debug("GAS GET timeout, reconnecting")
            except Exception as e:
                logger.error(f"GAS GET error: {e}")
                await asyncio.sleep(2)

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
                    proxy='socks5://172.20.10.1:1082',
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as r:
                logger.info(f"GAS POST {r.status}")
        except Exception as e:
            logger.error(f"GAS POST error: {e}")
