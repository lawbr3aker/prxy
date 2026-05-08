import typing
import asyncio
import json

import aiohttp

import logging
#
logger = logging.getLogger(__name__)

from core.shared import RelayBase, RelayRequest
from core.server import Dispatcher


class GASRelay(RelayBase):
    GAS_URL = 'https://script.google.com/macros/s/AKfycbyNiZLWWac3GaXRJVTInzZ6tQQXfB8bD3YwGLvCt4HW9xTuK1mugOLo0X9KvpT44BeK/exec'

    def __init__(self, handler):
        self._handler    = handler
        self._session:    typing.Optional[aiohttp.ClientSession] = None
        self._dispatcher: typing.Optional[Dispatcher] = None

    async def start(self
        ):
        self._session    = aiohttp.ClientSession()
        self._dispatcher = Dispatcher(self._handler)
        await self._dispatcher.start()
        asyncio.ensure_future(self._puller())
        logger.info("server GASRelay started")

    # Persistent hanging GET — GAS holds it open until request packets
    # from the client are ready in the cache.
    # On every return (data or timeout) reconnect immediately.
    async def _puller(self
        ):
        while True:
            try:
                logger.debug("GAS GET open (waiting for client requests)...")
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
                    logger.info(f"GAS → server: {len(data)} packet(s)")
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
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as r:
                logger.info(f"GAS POST {r.status}")
        except Exception as e:
            logger.error(f"GAS POST error: {e}")
