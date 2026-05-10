import typing
import asyncio
import logging

from core.shared import class_logger, RelayBase, RelayRequest
from core.server import Dispatcher
from client_relay_test import LocalBridge


class ServerTestRelay(RelayBase):
    _log = class_logger(__name__, 'ServerTestRelay')

    def __init__(self, handler):
        self._handler    = handler
        self._dispatcher : typing.Optional[Dispatcher] = None

    async def start(self):
        self._dispatcher = Dispatcher(self._handler)
        await self._dispatcher.start()
        asyncio.ensure_future(self._dispatcher.run_fallback())
        LocalBridge.register_server(self)
        self._log.info("ready")

    async def push(self, raw_packets: list[dict]) -> list[dict]:
        responses = await asyncio.gather(
            *[self._dispatcher.dispatch(p) for p in raw_packets]
        )
        valid = [r for r in responses if r is not None]
        return [r.serialize() for r in valid]

    async def send(self, request: RelayRequest):
        client = LocalBridge.get_client()
        if client is None:
            self._log.error("no client registered")
            return
        raw = [p.serialize() for p in request.packets]
        await client._handler.receive(raw)
