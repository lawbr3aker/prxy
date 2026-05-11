import typing
import asyncio
import logging

from core.shared import _logger, RelayBase, RelayRequest


_log = _logger(__name__ + '.LocalBridge')


class LocalBridge:
    _server : typing.Optional['ServerTestRelay'] = None
    _client : typing.Optional['ClientTestRelay'] = None

    @classmethod
    def register_server(cls, s: 'ServerTestRelay'):
        cls._server = s
        _log.info("server registered")

    @classmethod
    def register_client(cls, c: 'ClientTestRelay'):
        cls._client = c
        _log.info("client registered")

    @classmethod
    def get_server(cls) -> typing.Optional['ServerTestRelay']:
        return cls._server

    @classmethod
    def get_client(cls) -> typing.Optional['ClientTestRelay']:
        return cls._client


class ClientTestRelay(RelayBase):
    _log = _logger(__name__ + '.ClientTestRelay')

    def __init__(self, handler):
        self._handler = handler

    async def start(self):
        LocalBridge.register_client(self)

    async def send(self, request: RelayRequest):
        server = LocalBridge.get_server()
        if server is None:
            self._log.error("no server registered")
            return

        raw       = [p.serialize() for p in request.packets]
        responses = await server.push(raw)

        if responses:
            responses.sort(key=lambda p: p.get('timestamp', 0))
            await self._handler.receive(responses)
