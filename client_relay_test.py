import typing
import asyncio

import logging
#
logger = logging.getLogger(__name__)

from core.shared import RelayBase, RelayRequest, PacketType


class LocalBridge:
    """
    Wires the two in-process relays together.

    ServerTestRelay.start() calls LocalBridge.register_server(self).
    ClientTestRelay.start() calls LocalBridge.register_client(self).
    Both must be registered before any traffic flows.
    """

    _server: typing.Optional['ServerTestRelay'] = None
    _client: typing.Optional['ClientTestRelay'] = None

    @classmethod
    def register_server(cls, server: 'ServerTestRelay'):
        cls._server = server
        logger.info("LocalBridge.register_server: server relay registered")

    @classmethod
    def get_server(cls) -> typing.Optional['ServerTestRelay']:
        return cls._server

    @classmethod
    def register_client(cls, client: 'ClientTestRelay'):
        cls._client = client
        logger.info("LocalBridge.register_client: client relay registered")

    @classmethod
    def get_client(cls) -> typing.Optional['ClientTestRelay']:
        return cls._client


class ClientTestRelay(RelayBase):
    """
    Drop-in replacement for GASRelay on the client side.

    Plain HTTP (REQUEST):
        send() → server.push() → inline responses → handler.receive()

    STREAM (REQUEST|STREAM):
        send() → server.push()
        dispatch() returns None for stream; pump responses come back via
        ServerTestRelay.send() → this relay's handler.receive() →
        StreamRegistry (in-order delivery to _handle_stream).
    """

    def __init__(self, handler):
        self._handler = handler

    async def start(self
        ):
        LocalBridge.register_client(self)
        logger.info("ClientTestRelay.start: local relay ready")

    async def send(self,
            request: RelayRequest
        ):
        server = LocalBridge.get_server()
        if server is None:
            logger.error(
                "ClientTestRelay.send: LocalBridge has no server relay — "
                "did you call LocalBridge.register_server() before starting?"
            )
            return

        packets_info = [
            f"pid={p.pid} ptype={PacketType.name(p.ptype)} "
            f"body={len(p.get('b') or b'') if p.get('b') else 0}B"
            for p in request.packets
        ]
        logger.info(
            f"ClientTestRelay.send: → server {len(request.packets)} packet(s) "
            f"| {packets_info}"
        )

        raw       = [p.serialize() for p in request.packets]
        responses = await server.push(raw)

        if not responses:
            return

        logger.info(
            f"ClientTestRelay.send: ← {len(responses)} response(s) inline "
            f"pids={[r.get('pid') for r in responses]}"
        )
        for r in responses:
            logger.debug(
                f"ClientTestRelay.send: response pid={r.get('pid')} "
                f"ptype={PacketType.name(r.get('ptype', 0))} "
                f"body={len(r.get('b') or '') if r.get('b') else 0}B"
            )
        await self._handler.receive(responses)
