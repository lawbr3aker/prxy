import typing
import asyncio

import logging
#
logger = logging.getLogger(__name__)

from core.shared import RelayBase, RelayRequest, PacketType


class LocalBridge:
    """
    Wires the two in-process relays together.

    ServerTestRelay.start() calls LocalBridge.register(self) so that
    ClientTestRelay.send() can reach it.
    """

    _server: typing.Optional['ServerTestRelay'] = None
    _client: typing.Optional['ServerTestRelay'] = None

    @classmethod
    def register_server(cls, server: 'ServerTestRelay'):
        cls._server = server
        logger.info("LocalBridge.register: server relay registered")

    @classmethod
    def get_server(cls) -> typing.Optional['ServerTestRelay']:
        return cls._server
    @classmethod
    def register_client(cls, server: 'ServerTestRelay'):
        cls._client = server
        logger.info("LocalBridge.register: server relay registered")

    @classmethod
    def get_client(cls) -> typing.Optional['ServerTestRelay']:
        return cls._client


class ClientTestRelay(RelayBase):
    """
    Drop-in replacement for GASRelay on the client side.

    send() → server.push() with all packets (plain and stream)
    → responses returned inline → handler.receive()
    
    Stream responses (STREAM|RESPONSE) are queued in handler.pool
    using per-pid asyncio.Queue so chunks can be consumed in order.
    """

    def __init__(self, handler):
        self._handler = handler

    async def start(self):
        LocalBridge.register_client(self)
        logger.info("ClientTestRelay.start: local relay ready")

    async def push(self, raw_packets: list[dict]) -> list[dict]:
        logger.info(
            f"ClientTestRelay.push: received {len(raw_packets)} packet(s) "
            f"pids={[p.get('pid') for p in raw_packets]}"
        )
        for p in raw_packets:
            logger.debug(
                f"ClientTestRelay.push: packet pid={p.get('pid')} "
                f"ptype={p.get('ptype')} seq={p.get('seq', 0)} "
                f"destination={p.get('d') or p.get('destination')} "
                f"body={len(p.get('b') or '') if p.get('b') else 0}B"
            )

        self._handler.recieve(raw_packets)

    async def send(self, request: RelayRequest):
        server = LocalBridge.get_server()
        if server is None:
            logger.error(
                "ClientTestRelay.send: LocalBridge has no server relay — "
                "did you call LocalBridge.register() before starting?"
            )
            return

        logger.info(
            f"ClientTestRelay.send: → server {len(request.packets)} packet(s) "
            f"pids={[p.pid for p in request.packets]}"
        )
        for p in request.packets:
            logger.debug(
                f"ClientTestRelay.send: pid={p.pid} ptype={p.ptype} seq={p.seq} "
                f"body={len(p.get('b') or b'') if p.get('b') else 0}B"
            )

        # All packets go to push() — both plain and stream
        raw = [p.serialize() for p in request.packets]
        responses = await server.push(raw)

        if responses:
            logger.info(
                f"ClientTestRelay.send: ← {len(responses)} response(s) inline "
                f"pids={[r.get('pid') for r in responses]}"
            )
            for r in responses:
                logger.debug(
                    f"ClientTestRelay.send: response pid={r.get('pid')} "
                    f"ptype={r.get('ptype')} seq={r.get('seq', 0)} "
                    f"body={len(r.get('b') or '') if r.get('b') else 0}B"
                )
            await self._handler.receive(responses)