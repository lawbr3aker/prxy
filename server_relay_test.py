import typing
import asyncio

import logging
#
logger = logging.getLogger(__name__)

from core.shared import RelayBase, RelayRequest
from core.server import Dispatcher

from client_relay_test import LocalBridge


class ServerTestRelay(RelayBase):
    """
    Drop-in replacement for GASRelay on the server side.

    push(raw_packets)   — called directly by ClientTestRelay.send()
                          dispatches all packets (plain and stream) concurrently
                          returns serialised responses inline
                          mirrors the /push HTTP handler in server_relay_gas.py

    send(request)       — receives response batches from stream pumps
                          relays them back to client via ClientTestRelay.receive()

    start()             — creates the Dispatcher and registers with LocalBridge
    """

    def __init__(self, handler):
        self._handler    = handler
        self._dispatcher: typing.Optional[Dispatcher] = None

    async def start(self):
        self._dispatcher = Dispatcher(self._handler)
        await self._dispatcher.start()
        logger.info("ServerTestRelay.start: dispatcher ready")

        LocalBridge.register_server(self)
        logger.info("ServerTestRelay.start: registered with LocalBridge")

    # Primary path — called directly by ClientTestRelay.send()
    # Handles BOTH plain packets and stream chunks
    async def push(self, raw_packets: list[dict]) -> list[dict]:
        logger.info(
            f"ServerTestRelay.push: received {len(raw_packets)} packet(s) "
            f"pids={[p.get('pid') for p in raw_packets]}"
        )
        for p in raw_packets:
            logger.debug(
                f"ServerTestRelay.push: packet pid={p.get('pid')} "
                f"ptype={p.get('ptype')} seq={p.get('seq', 0)} "
                f"destination={p.get('d') or p.get('destination')} "
                f"body={len(p.get('b') or '') if p.get('b') else 0}B"
            )

        # Dispatch all packets (plain returns inline, stream returns None)
        responses = await asyncio.gather(
            *[self._dispatcher.dispatch(p) for p in raw_packets],
            return_exceptions=False
        )
        
        # Collect only non-None responses (plain packets)
        valid      = [r for r in responses if r is not None]
        print(valid)
        serialized = [r.serialize() for r in valid]

        logger.info(
            f"ServerTestRelay.push: returning {len(serialized)} response(s) "
            f"pids={[r.pid for r in valid]}"
        )
        for r in valid:
            logger.debug(
                f"ServerTestRelay.push: response pid={r.pid} "
                f"ptype={r.ptype} seq={r.seq} "
                f"status={r.get('status')} "
                f"body={len(r.get('b') or b'') if r.get('b') else 0}B"
            )

        return serialized

    # Fallback send() — called by stream pumps with response batches
    # Routes responses back to client via the bridge
    async def send(self, request: RelayRequest):
        """
        Receives response batches from Dispatcher._stream_pump
        and routes them back to the client relay.
        """
        client = LocalBridge.get_client()
        if client is None or not hasattr(client, '_handler'):
            logger.error(
                f"ServerTestRelay.send: cannot reach client relay — "
                f"bridge={client}"
            )
            return

        logger.info(
            f"ServerTestRelay.send: routing {len(request.packets)} response(s) "
            f"back to client pids={[p.pid for p in request.packets]}"
        )
        for p in request.packets:
            logger.debug(
                f"ServerTestRelay.send: response pid={p.pid} "
                f"ptype={p.ptype} seq={p.seq} "
                f"body={len(p.get('b') or b'') if p.get('b') else 0}B"
            )

        # Deposit the responses into the client's pool
        # This is how stream pump responses reach the client
        raw = [p.serialize() for p in request.packets]
        print("****** sending")
        await client._handler.receive(raw)
        print("****** sent")