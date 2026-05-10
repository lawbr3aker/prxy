import typing
import asyncio

import logging
#
logger = logging.getLogger(__name__)

from core.shared import RelayBase, RelayRequest, PacketType
from core.server import Dispatcher

from client_relay_test import LocalBridge


class ServerTestRelay(RelayBase):
    """
    Drop-in replacement for GASRelay on the server side.

    push(raw_packets)
        Called directly by ClientTestRelay.send().
        Dispatches all packets via Dispatcher.dispatch():
          • Plain HTTP → response returned inline.
          • STREAM     → returns None; pump pushes responses via send().
        Returns only the non-None (plain HTTP) responses.

    send(request)
        Called by Dispatcher._stream_pump via Handler.submit() with
        outbound response batches (stream chunks, CLOSE, EOF).
        Routes them straight into the client's Handler.receive() so they
        land in the client's StreamRegistry and unblock _handle_stream.

    start()
        Creates the Dispatcher and registers both sides with LocalBridge.
    """

    def __init__(self, handler):
        self._handler    = handler
        self._dispatcher: typing.Optional[Dispatcher] = None

    async def start(self
        ):
        self._dispatcher = Dispatcher(self._handler)
        await self._dispatcher.start()
        logger.info("ServerTestRelay.start: dispatcher ready")

        LocalBridge.register_server(self)
        logger.info("ServerTestRelay.start: registered with LocalBridge")

    # Primary path — called directly by ClientTestRelay.send()
    async def push(self,
            raw_packets: list[dict]
        ) -> list[dict]:
        logger.info(
            f"ServerTestRelay.push: received {len(raw_packets)} packet(s) "
            f"pids={[p.get('pid') for p in raw_packets]}"
        )
        for p in raw_packets:
            logger.debug(
                f"ServerTestRelay.push: packet pid={p.get('pid')} "
                f"ptype={PacketType.name(p.get('ptype', 0))} "
                f"dst={p.get('d') or p.get('destination')} "
                f"body={len(p.get('b') or '') if p.get('b') else 0}B"
            )

        responses = await asyncio.gather(
            *[self._dispatcher.dispatch(p) for p in raw_packets],
            return_exceptions=False
        )

        # Stream packets return None — only plain HTTP has inline responses
        valid      = [r for r in responses if r is not None]
        serialized = [r.serialize() for r in valid]

        logger.info(
            f"ServerTestRelay.push: returning {len(serialized)} inline response(s) "
            f"pids={[r.pid for r in valid]}"
        )
        for r in valid:
            logger.debug(
                f"ServerTestRelay.push: response pid={r.pid} "
                f"ptype={PacketType.name(r.ptype)} "
                f"status={r.get('status')} "
                f"body={len(r.get('b') or b'') if r.get('b') else 0}B"
            )

        return serialized

    # Called by Dispatcher._stream_pump via Handler.submit()
    # Delivers stream chunks / CLOSE / EOF directly into the client's
    # StreamRegistry so _handle_stream unblocks immediately.
    async def send(self,
            request: RelayRequest
        ):
        client = LocalBridge.get_client()
        if client is None:
            logger.error(
                "ServerTestRelay.send: LocalBridge has no client relay — "
                "did you call LocalBridge.register_client() before starting?"
            )
            return

        logger.info(
            f"ServerTestRelay.send: routing {len(request.packets)} response(s) "
            f"→ client pids={[p.pid for p in request.packets]}"
        )
        for p in request.packets:
            logger.debug(
                f"ServerTestRelay.send: pid={p.pid} "
                f"ptype={PacketType.name(p.ptype)} "
                f"body={len(p.get('b') or b'') if p.get('b') else 0}B"
            )

        raw = [p.serialize() for p in request.packets]
        await client._handler.receive(raw)
