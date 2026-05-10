"""
local_test.py — run the full proxy tunnel in a single process, no GAS needed.

    python local_test.py

Then point your browser / curl at http://localhost:8888 as a proxy:

    curl -x http://localhost:8888 http://httpbin.org/get
    curl -x http://localhost:8888 https://httpbin.org/get   # CONNECT tunnel
"""
import asyncio
import threading

import logging
#
logging.basicConfig(
  level=logging.DEBUG,
  format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
  handlers=[
    logging.FileHandler('local_test.log'),
    logging.StreamHandler()
  ]
)

from core.client      import ProxyServer, ProxyRequestHandler
from core.server      import Handler  as ServerHandler

from server_relay_test import ServerTestRelay
from client_relay_test import ClientTestRelay


async def main():
    # ── 1. Boot server side first so LocalBridge is populated ─────────────────
    server_handler = ServerHandler(ServerTestRelay)
    await server_handler.init()         # calls ServerTestRelay.start() → registers bridge

    # ── 2. Boot client proxy ───────────────────────────────────────────────────
    proxy = ProxyServer(ClientTestRelay, ('0.0.0.0', 8888), ProxyRequestHandler)
    await proxy.handler.init()          # calls ClientTestRelay.start() (no-op)

    thread = threading.Thread(target=proxy.serve_forever, daemon=True)
    thread.start()

    print("Local test proxy running on 0.0.0.0:8888")
    print("  plain HTTP : curl -x http://localhost:8888 http://httpbin.org/get")
    print("  HTTPS/CONNECT: curl -x http://localhost:8888 https://httpbin.org/get")
    print("  Ctrl-C to stop")

    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        proxy.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
