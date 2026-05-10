import asyncio
import threading
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(name)s %(levelname)s %(message)s',
    handlers=[logging.FileHandler('local_test.log'), logging.StreamHandler()],
)

from core.client       import ProxyServer, ProxyRequestHandler
from core.server       import Handler as ServerHandler
from server_relay_test import ServerTestRelay
from client_relay_test import ClientTestRelay


async def main():
    server_handler = ServerHandler(ServerTestRelay)
    await server_handler.init()

    proxy = ProxyServer(ClientTestRelay, ('0.0.0.0', 8888), ProxyRequestHandler)
    await proxy.handler.init()

    threading.Thread(target=proxy.serve_forever, daemon=True).start()

    print("Local proxy on 0.0.0.0:8888")
    print("  http : curl -x http://localhost:8888 http://httpbin.org/get")
    print("  https: curl -x http://localhost:8888 https://httpbin.org/get")

    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        proxy.shutdown()


if __name__ == '__main__':
    asyncio.run(main())
