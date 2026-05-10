import asyncio
import threading
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(name)s %(levelname)s %(message)s',
    handlers=[logging.FileHandler('client.log'), logging.StreamHandler()],
)

from core.client      import ProxyServer, ProxyRequestHandler
from client_relay_gas import GASRelay


async def main():
    server = ProxyServer(GASRelay, ('0.0.0.0', 8889), ProxyRequestHandler)
    await server.handler.init()

    threading.Thread(target=server.serve_forever, daemon=True).start()
    print("Proxy on 0.0.0.0:8889")

    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        server.shutdown()


if __name__ == '__main__':
    asyncio.run(main())
