import asyncio
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s %(name)s %(levelname)s %(message)s',
    handlers=[logging.FileHandler('server.log'), logging.StreamHandler()],
)

from core.server      import Handler
from server_relay_gas import GASRelay


async def main():
    handler = Handler(GASRelay)
    await handler.init()
    print(f"Server on 0.0.0.0:{GASRelay.LISTEN_PORT}")

    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass


if __name__ == '__main__':
    asyncio.run(main())
