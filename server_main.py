import asyncio

import logging
#
logging.basicConfig(
  level=logging.INFO,
  format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
  handlers=[
    logging.FileHandler('server.log'),
    logging.StreamHandler()
  ]
)

from core.server      import Handler
from server_relay_gas import GASRelay


async def main():
    handler = Handler(GASRelay)
    await handler.init()

    print("Server running, waiting for packets via GAS...")
    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(main())
