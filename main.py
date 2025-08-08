import asyncio
import logging

from feeds.coinbase import listen_coinbase
from feeds.kraken import listen_kraken
from feeds.bitstamp import listen_bitstamp
# from feeds.uniswap import poll_uniswap_price  # optional, if you still want the ref feed

from market_monitor.trade_handler import trade_logger_and_updater
from market_monitor.spread_monitor import price_update_dispatcher

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s:%(name)s:%(message)s"
)
logger = logging.getLogger(__name__)


async def main():
    logger.info("🚀 Starting Live Crypto Price Monitor...")

    tasks = [
        asyncio.create_task(trade_logger_and_updater()),
        asyncio.create_task(price_update_dispatcher()),
        asyncio.create_task(listen_coinbase()),
        asyncio.create_task(listen_kraken()),
        asyncio.create_task(listen_bitstamp()),
        # asyncio.create_task(poll_uniswap_price()),  # optional
    ]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        logger.info("Stopping...")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
