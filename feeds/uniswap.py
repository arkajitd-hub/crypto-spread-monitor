# feeds/uniswap.py

import asyncio
from datetime import datetime, timezone
import aiohttp
import backoff  # if not installed, you can substitute simple retry logic
import logging

from utils.pairs import normalize_pair
from market_monitor.trade_handler import trade_queue, price_update_queue

logger = logging.getLogger(__name__)

UNISWAP_DEXSCREENER = "https://api.dexscreener.com/latest/dex/pairs/ethereum/0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8"


async def _fetch_with_backoff(session: aiohttp.ClientSession):
    # lightweight backoff without external library if not installed
    tries = 0
    while True:
        try:
            async with session.get(UNISWAP_DEXSCREENER, timeout=5) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    logger.warning("Uniswap HTTP error %s", resp.status)
        except Exception as e:
            logger.warning("Uniswap polling exception: %s", e)
        tries += 1
        await asyncio.sleep(min(5, 2 ** min(tries, 5)))  # exponential backoff cap at 5s


async def poll_uniswap_price():
    logger.info("🔄 Polling Uniswap (Dexscreener) every 30 seconds...")
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                data = await _fetch_with_backoff(session)
                pair = data.get("pair", {})
                base = pair.get("baseToken", {}).get("symbol")
                quote = pair.get("quoteToken", {}).get("symbol")
                if base is None or quote is None:
                    logger.warning("Uniswap response missing tokens: %s", data)
                    await asyncio.sleep(30)
                    continue

                price = float(pair.get("priceUsd", 0.0))
                now = datetime.now(tz=timezone.utc)
                now_iso = now.isoformat()

                normalized_pair = normalize_pair("Uniswap", f"{base}/{quote}")

                # Push into trade queue for logging (treat as reference trade)
                await trade_queue.put(("Uniswap", normalized_pair, "REFERENCE", price, 0.0, now_iso))

                # Notify spread monitor
                try:
                    price_update_queue.put_nowait(("Uniswap", normalized_pair, price, None))
                except asyncio.QueueFull:
                    pass

            except Exception as e:
                logger.error("❌ Uniswap polling error: %s", e)
            await asyncio.sleep(30)
