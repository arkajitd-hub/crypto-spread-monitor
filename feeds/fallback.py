# feeds/fallback.py

import asyncio
from datetime import datetime, timezone
import aiohttp
import logging

from utils.pairs import normalize_pair
from market_monitor.trade_handler import trade_queue

logger = logging.getLogger(__name__)


async def fetch_coinbase_rest_api():
    logger.info("🔄 Fetching Coinbase prices via REST fallback...")
    try:
        async with aiohttp.ClientSession() as session:
            for symbol in ["BTC", "ETH"]:
                url = f"https://api.coinbase.com/v2/exchange-rates?currency={symbol}"
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        rate_map = data.get("data", {}).get("rates", {})
                        price = float(rate_map.get("USD", 0.0))
                        now = datetime.now(tz=timezone.utc).isoformat()
                        raw_pair = f"{symbol}/USD"
                        normalized_pair = normalize_pair("Coinbase", raw_pair)
                        # treat as REST reference trade
                        await trade_queue.put(("Coinbase REST", normalized_pair, "REST", price, 0.0, now))
                    else:
                        logger.warning("Coinbase REST HTTP error %s", resp.status)
    except Exception as e:
        logger.error("❌ Coinbase REST fetch failed: %s", e)
