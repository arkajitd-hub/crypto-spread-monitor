# feeds/coinbase.py

import asyncio
import json
from datetime import datetime, timezone
import websockets
import logging

from utils.pairs import normalize_pair
from market_monitor.trade_handler import trade_queue
from feeds.fallback import fetch_coinbase_rest_api
from config import COINBASE_WS  # assumes config.py at repo root defines COINBASE_WS

logger = logging.getLogger(__name__)


async def listen_coinbase():
    """
    Connect to Coinbase WebSocket, ingest trade events, normalize pairs, and push into trade queue.
    On error, invoke REST fallback and retry with exponential backoff.
    """
    backoff_seconds = 1
    while True:
        try:
            async with websockets.connect(COINBASE_WS, ping_interval=30, ping_timeout=10) as ws:
                logger.info("🔗 Connected to Coinbase WebSocket")
                subscribe_msg = {
                    "type": "subscribe",
                    "channel": "market_trades",
                    "product_ids": ["BTC-USD", "ETH-USD"]
                }
                await ws.send(json.dumps(subscribe_msg))
                logger.info("📡 Subscribed to Coinbase market trades...")

                # Reset backoff on successful connection
                backoff_seconds = 1

                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    events = data.get("events", [])
                    for ev in events:
                        trades = ev.get("trades", [])
                        for t in trades:
                            try:
                                raw_product = t.get("product_id", "")
                                normalized_pair = normalize_pair("Coinbase", raw_product)
                                side = t.get("side", "").upper()
                                price = float(t.get("price", 0.0))
                                size = float(t.get("size", 0.0))
                                timestamp = t.get("time")  # expecting ISO format with Z

                                # Push into shared trade queue
                                await trade_queue.put(("Coinbase", normalized_pair, side, price, size, timestamp))
                            except Exception as inner:
                                logger.warning("Malformed trade entry from Coinbase skipped: %s (%s)", t, inner)
        except Exception as e:
            logger.error("❌ Coinbase WS error: %s", e)
            # Fallback once before reconnecting
            try:
                await fetch_coinbase_rest_api()
            except Exception as fe:
                logger.error("Fallback fetch failed: %s", fe)
            # Backoff before retrying connection
            await asyncio.sleep(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, 30)
