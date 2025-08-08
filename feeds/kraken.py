# feeds/kraken.py

import asyncio
import json
from datetime import datetime, timezone
import websockets
import logging

from utils.pairs import normalize_pair
from market_monitor.trade_handler import trade_queue

logger = logging.getLogger(__name__)

KRAKEN_WS = "wss://ws.kraken.com"

KRAKEN_PAIR_MAP = {
    "XBT/USD": "BTC/USD",
    "ETH/USD": "ETH/USD"
}


async def listen_kraken():
    try:
        async with websockets.connect(KRAKEN_WS) as ws:
            subscribe_msg = {
                "event": "subscribe",
                "pair": list(KRAKEN_PAIR_MAP.keys()),
                "subscription": {"name": "trade"}
            }
            await ws.send(json.dumps(subscribe_msg))
            logger.info("🔗 Subscribed to Kraken WebSocket trades...")

            while True:
                msg = await ws.recv()
                data = json.loads(msg)

                if isinstance(data, list) and len(data) >= 4:
                    pair_code = data[-1]
                    trades = data[1]
                    for t in trades:
                        price = float(t[0])
                        size = float(t[1])
                        side = "BUY" if t[3] == "b" else "SELL"
                        timestamp = float(t[2])
                        time_iso = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()

                        raw_pair = KRAKEN_PAIR_MAP.get(pair_code, pair_code)
                        normalized_pair = normalize_pair("Kraken", raw_pair)

                        # enqueue for logging and spread monitor
                        await trade_queue.put(("Kraken", normalized_pair, side, price, size, time_iso))
    except Exception as e:
        logger.error("❌ Kraken WS error: %s", e)
        # no fallback here; if needed, you can add a backoff reconnect loop
