# feeds/bitstamp.py

import asyncio
import json
import logging
from datetime import datetime, timezone

import websockets

from utils.pairs import normalize_pair
from market_monitor.trade_handler import trade_queue

logger = logging.getLogger(__name__)

BITSTAMP_WS = "wss://ws.bitstamp.net"

# We’ll listen to BTC/USD and ETH/USD
CHANNELS = [
    "live_trades_btcusd",
    "live_trades_ethusd",
]

# Bitstamp docs: event = "trade" carries data; "type" 0=buy, 1=sell
TYPE_SIDE = {0: "BUY", 1: "SELL"}


async def _subscribe(ws):
    for ch in CHANNELS:
        msg = {"event": "bts:subscribe", "data": {"channel": ch}}
        await ws.send(json.dumps(msg))
    logger.info("📡 Subscribed to Bitstamp channels: %s", ", ".join(CHANNELS))


def _channel_to_pair(channel: str) -> str:
    # channel like "live_trades_btcusd" -> "BTC/USD"
    if not channel.startswith("live_trades_"):
        return channel
    basequote = channel.replace("live_trades_", "")
    # defensive: ensure 3+3 split where possible
    if basequote.endswith("usd") and len(basequote) >= 6:
        base = basequote[:-3].upper()
        quote = "USD"
        return f"{base}/{quote}"
    return basequote.upper()


async def listen_bitstamp():
    backoff = 1
    while True:
        try:
            async with websockets.connect(BITSTAMP_WS, ping_interval=30, ping_timeout=10) as ws:
                logger.info("🔗 Connected to Bitstamp WebSocket")
                await _subscribe(ws)
                backoff = 1  # reset backoff after a good connect

                while True:
                    raw = await ws.recv()
                    try:
                        msg = json.loads(raw)
                    except Exception:
                        logger.debug("Bitstamp non-JSON message: %s", raw)
                        continue

                    event = msg.get("event")
                    if event == "bts:subscription_succeeded":
                        # ignore
                        continue
                    if event == "bts:heartbeat":
                        # ignore
                        continue
                    if event != "trade":
                        # other events we don't care about
                        continue

                    data = msg.get("data", {})
                    channel = msg.get("channel", "")
                    pair = _channel_to_pair(channel)
                    pair = normalize_pair("Bitstamp", pair)

                    try:
                        price = float(data.get("price")) if "price" in data else float(data.get("price_str"))
                        size = float(data.get("amount")) if "amount" in data else float(data.get("amount_str"))
                        ttype = int(data.get("type", -1))
                        side = TYPE_SIDE.get(ttype, "BUY" if ttype == 0 else "SELL")

                        # prefer microtimestamp if present
                        micro = data.get("microtimestamp")
                        if micro is not None:
                            ts = datetime.fromtimestamp(int(micro) / 1_000_000, tz=timezone.utc).isoformat()
                        else:
                            ts_epoch = float(data.get("timestamp"))
                            ts = datetime.fromtimestamp(ts_epoch, tz=timezone.utc).isoformat()

                        # Ship it to the shared trade queue
                        await trade_queue.put(("Bitstamp", pair, side, price, size, ts))
                    except Exception as parse_err:
                        logger.warning("Skipping malformed Bitstamp trade: %s (%s)", msg, parse_err)

        except Exception as e:
            logger.error("❌ Bitstamp WS error: %s", e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
