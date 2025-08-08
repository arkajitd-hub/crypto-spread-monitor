import asyncio
import websockets
import json
import aiohttp
from datetime import datetime, timezone
import logging

# ───────────── Logging Setup ─────────────
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ───────────── Constants ─────────────
COINBASE_WS = "wss://advanced-trade-ws.coinbase.com"
KRAKEN_WS = "wss://ws.kraken.com"
UNISWAP_DEXSCREENER = "https://api.dexscreener.com/latest/dex/pairs/ethereum/0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8"

# ───────────── Coinbase WebSocket ─────────────
async def listen_coinbase():
    try:
        async with websockets.connect(COINBASE_WS, ping_interval=30, ping_timeout=10) as ws:
            print("🔗 Connected to Coinbase WebSocket")
            subscribe_msg = {
                "type": "subscribe",
                "channel": "market_trades",
                "product_ids": ["BTC-USD", "ETH-USD"]
            }
            await ws.send(json.dumps(subscribe_msg))
            print("📡 Subscribed to Coinbase market trades...")

            while True:
                msg = await ws.recv()
                data = json.loads(msg)
                events = data.get("events", [])
                for ev in events:
                    trades = ev.get("trades", [])
                    for t in trades:
                        print(
                            f"[Coinbase] {t['product_id']} | {t['side']:4s} | "
                            f"Price: ${float(t['price']):,.2f} | Size: {t['size']} | Time: {t['time']}"
                        )
    except Exception as e:
        print(f"❌ Coinbase WS error: {e}")
        await fetch_coinbase_rest_api()

# ───────────── Kraken WebSocket ─────────────
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
            print("🔗 Subscribed to Kraken WebSocket trades...")

            while True:
                msg = await ws.recv()
                data = json.loads(msg)

                if isinstance(data, list) and len(data) >= 4:
                    pair_code = data[-1]
                    trades = data[1]
                    for t in trades:
                        price = float(t[0])
                        size = t[1]
                        side = "BUY" if t[3] == "b" else "SELL"
                        timestamp = float(t[2])
                        time = datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()
                        pair_name = KRAKEN_PAIR_MAP.get(pair_code, pair_code)
                        print(f"[Kraken] {pair_name} | {side:4s} | Price: ${price:,.2f} | Size: {size} | Time: {time}")
    except Exception as e:
        print(f"❌ Kraken WS error: {e}")


# ───────────── Uniswap Polling ─────────────
async def poll_uniswap_price():
    print("🔄 Polling Uniswap (Dexscreener) every 30 seconds...")
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(UNISWAP_DEXSCREENER, timeout=5) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        pair = data["pair"]
                        base = pair["baseToken"]["symbol"]
                        quote = pair["quoteToken"]["symbol"]
                        price = float(pair["priceUsd"])
                        now = datetime.utcnow().isoformat()
                        print(f"[Uniswap] {base}/{quote} | Price: ${price:,.2f} | Time: {now}")
                    else:
                        print(f"❌ Uniswap HTTP error {resp.status}")
            except Exception as e:
                print(f"❌ Uniswap polling error: {e}")
            await asyncio.sleep(30)

# ───────────── Coinbase REST Fallback ─────────────
async def fetch_coinbase_rest_api():
    print("🔄 Fetching Coinbase prices via REST fallback...")
    try:
        async with aiohttp.ClientSession() as session:
            for symbol in ["BTC", "ETH"]:
                url = f"https://api.coinbase.com/v2/exchange-rates?currency={symbol}"
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        price = float(data['data']['rates']['USD'])
                        now = datetime.utcnow().isoformat()
                        print(f"[Coinbase REST] {symbol}/USD | Price: ${price:,.2f} | Time: {now}")
    except Exception as e:
        print(f"❌ Coinbase REST fetch failed: {e}")

# ───────────── Main ─────────────
async def main():
    print("🚀 Starting Live Crypto Price Monitor...")
    tasks = [
        asyncio.create_task(listen_coinbase()),
        asyncio.create_task(listen_kraken()),
        asyncio.create_task(poll_uniswap_price())
    ]
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        print("⏹️  Stopping monitor...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

# ───────────── Entry ─────────────
if __name__ == "__main__":
    asyncio.run(main())
