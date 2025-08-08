import asyncio
import csv
import json
import os
from datetime import datetime, timezone

# shared queues
trade_queue: asyncio.Queue = asyncio.Queue()
price_update_queue: asyncio.Queue = asyncio.Queue()

CSV_FILE = "trades.csv"
JSONL_FILE = "trades.jsonl"


def _ensure_csv_header():
    if not os.path.exists(CSV_FILE) or os.path.getsize(CSV_FILE) == 0:
        with open(CSV_FILE, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["exchange", "pair", "side", "price", "size", "timestamp", "received_at_utc"])


async def trade_logger_and_updater():
    """
    Consume trades from trade_queue, persist to CSV/JSONL,
    and push price updates (exchange, pair, price, side) to price_update_queue.
    """
    _ensure_csv_header()

    while True:
        try:
            exchange, pair, side, price, size, timestamp = await trade_queue.get()
        except Exception:
            continue  # defensive

        received_at = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

        # Persist to CSV
        with open(CSV_FILE, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([exchange, pair, side, price, size, timestamp, received_at])

        # Persist to JSONL
        record = {
            "exchange": exchange,
            "pair": pair,
            "side": side,
            "price": price,
            "size": size,
            "timestamp": timestamp,
            "received_at": received_at,
        }
        with open(JSONL_FILE, "a") as f:
            f.write(json.dumps(record) + "\n")

        # Notify spread monitor (non-blocking; drop if queue is full)
        try:
            price_update_queue.put_nowait((exchange, pair, price, side))
        except asyncio.QueueFull:
            pass

        trade_queue.task_done()
