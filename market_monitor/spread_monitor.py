import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from market_monitor.trade_handler import price_update_queue

logger = logging.getLogger(__name__)


class UpdateSuppressor:
    def __init__(self, min_interval: float = 1.0, abs_threshold: float = 0.5, rel_threshold: float = 0.005):
        self.min_interval = min_interval
        self.abs_threshold = abs_threshold
        self.rel_threshold = rel_threshold
        self._last_value: dict[str, float] = {}
        self._last_time: dict[str, float] = {}

    def should_emit(self, key: str, new_value: float, now_ts: float) -> bool:
        last_val = self._last_value.get(key)
        last_time = self._last_time.get(key, 0)

        if last_val is None:
            self._last_value[key] = new_value
            self._last_time[key] = now_ts
            return True

        time_delta = now_ts - last_time
        abs_diff = abs(new_value - last_val)
        rel_diff = abs_diff / last_val if last_val != 0 else float("inf")

        if time_delta < self.min_interval:
            return False
        if abs_diff >= self.abs_threshold or rel_diff >= self.rel_threshold:
            self._last_value[key] = new_value
            self._last_time[key] = now_ts
            return True
        return False


_suppressor = UpdateSuppressor(min_interval=1.0, abs_threshold=0.25, rel_threshold=0.002)
_lock = asyncio.Lock()

# latest prices by exchange
prices: dict[str, dict[str, float]] = {
    "Coinbase": {},
    "Kraken": {},
    "Bitstamp": {},
    "Coinbase REST": {},  # optional fallback
}


def _format_price(p: float) -> str:
    return f"${p:,.2f}"


def _spread_label(a: str, b: str) -> str:
    abbr = {
        ("Coinbase", "Kraken"): "C-K",
        ("Kraken", "Coinbase"): "C-K",
        ("Coinbase", "Bitstamp"): "C-B",
        ("Bitstamp", "Coinbase"): "C-B",
        ("Kraken", "Bitstamp"): "K-B",
        ("Bitstamp", "Kraken"): "K-B",
    }
    return abbr.get((a, b), f"{a[:1]}-{b[:1]}")


async def update_price(exchange: str, pair: str, price: float, side: Optional[str] = None):
    now_ts = datetime.now(tz=timezone.utc).timestamp()

    async with _lock:
        prices.setdefault(exchange, {})[pair] = price

        def get(ex: str):
            return prices.get(ex, {}).get(pair)

        combos = [("Coinbase", "Kraken"), ("Coinbase", "Bitstamp"), ("Kraken", "Bitstamp")]
        spreads: list[tuple[str, float]] = []

        for a, b in combos:
            pa, pb = get(a), get(b)
            if pa is not None and pb is not None:
                spreads.append((_spread_label(a, b), pa - pb))

        if not spreads:
            return

        triggered = False
        parts = []
        for short_lbl, val in spreads:
            key = f"{pair}-{short_lbl}"
            if _suppressor.should_emit(key, val, now_ts):
                triggered = True
            sign = "+" if val >= 0 else "-"
            parts.append(f"{short_lbl} {pair}: {sign}{abs(val):.2f}")

        if not triggered:
            return

        source = f"{exchange} {pair}" + (f" {side}" if side else "") + f" price {_format_price(price)}"
        logger.info("💱 %s | Source update: %s", " | ".join(parts), source)


async def price_update_dispatcher():
    """
    Consume the price_update_queue and call update_price.
    EXPECTS tuples of (exchange, pair, price, side).
    """
    while True:
        try:
            exchange, pair, price, side = await price_update_queue.get()
        except Exception:
            continue
        try:
            await update_price(exchange, pair, price, side=side)
        finally:
            price_update_queue.task_done()
