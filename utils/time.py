# market_monitor/utils/time.py
from datetime import datetime, timezone

def utc_now_iso():
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

def timestamp_to_utc(ts: float):
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
