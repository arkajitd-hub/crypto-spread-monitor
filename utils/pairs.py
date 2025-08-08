# utils/pairs.py

# Normalize venue-specific pair formats into a single "BASE/QUOTE" (BTC/USD, ETH/USD)

def normalize_pair(venue: str, raw_pair: str) -> str:
    venue = (venue or "").lower()
    p = raw_pair.replace("-", "/").replace("_", "/").upper()

    # Coinbase: "BTC-USD" -> "BTC/USD"
    if venue == "coinbase":
        return p

    # Kraken: "XBT/USD" -> "BTC/USD"
    if venue == "kraken":
        p = p.replace("XBT/", "BTC/")
        return p

    # Bitstamp already comes through as "BTC/USD", but be defensive
    if venue == "bitstamp":
        return p

    # Uniswap / reference sources may pass symbols already normalized
    if venue == "uniswap" or venue == "coinbase rest":
        return p

    # default
    return p
