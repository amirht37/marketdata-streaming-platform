import time

from prometheus_client import Counter, Histogram

from Logger import log_parser


# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------

PARSE_TOTAL = Counter(
    "parse_messages_total",
    "Number of messages parsed, by channel type and outcome",
    ["channel_type", "outcome"],  # outcome: success | error
)

PARSE_ERRORS_TOTAL = Counter(
    "parse_errors_total",
    "Number of parse failures, by channel type and error kind",
    ["channel_type", "error_type"],  # error_type: KeyError | ValueError | TypeError | unknown_channel | root_malformed
)

PARSE_DURATION_SECONDS = Histogram(
    "parse_duration_seconds",
    "Time spent parsing a single raw message into a normalized record",
    ["channel_type"],
    buckets=(0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5),
)


def parse_orderbook(msg):
    started_at = time.time()
    try:
        push = msg["push"]
        channel = push["channel"]
        symbol = channel.split("orderbook-")[1]

        pub = push["pub"]
        data = pub["data"]
        offset = pub["offset"]

        bids = [{"price": float(p), "amount": float(a)} for p, a in data["bids"]]
        asks = [{"price": float(p), "amount": float(a)} for p, a in data["asks"]]

        record = {
            "symbol": symbol,
            "lastUpdate": data["lastUpdate"],
            "lastTradePrice": float(data["lastTradePrice"]),
            "bids": bids,
            "asks": asks,
            "offset": offset
        }

        PARSE_TOTAL.labels(channel_type="orderbook", outcome="success").inc()
        return record, "orderbook"

    except (KeyError, ValueError, TypeError) as e:
        log_parser.error("parse_orderbook_failed", extra={"error": str(e), "msg": str(msg)[:200]})
        PARSE_TOTAL.labels(channel_type="orderbook", outcome="error").inc()
        PARSE_ERRORS_TOTAL.labels(channel_type="orderbook", error_type=type(e).__name__).inc()
        return None, None
    finally:
        PARSE_DURATION_SECONDS.labels(channel_type="orderbook").observe(time.time() - started_at)



def parse_candle(msg):
    started_at = time.time()
    try:
        push = msg["push"]
        channel = push["channel"]

        _, rest = channel.split("candle-")
        symbol, resolution = rest.split("-")

        pub = push["pub"]
        data = pub["data"]
        offset = pub["offset"]

        record = {
            "symbol": symbol,
            "resolution": resolution,
            "ts": data["t"],
            "open": float(data["o"]),
            "high": float(data["h"]),
            "low": float(data["l"]),
            "close": float(data["c"]),
            "volume": float(data["v"]),
            "offset": offset
        }

        PARSE_TOTAL.labels(channel_type="candle", outcome="success").inc()
        return record, "candle"

    except (KeyError, ValueError, TypeError) as e:
        log_parser.error("parse_candle_failed", extra={"error": str(e), "msg": str(msg)[:200]})
        PARSE_TOTAL.labels(channel_type="candle", outcome="error").inc()
        PARSE_ERRORS_TOTAL.labels(channel_type="candle", error_type=type(e).__name__).inc()
        return None, None
    finally:
        PARSE_DURATION_SECONDS.labels(channel_type="candle").observe(time.time() - started_at)



def parse_market_stats(msg):
    started_at = time.time()
    try:
        push = msg["push"]
        channel = push["channel"]
        symbol = channel.split("market-stats-")[1]

        pub = push["pub"]
        data = pub["data"]
        offset = pub["offset"]

        record = {
            "symbol": symbol,
            "isClosed": data["isClosed"] if isinstance(data["isClosed"], bool) else data["isClosed"] == "true",
            "bestSell": float(data["bestSell"]),
            "bestBuy": float(data["bestBuy"]),
            "volumeSrc": float(data["volumeSrc"]),
            "volumeDst": float(data["volumeDst"]),
            "latest": float(data["latest"]),
            "mark": float(data["mark"]),
            "dayLow": float(data["dayLow"]),
            "dayHigh": float(data["dayHigh"]),
            "dayOpen": float(data["dayOpen"]),
            "dayClose": float(data["dayClose"]),
            "dayChange": float(data["dayChange"]),
            "offset": offset
        }

        PARSE_TOTAL.labels(channel_type="market-stats", outcome="success").inc()
        return record, "market-stats"

    except (KeyError, ValueError, TypeError) as e:
        log_parser.error("parse_market_stats_failed", extra={"error": str(e), "msg": str(msg)[:200]})
        PARSE_TOTAL.labels(channel_type="market-stats", outcome="error").inc()
        PARSE_ERRORS_TOTAL.labels(channel_type="market-stats", error_type=type(e).__name__).inc()
        return None, None
    finally:
        PARSE_DURATION_SECONDS.labels(channel_type="market-stats").observe(time.time() - started_at)



def parse_trade(msg):
    started_at = time.time()
    try:
        push = msg["push"]
        channel = push["channel"]
        symbol = channel.split("trades-")[1]

        pub = push["pub"]
        data = pub["data"]
        offset = pub["offset"]

        record = {
            "symbol": symbol,
            "price": float(data["price"]),
            "volume": float(data["volume"]),
            "side": data["type"],
            "ts": data["time"],
            "offset": offset
        }

        PARSE_TOTAL.labels(channel_type="trades", outcome="success").inc()
        return record, "trades"

    except (KeyError, ValueError, TypeError) as e:
        log_parser.error("parse_trade_failed", extra={"error": str(e), "msg": str(msg)[:200]})
        PARSE_TOTAL.labels(channel_type="trades", outcome="error").inc()
        PARSE_ERRORS_TOTAL.labels(channel_type="trades", error_type=type(e).__name__).inc()
        return None, None
    finally:
        PARSE_DURATION_SECONDS.labels(channel_type="trades").observe(time.time() - started_at)



def parse(msg):
    try:
        channel = msg["push"]["channel"]
    except Exception as e:
        log_parser.error("parse_root_failed", extra={"error": str(e), "msg": str(msg)[:200]})
        PARSE_TOTAL.labels(channel_type="unknown", outcome="error").inc()
        PARSE_ERRORS_TOTAL.labels(channel_type="unknown", error_type="root_malformed").inc()
        return None, None

    if "orderbook" in channel:
        return parse_orderbook(msg)

    if "candle" in channel:
        return parse_candle(msg)

    if "market-stats" in channel:
        return parse_market_stats(msg)

    if "trades" in channel:
        return parse_trade(msg)

    log_parser.error("parse_unknown_channel", extra={"channel": channel, "msg": str(msg)[:200]})
    PARSE_TOTAL.labels(channel_type="unknown", outcome="error").inc()
    PARSE_ERRORS_TOTAL.labels(channel_type="unknown", error_type="unknown_channel").inc()
    return None, None