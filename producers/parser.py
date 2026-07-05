import logging

logger = logging.getLogger(__name__)


def parse_orderbook(msg):
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
        return record, "orderbook"

    except (KeyError, ValueError, TypeError) as e:
        logger.error("parse_orderbook_failed", extra={"error": str(e), "msg": str(msg)[:200]})
        return None, None



def parse_candle(msg):
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

        return record, "candle"

    except (KeyError, ValueError, TypeError) as e:
        logger.error("parse_candle_failed", extra={"error": str(e), "msg": str(msg)[:200]})
        return None, None



def parse_market_stats(msg):
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

        return record, "market-stats"

    except (KeyError, ValueError, TypeError) as e:
        logger.error("parse_market_stats_failed", extra={"error": str(e), "msg": str(msg)[:200]})
        return None, None



def parse_trade(msg):
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

        return record, "trades"

    except (KeyError, ValueError, TypeError) as e:
        logger.error("parse_trade_failed", extra={"error": str(e), "msg": str(msg)[:200]})
        return None, None



def parse(msg):
    try:
        channel = msg["push"]["channel"]
    except Exception as e:
        logger.error("parse_root_failed", extra={"error": str(e), "msg": str(msg)[:200]})
        return None, None

    if "orderbook" in channel:
        return parse_orderbook(msg)

    if "candle" in channel:
        return parse_candle(msg)

    if "market-stats" in channel:
        return parse_market_stats(msg)

    if "trades" in channel:
        return parse_trade(msg)

    logger.error("parse_unknown_channel", extra={"channel": channel, "msg": str(msg)[:200]})
    return None, None
