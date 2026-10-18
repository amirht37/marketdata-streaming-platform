def stream_flattened_orderbook(data: dict):
    symbol = data["symbol"]
    last_update = data["lastUpdate"]
    offset=data['offset']

    for side, entries in (("bid", data.get("bids", [])), ("ask", data.get("asks", []))):
        for entry in entries:
            yield {
                "symbol": symbol,
                "side": side,
                "price": entry["price"],
                "amount": entry["amount"],
                "last_update": last_update,
            }
