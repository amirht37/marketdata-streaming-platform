
import json


class OrderbookWriter:
    def __init__(self, redis_client):
        self.redis = redis_client

    @staticmethod
    def key(symbol: str) -> str:
        return f"orderbook:{symbol.lower()}"

    @staticmethod
    def build(data: dict) -> dict:
        if not isinstance(data, dict):
            raise ValueError("Orderbook data must be a dictionary")

        if "symbol" not in data:
            raise ValueError("Orderbook payload has no symbol")

        if "lastUpdate" not in data:
            raise ValueError("Orderbook payload has no lastUpdate")

        raw_bids = data.get("bids", [])
        raw_asks = data.get("asks", [])

        if not isinstance(raw_bids, (list, tuple)):
            raise ValueError("bids must be a list")

        if not isinstance(raw_asks, (list, tuple)):
            raise ValueError("asks must be a list")

        def normalize_levels(levels, reverse):
            normalized = []

            for level in levels:
                if not isinstance(level, dict):
                    raise ValueError("Each orderbook level must be a dictionary")

                if "price" not in level or "amount" not in level:
                    raise ValueError(
                        "Each orderbook level must contain price and amount"
                    )

                price = str(level["price"])
                amount = str(level["amount"])

                # Validate numeric values without changing their representation.
                price_num = float(price)
                amount_num = float(amount)

                if price_num <= 0:
                    raise ValueError(f"Invalid orderbook price: {price}")

                if amount_num < 0:
                    raise ValueError(f"Invalid orderbook amount: {amount}")

                normalized.append({
                    "price": price,
                    "amount": amount,
                    "_price_num": price_num,
                })

            normalized.sort(
                key=lambda item: item["_price_num"],
                reverse=reverse,
            )

            return normalized

        bids = normalize_levels(raw_bids, reverse=True)
        asks = normalize_levels(raw_asks, reverse=False)

        mapping = {
            "last_update": str(data["lastUpdate"]),
            "bids": json.dumps([
                [level["price"], level["amount"]]
                for level in bids
            ]),
            "asks": json.dumps([
                [level["price"], level["amount"]]
                for level in asks
            ]),
            "best_bid": bids[0]["price"] if bids else "",
            "best_bid_amount": bids[0]["amount"] if bids else "",
            "best_ask": asks[0]["price"] if asks else "",
            "best_ask_amount": asks[0]["amount"] if asks else "",
        }

        return mapping

    def write(self, data: dict):
        mapping = self.build(data)
        print("Mapping built:", mapping.keys())
        key = self.key(data["symbol"])
        print("Writing Redis key:", key)
        self.redis.replace_hash(key, mapping)
        print("REDIS WRITE COMPLETED")
