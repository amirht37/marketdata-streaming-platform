


class TradeWriter:
    def __init__(self,redis_client):
        self.redis=redis_client




    def write(self,data):

        symbol = data["symbol"]
        key = f"trades:{symbol}"
        global_key='trades'
        self.redis.add_to_stream(key,data)
        self.redis.add_to_stream(global_key,data)
