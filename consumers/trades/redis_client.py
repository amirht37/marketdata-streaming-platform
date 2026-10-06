import redis
import time

class RedisClient:

    def __init__(self, host='redis', port=6379):
        self.client = redis.Redis(
            host=host,
            port=port,
            decode_responses=True
        )

    def add_to_stream(self, key, data):

        return self.client.xadd(
            key,
            data
        ) 
        
