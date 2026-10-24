import os
import redis
import time

class RedisClient:

    def __init__(self, host=None, port=None):
        host = host or os.getenv('REDIS_HOST', 'redis')
        port = int(port or os.getenv('REDIS_PORT', '6379'))
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
        
