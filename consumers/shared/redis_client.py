import os

import redis


class RedisClient:
    def __init__(self, host=None, port=None):
        host = host or os.getenv("REDIS_HOST", "redis")
        port = int(port or os.getenv("REDIS_PORT", "6379"))
        self.client = redis.Redis(host=host, port=port, decode_responses=True)

    # --- plain wrappers ---
    def add_to_stream(self, key, data, maxlen=50):
        return self.client.xadd(key, data, maxlen=maxlen, approximate=True)

    def zadd(self, key, mapping):
        return self.client.zadd(key, mapping)

    def hset(self, key, mapping):
        return self.client.hset(key, mapping=mapping)   # fixed: mapping=

    def hmget(self, key, fields):
        return self.client.hmget(key, fields)

    def hgetall(self, key):
        return self.client.hgetall(key)

    def delete(self, *keys):
        return self.client.delete(*keys)

    # --- atomic helper ---
    def replace_hash(self, key, mapping):
        """Delete + HSET in one MULTI/EXEC, so readers never see a half-written hash."""
        pipe = self.client.pipeline(transaction=True)
        pipe.delete(key)
        pipe.hset(key, mapping=mapping)
        pipe.execute()