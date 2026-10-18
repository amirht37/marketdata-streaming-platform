import os

# Kafka
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TRADES_TOPIC", "orderbook")
KAFKA_CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "orderbook-consumer-group")

# Schema Registry
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")

# Postgres
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "exchange")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")  # no default — should fail loudly if missing

# Connection pool sizing
POSTGRES_POOL_MIN_CONN = int(os.environ.get("POSTGRES_POOL_MIN_CONN", "2"))
POSTGRES_POOL_MAX_CONN = int(os.environ.get("POSTGRES_POOL_MAX_CONN", "10"))
