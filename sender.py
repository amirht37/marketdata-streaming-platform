# sender.py
from io import BytesIO
from fastavro import parse_schema, schemaless_writer
from kafka import KafkaProducer
import json
import os

from logger import logger

# ── Kafka ──────────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers=[os.getenv("KAFKA_BROKER", "kafka:29092")],
    compression_type="gzip",
    linger_ms=5,
    acks="all",
    retries=5,
    retry_backoff_ms=300,
)

# ── Schemas ────────────────────────────────────────────────
SCHEMA_DIR = "/app/schemas"

def _load(name: str):
    with open(f"{SCHEMA_DIR}/{name}.avsc", "r") as f:
        return parse_schema(json.load(f))

SCHEMAS = {
    "orderbook":    _load("orderbook"),
    "candle":       _load("candle"),
    "market-stats": _load("market_stats"),
    "trades":       _load("trades"),
}

# ── Topic mapping ──────────────────────────────────────────
def _topic(domain: str, symbol: str) -> str:
    clean = symbol.replace("-", "").lower()  # BTC-USDT → btcusdt
    return f"nobitex_{domain.replace('-', '_')}_{clean}"

# ── Avro encode ────────────────────────────────────────────
def _encode(domain: str, record: dict) -> bytes:
    buf = BytesIO()
    schemaless_writer(buf, SCHEMAS[domain], record)
    return buf.getvalue()

# ── Public interface ───────────────────────────────────────
def send(record: dict, domain: str):
    try:
        topic   = _topic(domain, record["symbol"])
        payload = _encode(domain, record)
        producer.send(topic, value=payload, key=record["symbol"].encode())
        logger.info("sent", extra={
            "domain": domain,
            "symbol": record["symbol"],
            "topic":  topic
        })
    except Exception as e:
        logger.error("send_failed", extra={
            "domain": domain,
            "error":  str(e)
        })

def flush():
    producer.flush()

def close():
    producer.flush()
    producer.close()
