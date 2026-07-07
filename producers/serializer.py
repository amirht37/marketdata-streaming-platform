import json
import os
import time
from pathlib import Path

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from prometheus_client import Counter, Histogram

from Logger import log_serializer

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SCHEMA_DIR = Path(os.getenv("SCHEMA_DIR", "/app/schemas"))
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

CHANNEL_CONFIG = {
    "orderbook": {
        "schema_file": "orderbook.avsc",
        "subject": "nobitex.orderbook-value",
    },
    "candle": {
        "schema_file": "candles.avsc",
        "subject": "nobitex.candles-value",
    },
    "market-stats": {
        "schema_file": "market-stats.avsc",
        "subject": "nobitex.market-stats-value",
    },
    "trades": {
        "schema_file": "trades.avsc",
        "subject": "nobitex.trades-value",
    },
}

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------

SERIALIZE_TOTAL = Counter(
    "serialize_messages_total",
    "Number of messages serialized, by channel type and outcome",
    ["channel_type", "outcome"],  # success | error
)

SERIALIZE_ERRORS_TOTAL = Counter(
    "serialize_errors_total",
    "Number of serialization failures, by channel type and error kind",
    ["channel_type", "error_type"],
)

SERIALIZE_DURATION_SECONDS = Histogram(
    "serialize_duration_seconds",
    "Time spent serializing a normalized record to Avro bytes",
    ["channel_type"],
    buckets=(0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5),
)

SCHEMA_REGISTRY_UP = Counter(
    "schema_registry_operations_total",
    "Schema registry operations during startup",
    ["operation", "outcome"],  # register | load, success | error
)

# ---------------------------------------------------------------------------
# Schema Registry client + Avro serializers (lazy init)
# ---------------------------------------------------------------------------

_registry_client: SchemaRegistryClient | None = None
_serializers: dict[str, AvroSerializer] = {}


def _get_registry_client() -> SchemaRegistryClient:
    global _registry_client
    if _registry_client is None:
        _registry_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        log_serializer.info(
            "schema_registry_client_initialized",
            extra={"url": SCHEMA_REGISTRY_URL},
        )
    return _registry_client


def _load_schema_text(schema_file: str) -> str:
    path = SCHEMA_DIR / schema_file
    if not path.exists():
        raise FileNotFoundError(f"schema file not found: {path}")
    return path.read_text(encoding="utf-8")


def init_serializers() -> None:
    """
    Call once at app startup.
    Loads .avsc files, registers subjects, builds AvroSerializer per channel.
    """
    client = _get_registry_client()

    for channel_type, cfg in CHANNEL_CONFIG.items():
        try:
            schema_str = _load_schema_text(cfg["schema_file"])
            subject = cfg["subject"]

            schema = Schema(schema_str, schema_type="AVRO")
            client.register_schema(subject, schema)
            SCHEMA_REGISTRY_UP.labels(operation="register", outcome="success").inc()

            _serializers[channel_type] = AvroSerializer(
                client,
                schema_str,
                lambda record, ctx: record,
            )

            log_serializer.info(
                "serializer_initialized",
                extra={
                    "channel_type": channel_type,
                    "subject": subject,
                    "schema_file": cfg["schema_file"],
                },
            )

        except Exception as e:
            SCHEMA_REGISTRY_UP.labels(operation="register", outcome="error").inc()
            log_serializer.error(
                "serializer_init_failed",
                extra={
                    "channel_type": channel_type,
                    "error": str(e),
                    "schema_file": cfg["schema_file"],
                },
            )
            raise


def serialize(record: dict, channel_type: str) -> bytes | None:
    """
    Serialize a parsed record to Confluent Avro wire-format bytes.
    Returns None on failure (same contract as parser).
    """
    started_at = time.time()

    try:
        if channel_type not in CHANNEL_CONFIG:
            log_serializer.error(
                "serialize_unknown_channel",
                extra={"channel_type": channel_type},
            )
            SERIALIZE_TOTAL.labels(channel_type="unknown", outcome="error").inc()
            SERIALIZE_ERRORS_TOTAL.labels(
                channel_type="unknown", error_type="unknown_channel"
            ).inc()
            return None

        serializer = _serializers.get(channel_type)
        if serializer is None:
            log_serializer.error(
                "serialize_serializer_not_initialized",
                extra={"channel_type": channel_type},
            )
            SERIALIZE_TOTAL.labels(channel_type=channel_type, outcome="error").inc()
            SERIALIZE_ERRORS_TOTAL.labels(
                channel_type=channel_type, error_type="schema_not_found"
            ).inc()
            return None

        ctx = SerializationContext(
            topic=CHANNEL_CONFIG[channel_type]["subject"].replace("-value", ""),
            field=MessageField.VALUE,
        )

        payload = serializer(record, ctx)

        SERIALIZE_TOTAL.labels(channel_type=channel_type, outcome="success").inc()
        return payload

    except Exception as e:
        error_type = type(e).__name__
        log_serializer.error(
            "serialize_failed",
            extra={
                "channel_type": channel_type,
                "error": str(e),
                "error_type": error_type,
                "record_preview": json.dumps(record, default=str)[:200],
            },
        )
        SERIALIZE_TOTAL.labels(channel_type=channel_type, outcome="error").inc()
        SERIALIZE_ERRORS_TOTAL.labels(
            channel_type=channel_type, error_type=error_type
        ).inc()
        return None

    finally:
        SERIALIZE_DURATION_SECONDS.labels(
            channel_type=channel_type if channel_type in CHANNEL_CONFIG else "unknown"
        ).observe(time.time() - started_at)