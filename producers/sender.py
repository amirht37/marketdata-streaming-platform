import os
import time
from typing import Any

from confluent_kafka import Producer
from prometheus_client import Counter, Histogram

from Logger import log_sender

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"
)

TOPIC_MAP = {
    "orderbook": "nobitex.orderbook",
    "candle": "nobitex.candles",
    "market-stats": "nobitex.market-stats",
    "trades": "nobitex.trades",
}

# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------

KAFKA_PRODUCE_TOTAL = Counter(
    "kafka_produce_messages_total",
    "Number of Kafka produce attempts, by channel type and outcome",
    ["channel_type", "outcome"],  # success | error | queued
)

KAFKA_PRODUCE_ERRORS_TOTAL = Counter(
    "kafka_produce_errors_total",
    "Number of Kafka produce failures, by channel type and error kind",
    ["channel_type", "error_type"],
    # unknown_channel | producer_not_initialized | BufferError | delivery_failed
)

KAFKA_PRODUCE_DURATION_SECONDS = Histogram(
    "kafka_produce_duration_seconds",
    "Time spent enqueueing a message to Kafka",
    ["channel_type"],
    buckets=(0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5),
)

KAFKA_DELIVERY_TOTAL = Counter(
    "kafka_delivery_reports_total",
    "Kafka broker delivery reports from delivery callback",
    ["channel_type", "outcome"],  # delivered | failed
)

# ---------------------------------------------------------------------------
# Kafka producer (lazy init)
# ---------------------------------------------------------------------------

_producer: Producer | None = None


def _delivery_report(err, msg) -> None:
    """Async broker ack callback."""
    headers = dict(msg.headers() or [])
    channel_type = headers.get("channel_type", b"unknown").decode("utf-8")

    if err is not None:
        KAFKA_DELIVERY_TOTAL.labels(channel_type=channel_type, outcome="failed").inc()
        log_sender.error(
            "kafka_delivery_failed",
            extra={
                "channel_type": channel_type,
                "topic": msg.topic(),
                "partition": msg.partition(),
                "error": str(err),
            },
        )
        return

    KAFKA_DELIVERY_TOTAL.labels(channel_type=channel_type, outcome="delivered").inc()


def init_sender() -> None:
    global _producer

    _producer = Producer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "client.id": "crypto-producer",
            "acks": "all",
            "linger.ms": 5,
            "compression.type": "lz4",
            "enable.idempotence": True,
        }
    )

    log_sender.info(
        "kafka_producer_initialized",
        extra={"bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS},
    )


def poll_sender(timeout: float = 0) -> None:
    """Drain delivery callbacks. Call from async loop periodically."""
    if _producer is not None:
        _producer.poll(timeout)


def send(payload: bytes, channel_type: str, key: str | None = None) -> bool:
    """
    Send serialized Avro bytes to Kafka.
    Returns True if enqueued, False on failure.
    """
    started_at = time.time()

    try:
        if channel_type not in TOPIC_MAP:
            log_sender.error(
                "kafka_unknown_channel",
                extra={"channel_type": channel_type},
            )
            KAFKA_PRODUCE_TOTAL.labels(channel_type="unknown", outcome="error").inc()
            KAFKA_PRODUCE_ERRORS_TOTAL.labels(
                channel_type="unknown", error_type="unknown_channel"
            ).inc()
            return False

        if _producer is None:
            log_sender.error(
                "kafka_producer_not_initialized",
                extra={"channel_type": channel_type},
            )
            KAFKA_PRODUCE_TOTAL.labels(channel_type=channel_type, outcome="error").inc()
            KAFKA_PRODUCE_ERRORS_TOTAL.labels(
                channel_type=channel_type, error_type="producer_not_initialized"
            ).inc()
            return False

        topic = TOPIC_MAP[channel_type]

        _producer.produce(
            topic=topic,
            key=key.encode("utf-8") if key else None,
            value=payload,
            headers=[("channel_type", channel_type.encode("utf-8"))],
            on_delivery=_delivery_report,
        )

        # non-blocking flush of internal queue + callback dispatch
        _producer.poll(0)

        KAFKA_PRODUCE_TOTAL.labels(channel_type=channel_type, outcome="queued").inc()
        return True

    except BufferError as e:
        # local queue full — poll and retry once
        log_sender.error(
            "kafka_produce_buffer_full",
            extra={"channel_type": channel_type, "error": str(e)},
        )
        poll_sender(1.0)

        try:
            _producer.produce(
                topic=TOPIC_MAP[channel_type],
                key=key.encode("utf-8") if key else None,
                value=payload,
                headers=[("channel_type", channel_type.encode("utf-8"))],
                on_delivery=_delivery_report,
            )
            _producer.poll(0)
            KAFKA_PRODUCE_TOTAL.labels(channel_type=channel_type, outcome="queued").inc()
            return True

        except Exception as retry_err:
            log_sender.error(
                "kafka_produce_failed_after_retry",
                extra={"channel_type": channel_type, "error": str(retry_err)},
            )
            KAFKA_PRODUCE_TOTAL.labels(channel_type=channel_type, outcome="error").inc()
            KAFKA_PRODUCE_ERRORS_TOTAL.labels(
                channel_type=channel_type, error_type=type(retry_err).__name__
            ).inc()
            return False

    except Exception as e:
        log_sender.error(
            "kafka_produce_failed",
            extra={"channel_type": channel_type, "error": str(e)},
        )
        KAFKA_PRODUCE_TOTAL.labels(channel_type=channel_type, outcome="error").inc()
        KAFKA_PRODUCE_ERRORS_TOTAL.labels(
            channel_type=channel_type, error_type=type(e).__name__
        ).inc()
        return False

    finally:
        KAFKA_PRODUCE_DURATION_SECONDS.labels(
            channel_type=channel_type if channel_type in TOPIC_MAP else "unknown"
        ).observe(time.time() - started_at)


def flush_sender(timeout: float = 10.0) -> None:
    if _producer is not None:
        remaining = _producer.flush(timeout)
        if remaining > 0:
            log_sender.error(
                "kafka_flush_timeout",
                extra={"remaining_messages": remaining, "timeout": timeout},
            )
