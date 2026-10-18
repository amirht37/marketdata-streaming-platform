
import time

from confluent_kafka.serialization import SerializationContext, MessageField
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

from shared.consumer import get_consumer
from shared.deserializer import get_deserializer
from shared.logger import get_logger
from shared.redis_client import RedisClient
from orderbook_writer import OrderbookWriter


logger = get_logger(consumer_name="orderbook", to_console=True)

# Prometheus
start_http_server(8001)

processing_time = Histogram(
    "orderbook_processing_seconds",
    "Deserialize, build and Redis write time",
    buckets=(0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.5, 1),
)

end_to_end_delay = Histogram(
    "orderbook_end_to_end_seconds",
    "Kafka timestamp to Redis write",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60),
)

errors = Counter(
    "orderbook_errors_total",
    "Errors by processing stage",
    ["stage"],
)

messages_processed = Counter(
    "orderbook_messages_total",
    "Successfully written orderbook snapshots",
    ["symbol"],
)

last_write_ts = Gauge(
    "orderbook_last_write_timestamp_seconds",
    "Unix timestamp of the last successful Redis write",
    ["symbol"],
)

stage_time = Histogram(
    "orderbook_stage_seconds",
    "Duration of each processing stage",
    ["stage"],
    buckets=(0.0001, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.1, 0.5, 1),
)


def main():
    consumer = None
    redis_client = None

    try:
        consumer = get_consumer(group_id="orderbook_redis_group",topic="nobitex.orderbook",)
        deserializer = get_deserializer()
        redis_client = RedisClient()
        orderbook_writer = OrderbookWriter(redis_client)

        logger.info("Orderbook consumer started")

        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                errors.labels(stage="kafka").inc()
                logger.error("Kafka error: %s", msg.error())
                continue

            # 1. Deserialize
            t0 = time.perf_counter()

            try:
                ctx = SerializationContext(
                    msg.topic(),
                    MessageField.VALUE,
                )
                raw_dict = deserializer(msg.value(), ctx)
                t1 = time.perf_counter()

                if not isinstance(raw_dict, dict):
                    raise ValueError(
                        f"Expected dictionary, got {type(raw_dict).__name__}"
                    )

            except Exception:
                errors.labels(stage="deserialize").inc()
                logger.exception(
                    "Deserialization failed: topic=%s partition=%s offset=%s",
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )
                continue

            stage_time.labels(stage="deserialize").observe(t1 - t0)

            # 2. Validate and build the Redis hash
            try:
                symbol = raw_dict["symbol"]

                if not isinstance(symbol, str) or not symbol.strip():
                    raise ValueError("Missing or invalid symbol")

                mapping = orderbook_writer.build(raw_dict)
                t2 = time.perf_counter()

            except Exception:
                errors.labels(stage="build").inc()
                logger.exception(
                    "Orderbook build failed: topic=%s partition=%s offset=%s",
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )
                continue

            stage_time.labels(stage="build").observe(t2 - t1)

            # 3. Write to Redis
            try:
                key = orderbook_writer.key(symbol)
                redis_client.replace_hash(key, mapping)
                t3 = time.perf_counter()

            except Exception:
                errors.labels(stage="redis").inc()
                logger.exception(
                    "Redis write failed: symbol=%s key=%s topic=%s "
                    "partition=%s offset=%s",
                    symbol,
                    key if "key" in locals() else "<not-created>",
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )
                continue

            stage_time.labels(stage="redis").observe(t3 - t2)
            processing_time.observe(t3 - t0)

            messages_processed.labels(symbol=symbol).inc()
            last_write_ts.labels(symbol=symbol).set(time.time())

            # Kafka timestamps are expressed in milliseconds.
            try:
                timestamp_type, timestamp_ms = msg.timestamp()

                if timestamp_ms is not None and timestamp_ms > 0:
                    delay = time.time() - timestamp_ms / 1000

                    # Avoid recording negative delays caused by clock skew.
                    if delay >= 0:
                        end_to_end_delay.observe(delay)
                    else:
                        logger.warning(
                            "Negative end-to-end delay: symbol=%s delay=%.3f",
                            symbol,
                            delay,
                        )

            except Exception:
                errors.labels(stage="timestamp").inc()
                logger.exception(
                    "Kafka timestamp measurement failed: symbol=%s",
                    symbol,
                )

    except KeyboardInterrupt:
        logger.info("Orderbook consumer interrupted")

    except Exception:
        logger.exception("Orderbook consumer crashed")
        raise

    finally:
        if consumer is not None:
            consumer.close()

        logger.info("Orderbook consumer stopped")


if __name__ == "__main__":
    main()
