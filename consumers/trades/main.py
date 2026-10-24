import os
import time

from confluent_kafka.serialization import SerializationContext, MessageField
from prometheus_client import Counter, Histogram, start_http_server

from shared.consumer import get_consumer
from shared.deserializer import get_deserializer
from shared.logger import get_logger
from shared.redis_client import RedisClient
from tradewriter import TradeWriter

logger = get_logger("trades", to_console=True)

METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

messages_processed = Counter(
    "trades_messages_processed",  # exposed as trades_messages_processed_total
    "Total number of trade messages successfully processed",
)

processing_time = Histogram(
    "trades_message_processing_seconds",
    "Time spent processing trade messages",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)

# New: lets you see failures instead of the consumer dying silently
errors = Counter(
    "trades_errors",  # exposed as trades_errors_total
    "Errors by processing stage",
    ["stage"],
)


def main():
    start_http_server(METRICS_PORT)
    logger.info("metrics server started on port %s", METRICS_PORT)

    consumer = get_consumer(group_id="trades_test_group")
    deserializer = get_deserializer()
    redis_client = RedisClient()
    trade_writer = TradeWriter(redis_client)
    logger.info("trades consumer started")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                errors.labels(stage="kafka").inc()
                logger.error("Kafka error: %s", msg.error())
                continue

            start = time.perf_counter()

            try:
                ctx = SerializationContext(msg.topic(), MessageField.VALUE)
                raw_dict = deserializer(msg.value(), ctx)
                if not isinstance(raw_dict, dict):
                    raise ValueError(
                        f"Expected dict, got {type(raw_dict).__name__}"
                    )
            except Exception:
                errors.labels(stage="deserialize").inc()
                logger.exception(
                    "Deserialization failed: topic=%s partition=%s offset=%s",
                    msg.topic(), msg.partition(), msg.offset(),
                )
                continue

            try:
                trade_writer.write(raw_dict)
            except Exception:
                errors.labels(stage="redis").inc()
                logger.exception(
                    "Redis write failed: topic=%s partition=%s offset=%s",
                    msg.topic(), msg.partition(), msg.offset(),
                )
                continue

            processing_time.observe(time.perf_counter() - start)
            messages_processed.inc()

    except KeyboardInterrupt:
        logger.info("Trade consumer interrupted")
    finally:
        consumer.close()
        logger.info("Trade consumer stopped")


if __name__ == "__main__":
    main()