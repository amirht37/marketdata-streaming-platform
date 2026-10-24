from redis_client import RedisClient
import time 
from consumer import get_consumer
from confluent_kafka.serialization import SerializationContext, MessageField
from tradewriter import TradeWriter
from datetime import datetime, timezone
from deserializer import get_deserializer
from shared.logger import get_logger
from shared.consumer import get_consumer
from shared.deserializer import get_deserializer
from shared.logger import get_logger
from shared.redis_client import RedisClient


logger = get_logger('trades',to_console=True)


consumer = get_consumer(group_id="trades_test_group")
logger.info("consumer started")
deserializer=get_deserializer()

redis_client = RedisClient()
trade_writer=TradeWriter(redis_client)

try:
    while True:
        msg = consumer.poll(1.0)
        start = time.perf_counter()
        if msg is None:
            print("NO MESSAGE", flush=True)
            continue

        if msg.error():
            logger.error("Kafka error: %s", msg.error())
            continue

        ctx = SerializationContext(
            msg.topic(),
            MessageField.VALUE
        )
        
        raw_dict = deserializer(msg.value(),ctx)
        trade_writer.write(raw_dict)
        elapsed = time.perf_counter() - start
        print(f"{elapsed * 1000:.3f} ms")


except KeyboardInterrupt:
    logger.info("Trade consumer stopped")

finally:
    consumer.close()