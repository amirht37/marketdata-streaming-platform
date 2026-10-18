import os
from confluent_kafka import Consumer

def get_consumer(group_id:str | None = None ,
                  topic:str | None= None):
    """
    Create and subscribe a Kafka consumer.

    Args:
        group_id: Consumer group ID. Defaults to GROUP_ID
            environment variable .
        topic: Kafka topic. Defaults to KAFKA_TOPIC
            environment variable .

    Returns:
        Configured and subscribed Kafka Consumer.
    """
    if group_id is None:
        group_id= os.getenv("GROUP_ID","trades_group")
    if topic is None:
        topic=os.getenv("KAFKA_TOPIC","nobitex.trades")


    config = {
        "bootstrap.servers":os.getenv("BOOTSTRAP_SERVER","kafka:29092"),
        "group.id": group_id,
        "auto.offset.reset": "earliest"
    }
    consumer=Consumer(config)
    consumer.subscribe([topic])
    print(f"consumer subscribed to {topic} as {group_id}")
    return consumer


