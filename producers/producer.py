# producers/base_producer.py

import io
import json
from fastavro import writer
from kafka import KafkaProducer

class BaseProducer:
    def __init__(self, schema_path, topic):
        self.topic = topic

        # Load Avro schema
        with open(schema_path, "r") as f:
            self.schema = json.load(f)

        # Kafka producer instance
        self.producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"],
            acks="all",
            retries=5,
            linger_ms=5,
            batch_size=32 * 1024,
        )

    def serialize(self, record):
        buf = io.BytesIO()
        writer(buf, self.schema, [record])
        return buf.getvalue()

    def send(self, record):
        avro_bytes = self.serialize(record)
        self.producer.send(self.topic, avro_bytes)
