from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient

import os


Schema_registry_client=SchemaRegistryClient({

    "url":os.getenv("SCHEMA_REGISTRY_URL","http://schema-registry:8081")
})


def get_deserializer():
    deserializer=AvroDeserializer(Schema_registry_client,schema_str=None)

    return deserializer

