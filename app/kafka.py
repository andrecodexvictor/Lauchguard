from __future__ import annotations
import logging
from threading import RLock
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from launchguard.config import Settings, kafka_config, schema_registry_config
from launchguard.schemas import CHECKOUT_SCHEMA, DEPLOYMENT_SCHEMA

log = logging.getLogger(__name__)

class EventProducer:
    def __init__(self, settings: Settings):
        self.lock = RLock()
        self.producer = Producer(kafka_config(settings))
        registry = SchemaRegistryClient(schema_registry_config(settings))
        self.serializers = {
            "checkout_events": AvroSerializer(registry, CHECKOUT_SCHEMA, conf={"auto.register.schemas": True}),
            "deployment_events": AvroSerializer(registry, DEPLOYMENT_SCHEMA, conf={"auto.register.schemas": True})}
        self.last_error = None

    def delivery(self, err, msg):
        if err:
            self.last_error = str(err)
            log.error("Kafka delivery failed on %s: %s", msg.topic(), err)

    def send(self, topic: str, key: str, row: dict, sync: bool = False):
        with self.lock:
            payload = self.serializers[topic](row, SerializationContext(topic, MessageField.VALUE))
            self.last_error = None
            self.producer.produce(topic, key=key, value=payload, callback=self.delivery)
            if sync:
                pending = self.producer.flush(15)
                if pending or self.last_error:
                    raise RuntimeError("Kafka delivery failed or timed out")
            else:
                self.producer.poll(0)

    def close(self):
        with self.lock:
            self.producer.flush(10)
