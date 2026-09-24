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
    def __init__(self, settings: Settings, on_delivery=None):
        self.lock = RLock()
        self.on_delivery = on_delivery
        self.producer = Producer(kafka_config(settings) | {
            "enable.idempotence": True, "message.timeout.ms": 15000,
        })
        registry = SchemaRegistryClient(schema_registry_config(settings))
        self.serializers = {
            "checkout_events": AvroSerializer(registry, CHECKOUT_SCHEMA, conf={"auto.register.schemas": True}),
            "deployment_events": AvroSerializer(registry, DEPLOYMENT_SCHEMA, conf={"auto.register.schemas": True}),
        }

    def send(self, topic: str, key: str, row: dict, sync: bool = False):
        # Track this record's acknowledgement, not a shared error cleared by another send.
        result = {"acknowledged": False}

        def delivered(error, message):
            result["acknowledged"] = error is None
            if self.on_delivery:
                self.on_delivery(error is None)
            if error:
                log.warning("Kafka delivery failed on %s (code %s)", message.topic(), error.code())

        with self.lock:
            payload = self.serializers[topic](row, SerializationContext(topic, MessageField.VALUE))
            self.producer.produce(topic, key=key, value=payload, on_delivery=delivered)
            if sync:
                self.producer.flush(16)
                if not result["acknowledged"]:
                    raise RuntimeError("Kafka delivery failed or timed out")
            else:
                self.producer.poll(0)

    def close(self):
        with self.lock:
            self.producer.flush(5)
