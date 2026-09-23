from __future__ import annotations
import json
import logging
from threading import Event, Thread
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext
from app.state import DashboardState
from launchguard.config import Settings, kafka_config, schema_registry_config

log = logging.getLogger(__name__)
TOPICS = ("release_metrics", "impact_forecast", "incident_signals")

def decode(value, topic, avro):
    if not value:
        return None
    try:
        row = avro(value, SerializationContext(topic, MessageField.VALUE))
        if isinstance(row, dict):
            return row
    except Exception:
        pass
    for payload in (value[5:] if value[:1] == b"\0" else b"", value):
        try:
            row = json.loads(payload)
            if isinstance(row, dict):
                return row.get("after") if isinstance(row.get("after"), dict) else row
        except (ValueError, UnicodeDecodeError):
            continue
    return None

class OutputConsumers:
    def __init__(self, settings: Settings, state: DashboardState, stop: Event):
        self.settings, self.state, self.stop = settings, state, stop
        self.avro = AvroDeserializer(SchemaRegistryClient(schema_registry_config(settings)))
        self.threads = [Thread(target=self.run, args=(t,), daemon=True, name="consume-"+t) for t in TOPICS]

    def start(self):
        for thread in self.threads:
            thread.start()

    def run(self, topic):
        conf = kafka_config(self.settings) | {"group.id": "launchguard-ui-"+topic, "auto.offset.reset": "latest",
                                             "enable.auto.commit": True, "allow.auto.create.topics": False}
        while not self.stop.is_set():
            consumer = None
            try:
                consumer = Consumer(conf)
                metadata = consumer.list_topics(timeout=8)
                ready = topic in metadata.topics and metadata.topics[topic].error is None
                self.state.ready[topic] = ready
                if not ready:
                    self.stop.wait(10)
                    continue
                consumer.subscribe([topic])
                while not self.stop.is_set():
                    msg = consumer.poll(1)
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                            break
                        log.warning("Consumer %s error: %s", topic, msg.error().code())
                        continue
                    row = decode(msg.value(), topic, self.avro)
                    if row:
                        self.state.ingest(topic, row)
                self.state.ready[topic] = False
            except Exception as exc:
                self.state.ready[topic] = False
                log.warning("Consumer %s waiting: %s", topic, type(exc).__name__)
                self.stop.wait(10)
            finally:
                if consumer:
                    consumer.close()
