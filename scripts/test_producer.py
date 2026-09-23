import uuid
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from launchguard.config import load_settings, kafka_config, schema_registry_config
from launchguard.schemas import CHECKOUT_SCHEMA, DEPLOYMENT_SCHEMA

def main():
    settings = load_settings()
    producer = Producer(kafka_config(settings))
    registry = SchemaRegistryClient(schema_registry_config(settings))
    rows = [
        ("deployment_events", DEPLOYMENT_SCHEMA, "checkout-service",
         {"deployment_id": str(uuid.uuid4()), "service": "checkout-service", "version": "2.3.7", "action": "DEPLOY"}),
        ("checkout_events", CHECKOUT_SCHEMA, "customer-001",
         {"event_id": str(uuid.uuid4()), "customer_id": "customer-001", "service": "checkout-service",
          "region": "BR", "plan": "enterprise", "payment_method": "PIX", "success": True,
          "amount": 249.90, "latency_ms": 210})]
    for topic, schema, key, row in rows:
        serializer = AvroSerializer(registry, schema, conf={"auto.register.schemas": True})
        producer.produce(topic, key=key, value=serializer(row, SerializationContext(topic, MessageField.VALUE)))
    if producer.flush(15):
        raise RuntimeError("Kafka delivery timed out")
    print("TEST EVENTS SENT")

if __name__ == "__main__":
    main()
