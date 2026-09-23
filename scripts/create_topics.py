from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from launchguard.config import kafka_config, load_settings

def main():
    admin = AdminClient(kafka_config(load_settings()))
    for topic, future in admin.create_topics([NewTopic(t, num_partitions=1)
                        for t in ("checkout_events", "deployment_events")], request_timeout=15).items():
        try:
            future.result()
            print("CREATED:", topic)
        except KafkaException as exc:
            if "already exists" in str(exc).lower() or "TOPIC_ALREADY_EXISTS" in str(exc):
                print("EXISTS:", topic)
            else:
                raise

if __name__ == "__main__":
    main()
