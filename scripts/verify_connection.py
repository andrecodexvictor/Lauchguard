from confluent_kafka.admin import AdminClient
from launchguard.config import kafka_config, load_settings

def main():
    metadata = AdminClient(kafka_config(load_settings())).list_topics(timeout=10)
    print("CONNECTED TO CONFLUENT CLOUD")
    print("Cluster ID:", metadata.cluster_id)
    print("Brokers:", len(metadata.brokers))
    print("Topics:", *sorted(t for t in metadata.topics if not t.startswith("_")), sep="\n  ")

if __name__ == "__main__":
    main()
