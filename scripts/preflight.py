"""Read-only connection diagnostics. Never prints credentials or response bodies."""
import socket
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from launchguard.config import load_settings, kafka_config, schema_registry_config


def main():
    settings = load_settings()
    ok = True
    try:
        subjects = SchemaRegistryClient(schema_registry_config(settings) | {"timeout": 10}).get_subjects()
        print("Schema Registry: authentication OK")
        for subject in ("checkout_events-value", "deployment_events-value"):
            present = subject in subjects
            print(f"  {subject}: {'present' if present else 'missing'}")
            ok = ok and present
    except Exception as error:
        status = getattr(error, "http_status_code", None)
        print(f"Schema Registry: {type(error).__name__}; HTTP status: {status}")
        if status == 401:
            print("  Public Schema Registry requires a resource-scoped Registry key; Global keys are unsupported.")
        ok = False
    dns_ok = True
    for server in settings.bootstrap_servers.split(","):
        try:
            host, port = server.strip().rsplit(":", 1)
            socket.getaddrinfo(host, int(port))
        except (OSError, ValueError):
            dns_ok = False
    if not dns_ok:
        print("Kafka: bootstrap DNS resolution failed in this execution environment.")
        print("  Credentials have not been tested. Run from a network that can reach the configured broker.")
        return 1
    try:
        metadata = AdminClient(kafka_config(settings) | {"log_level": 0}).list_topics(timeout=10)
        print("Kafka: authenticated metadata received")
        for topic in ("checkout_events", "deployment_events"):
            found = metadata.topics.get(topic)
            ready = found is not None and found.error is None
            print(f"  {topic}: {'ready' if ready else 'unavailable'}")
            ok = ok and ready
    except Exception as error:
        code = error.args[0].code() if error.args and hasattr(error.args[0], "code") else None
        print(f"Kafka: {type(error).__name__}; error code: {code}")
        ok = False
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
