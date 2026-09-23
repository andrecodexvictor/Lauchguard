from __future__ import annotations
import os
from dataclasses import dataclass
from dotenv import load_dotenv
load_dotenv()

@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str
    kafka_api_key: str
    kafka_api_secret: str
    schema_registry_url: str
    schema_registry_api_key: str
    schema_registry_api_secret: str

def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value or value == "replace_me":
        raise RuntimeError(f"Missing {name}; configure .env locally")
    return value

def load_settings() -> Settings:
    return Settings(*(_required(name) for name in (
        "KAFKA_BOOTSTRAP_SERVERS", "KAFKA_API_KEY", "KAFKA_API_SECRET",
        "SCHEMA_REGISTRY_URL", "SCHEMA_REGISTRY_API_KEY", "SCHEMA_REGISTRY_API_SECRET")))

def kafka_config(settings: Settings) -> dict[str, str]:
    return {"bootstrap.servers": settings.bootstrap_servers, "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN", "sasl.username": settings.kafka_api_key,
            "sasl.password": settings.kafka_api_secret, "client.id": "launchguard-bootstrap"}

def schema_registry_config(settings: Settings) -> dict[str, str]:
    return {"url": settings.schema_registry_url,
            "basic.auth.user.info": f"{settings.schema_registry_api_key}:{settings.schema_registry_api_secret}"}
