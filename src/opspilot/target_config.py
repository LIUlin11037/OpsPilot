"""Configuration for the read-only workflows of a diagnostic target."""

from dataclasses import dataclass
from pathlib import Path

from opspilot.config_file import parse_timeout_seconds, read_config_object
from opspilot.health_config import HealthConfig, parse_health_config


@dataclass(frozen=True)
class RabbitMQTargetConfig:
    management_url: str
    queue: str
    vhost: str = "/"
    timeout_seconds: float = 5.0


@dataclass(frozen=True)
class TargetConfig:
    health: HealthConfig | None = None
    rabbitmq: RabbitMQTargetConfig | None = None


def _rabbitmq_string(data: dict[str, object], field: str, default: str = "") -> str:
    value = data.get(field, default)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"rabbitmq.{field} must be a nonempty string.")
    return value


def _parse_rabbitmq(data: dict[str, object]) -> RabbitMQTargetConfig:
    if data.keys() - {"management_url", "queue", "vhost", "timeout_seconds"}:
        raise ValueError("RabbitMQ configuration contains unsupported fields.")
    return RabbitMQTargetConfig(
        management_url=_rabbitmq_string(data, "management_url"),
        queue=_rabbitmq_string(data, "queue"),
        vhost=_rabbitmq_string(data, "vhost", "/"),
        timeout_seconds=parse_timeout_seconds(data.get("timeout_seconds", 5.0)),
    )


def load_target_config(path: Path) -> TargetConfig:
    data = read_config_object(path, "Target")
    if data.keys() - {"health", "rabbitmq"}:
        raise ValueError("Target configuration contains unsupported fields.")
    if not data:
        raise ValueError("Target configuration requires a health or rabbitmq section.")
    health = data.get("health")
    rabbitmq = data.get("rabbitmq")
    if "health" in data and not isinstance(health, dict):
        raise ValueError("health must be a JSON object.")
    if "rabbitmq" in data and not isinstance(rabbitmq, dict):
        raise ValueError("rabbitmq must be a JSON object.")
    return TargetConfig(
        health=parse_health_config(health) if isinstance(health, dict) else None,
        rabbitmq=_parse_rabbitmq(rabbitmq) if isinstance(rabbitmq, dict) else None,
    )
