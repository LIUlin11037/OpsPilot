"""Read local configuration for service and dependency health checks."""

import json
from dataclasses import dataclass
from math import isfinite
from pathlib import Path

DEFAULT_HEALTH_TIMEOUT_SECONDS = 5.0
_FIELDS = {
    "service_url", "dependency_urls", "redis_dependency_urls", "mysql_dependency_urls",
    "timeout_seconds",
}


@dataclass(frozen=True)
class HealthConfig:
    service_url: str
    dependency_urls: tuple[str, ...] = ()
    redis_dependency_urls: tuple[str, ...] = ()
    timeout_seconds: float = DEFAULT_HEALTH_TIMEOUT_SECONDS
    mysql_dependency_urls: tuple[str, ...] = ()


def _urls(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(url, str) or not url.strip() for url in value):
        raise ValueError(f"{field} must be an array of nonempty URL strings.")
    return tuple(value)


def load_health_config(path: Path) -> HealthConfig:
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
    except OSError as error:
        raise ValueError(f"Cannot read the health configuration file: {error.strerror}.") from error
    except (UnicodeError, json.JSONDecodeError) as error:
        raise ValueError("Health configuration must be valid UTF-8 JSON.") from error

    if not isinstance(data, dict):
        raise ValueError("Health configuration must be a JSON object.")
    if data.keys() - _FIELDS:
        raise ValueError("Health configuration contains unsupported fields.")
    service_url = data.get("service_url")
    if not isinstance(service_url, str) or not service_url.strip():
        raise ValueError("service_url must be a nonempty URL string.")

    raw_timeout = data.get("timeout_seconds", DEFAULT_HEALTH_TIMEOUT_SECONDS)
    if type(raw_timeout) not in (float, int):
        raise ValueError("timeout_seconds must be a finite positive number.")
    try:
        timeout = float(raw_timeout)
    except OverflowError as error:
        raise ValueError("timeout_seconds must be a finite positive number.") from error
    if not isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout_seconds must be a finite positive number.")

    return HealthConfig(
        service_url=service_url,
        dependency_urls=_urls(data.get("dependency_urls", []), "dependency_urls"),
        redis_dependency_urls=_urls(data.get("redis_dependency_urls", []), "redis_dependency_urls"),
        timeout_seconds=timeout,
        mysql_dependency_urls=_urls(data.get("mysql_dependency_urls", []), "mysql_dependency_urls"),
    )
