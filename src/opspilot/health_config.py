"""Read local configuration for service and dependency health checks."""

from dataclasses import dataclass
from pathlib import Path

from opspilot.config_file import parse_timeout_seconds, read_config_object

DEFAULT_HEALTH_TIMEOUT_SECONDS = 5.0
_FIELDS = {
    "service_url", "dependency_urls", "redis_dependency_urls", "mysql_dependency_urls",
    "timeout_seconds",
    "service_log_path",
}


@dataclass(frozen=True)
class HealthConfig:
    service_url: str
    dependency_urls: tuple[str, ...] = ()
    redis_dependency_urls: tuple[str, ...] = ()
    timeout_seconds: float = DEFAULT_HEALTH_TIMEOUT_SECONDS
    mysql_dependency_urls: tuple[str, ...] = ()
    service_log_path: Path | None = None


def _urls(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(url, str) or not url.strip() for url in value):
        raise ValueError(f"{field} must be an array of nonempty URL strings.")
    return tuple(value)


def load_health_config(path: Path) -> HealthConfig:
    return parse_health_config(read_config_object(path, "Health"), config_dir=path.absolute().parent)


def parse_health_config(data: dict[str, object], *, config_dir: Path = Path(".")) -> HealthConfig:
    unsupported = data.keys() - _FIELDS
    if unsupported:
        names = ", ".join(repr(name) for name in sorted(unsupported))
        raise ValueError(f"Health configuration contains unsupported fields: {names}.")
    service_url = data.get("service_url")
    if not isinstance(service_url, str) or not service_url.strip():
        raise ValueError("service_url must be a nonempty URL string.")

    service_log_path = None
    if "service_log_path" in data:
        value = data["service_log_path"]
        if not isinstance(value, str) or not value.strip():
            raise ValueError("service_log_path must be a nonempty path.")
        service_log_path = config_dir / value

    return HealthConfig(
        service_url=service_url,
        dependency_urls=_urls(data.get("dependency_urls", []), "dependency_urls"),
        redis_dependency_urls=_urls(data.get("redis_dependency_urls", []), "redis_dependency_urls"),
        timeout_seconds=parse_timeout_seconds(data.get("timeout_seconds", DEFAULT_HEALTH_TIMEOUT_SECONDS)),
        mysql_dependency_urls=_urls(data.get("mysql_dependency_urls", []), "mysql_dependency_urls"),
        service_log_path=service_log_path,
    )
