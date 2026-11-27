"""Validate the Jaeger v3 read endpoint and its request limits."""

from dataclasses import dataclass
from urllib.parse import urlsplit

from opspilot.config_file import parse_timeout_seconds

MAX_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_SPANS = 1024


@dataclass(frozen=True)
class JaegerConfig:
    base_url: str
    timeout_seconds: float = 5


def parse_jaeger_config(value: object) -> JaegerConfig:
    if not isinstance(value, dict):
        raise ValueError("jaeger must be a JSON object.")
    unsupported = value.keys() - {"base_url", "timeout_seconds"}
    if unsupported:
        raise ValueError(f"jaeger contains unsupported fields: {', '.join(sorted(unsupported))}.")
    base = value.get("base_url")
    message = "jaeger.base_url must be an HTTP(S) URL without credentials, a query or a fragment."
    if not isinstance(base, str) or any(char.isspace() or ord(char) < 32 for char in base):
        raise ValueError(message)
    try:
        url = urlsplit(base)
        valid_url = (
            url.scheme in ("http", "https") and url.hostname and url.port != 0
            and url.username is None and "?" not in base and "#" not in base
        )
    except ValueError:
        valid_url = False
    if not valid_url:
        raise ValueError(message)
    try:
        timeout = parse_timeout_seconds(value.get("timeout_seconds", 5))
    except ValueError as error:
        raise ValueError(f"jaeger.{error}") from error
    if timeout > 15:
        raise ValueError("jaeger.timeout_seconds must be at most 15 seconds.")
    return JaegerConfig(base.rstrip("/"), timeout)
