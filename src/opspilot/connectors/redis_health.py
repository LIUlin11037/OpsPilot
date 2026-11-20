"""Read a Redis instance's PING response."""

import socket
from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import isfinite
from urllib.parse import SplitResult, urlsplit

from opspilot.core.diagnosis import HealthRead

_PING = b"*1\r\n$4\r\nPING\r\n"
_RESPONSE_LIMIT = 1024
_REDIS_ERROR_REASONS = {
    b"-NOAUTH": "authentication_failed",
    b"-NOPERM": "access_denied",
}


@dataclass(frozen=True)
class RedisHealthConnector:
    endpoint: str
    timeout_seconds: float = 5.0
    _url: SplitResult = field(init=False, repr=False)

    def __post_init__(self) -> None:
        url = urlsplit(self.endpoint)
        if (
            url.scheme != "redis"
            or not url.hostname
            or url.username is not None
            or url.path not in ("", "/")
            or url.query
            or url.fragment
        ):
            raise ValueError("Use a Redis health URL in the form redis://HOST[:PORT].")
        if url.port == 0:
            raise ValueError("Redis health URL port must be positive.")
        if not isfinite(self.timeout_seconds) or self.timeout_seconds <= 0:
            raise ValueError("Timeout must be a finite positive number.")
        object.__setattr__(self, "_url", url)

    def inspect_health(self) -> HealthRead:
        try:
            with socket.create_connection(
                (self._url.hostname, self._url.port or 6379), self.timeout_seconds
            ) as connection:
                connection.sendall(_PING)
                with connection.makefile("rb") as response:
                    reply = response.readline(_RESPONSE_LIMIT)
        except TimeoutError:
            return self._unavailable("request_timeout")
        except OSError:
            return self._unavailable("transport_error")
        except OverflowError:
            return self._unavailable("invalid_timeout")

        if reply == b"+PONG\r\n":
            return {
                "source": self.endpoint,
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "outcome": "observed",
                "health": "healthy",
            }
        if reply.startswith(b"-") and reply.endswith(b"\r\n"):
            prefix = reply[:-2].partition(b" ")[0]
            return self._unavailable(_REDIS_ERROR_REASONS.get(prefix, "redis_error"))
        return self._unavailable("invalid_redis_response")

    def _unavailable(self, reason: str) -> HealthRead:
        return {
            "source": self.endpoint,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "outcome": "timeout" if reason == "request_timeout" else "missing",
            "health": "unknown",
            "reason": reason,
        }
