"""Read the HTTP status of a configured health endpoint."""

from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.client import HTTPConnection, HTTPException, HTTPSConnection
from math import isfinite
from typing import Literal
from urllib.parse import SplitResult, quote, urlsplit

from opspilot.core.diagnosis import HealthRead

_STATUS_REASONS = {401: "authentication_failed", 403: "access_denied"}


@dataclass(frozen=True)
class HttpHealthConnector:
    endpoint: str
    timeout_seconds: float = 5.0
    _url: SplitResult = field(init=False, repr=False)

    def __post_init__(self) -> None:
        url = urlsplit(self.endpoint)
        if (
            url.scheme not in ("http", "https")
            or not url.hostname
            or url.username is not None
            or url.query
            or url.fragment
        ):
            raise ValueError("Use an HTTP(S) health URL without credentials, a query, or a fragment.")
        if url.port == 0:
            raise ValueError("Health URL port must be positive.")
        if not isfinite(self.timeout_seconds) or self.timeout_seconds <= 0:
            raise ValueError("Timeout must be a finite positive number.")
        path = quote(url.path or "/", safe="/%:@!$&'()*+,;=")
        object.__setattr__(self, "_url", url._replace(path=path))

    def inspect_health(self) -> HealthRead:
        url = self._url
        connection_type = HTTPSConnection if url.scheme == "https" else HTTPConnection
        connection = connection_type(url.hostname, url.port, timeout=self.timeout_seconds)
        try:
            with closing(connection):
                connection.request("GET", url.path)
                with connection.getresponse() as response:
                    status = response.status
        except TimeoutError:
            return self._unavailable("request_timeout")
        except (OSError, HTTPException):
            return self._unavailable("transport_error")
        except OverflowError:
            return self._unavailable("invalid_timeout")

        if 200 <= status < 300:
            return self._observed(status, "healthy")
        if status == 424 or 500 <= status < 600:
            return self._observed(status, "unhealthy")
        evidence = self._unavailable(_STATUS_REASONS.get(status, "unexpected_health_status"))
        evidence["http_status"] = status
        return evidence

    def _observed(self, status: int, health: Literal["healthy", "unhealthy"]) -> HealthRead:
        return {
            "source": self.endpoint,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "outcome": "observed",
            "health": health,
            "http_status": status,
        }

    def _unavailable(self, reason: str) -> HealthRead:
        return {
            "source": self.endpoint,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "outcome": "timeout" if reason == "request_timeout" else "missing",
            "health": "unknown",
            "reason": reason,
        }
