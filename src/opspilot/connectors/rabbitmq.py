"""Read queue counts from the RabbitMQ Management HTTP API."""

import json
from base64 import b64encode
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.client import HTTPConnection, HTTPException, HTTPSConnection
from math import isfinite
from typing import Literal
from urllib.parse import quote, urlsplit, urlunsplit

from opspilot.core.diagnosis import QueueEvidence, UnavailableQueueEvidence

FailureReason = Literal[
    "authentication_failed",
    "access_denied",
    "queue_not_found",
    "http_error",
    "request_timeout",
    "transport_error",
    "invalid_response",
    "metric_missing",
]

_HTTP_FAILURES: dict[int, FailureReason] = {
    401: "authentication_failed",
    403: "access_denied",
    404: "queue_not_found",
}


class RabbitMQUnavailableEvidence(UnavailableQueueEvidence):
    reason: FailureReason
    http_status: int | None


def _path_segment(name: str) -> str:
    if name in (".", ".."):
        return name.replace(".", "%2E")
    return quote(name, safe="")


def _nonnegative_count(value: object) -> int | None:
    return value if type(value) is int and value >= 0 else None


@dataclass(frozen=True)
class RabbitMQQueueConnector:
    management_url: str
    queue: str
    username: str
    password: str = field(repr=False)
    vhost: str = "/"
    timeout_seconds: float = 5.0

    def inspect_queue(self) -> QueueEvidence | RabbitMQUnavailableEvidence:
        if not self.queue or not self.vhost:
            raise ValueError("Queue and virtual host names must be nonempty.")
        if not self.username or ":" in self.username:
            raise ValueError("RabbitMQ username must be nonempty and contain no colon.")
        if not isfinite(self.timeout_seconds) or self.timeout_seconds <= 0:
            raise ValueError("Timeout must be a finite positive number.")

        base = urlsplit(self.management_url)
        if (
            base.scheme not in ("http", "https")
            or not base.hostname
            or base.username is not None
            or base.query
            or base.fragment
        ):
            raise ValueError(
                "Use an HTTP(S) management URL without credentials, a query, or a fragment."
            )

        vhost = _path_segment(self.vhost)
        queue = _path_segment(self.queue)
        path = f"{base.path.rstrip('/')}/api/queues/{vhost}/{queue}"
        source = urlunsplit((base.scheme, base.netloc, path, "", ""))

        def unavailable(
            reason: FailureReason,
            status: int | None = None,
            *,
            registered_consumers: int | None = None,
            unacknowledged_messages: int | None = None,
        ) -> RabbitMQUnavailableEvidence:
            return {
                "tool": "inspect_queue",
                "source": source,
                "observed_at": datetime.now(timezone.utc).isoformat(),
                "outcome": "timeout" if reason == "request_timeout" else "missing",
                "reason": reason,
                "http_status": status,
                "registered_consumers": registered_consumers,
                "unacknowledged_messages": unacknowledged_messages,
            }

        credentials = b64encode(f"{self.username}:{self.password}".encode()).decode("ascii")
        connection_type = HTTPSConnection if base.scheme == "https" else HTTPConnection
        connection = connection_type(base.hostname, base.port, timeout=self.timeout_seconds)
        try:
            with closing(connection):
                connection.request(
                    "GET",
                    path,
                    headers={
                        "Accept": "application/json",
                        "Authorization": f"Basic {credentials}",
                    },
                )
                with connection.getresponse() as response:
                    if response.status != 200:
                        reason = _HTTP_FAILURES.get(response.status, "http_error")
                        return unavailable(reason, response.status)
                    body = response.read()
        except TimeoutError:
            return unavailable("request_timeout")
        except (OSError, HTTPException):
            return unavailable("transport_error")

        try:
            data = json.loads(body)
        except ValueError:
            return unavailable("invalid_response", 200)

        if not isinstance(data, dict):
            return unavailable("invalid_response", 200)

        registered_consumers = _nonnegative_count(data.get("consumers"))
        unacknowledged_messages = _nonnegative_count(
            data.get("messages_unacknowledged")
        )
        raw_pending = data.get("messages_ready")
        pending_messages = _nonnegative_count(raw_pending)
        if pending_messages is None:
            return unavailable(
                "metric_missing" if raw_pending is None else "invalid_response",
                200,
                registered_consumers=registered_consumers,
                unacknowledged_messages=unacknowledged_messages,
            )

        return {
            "tool": "inspect_queue",
            "source": source,
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "outcome": "observed",
            "pending_messages": pending_messages,
            "registered_consumers": registered_consumers,
            "unacknowledged_messages": unacknowledged_messages,
            "sampled_pending_message_ids": [],
        }
