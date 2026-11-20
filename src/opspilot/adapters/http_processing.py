"""Read target processing records over HTTP."""

import json
from contextlib import closing
from dataclasses import dataclass, field
from http.client import HTTPConnection, HTTPException, HTTPSConnection
from math import isfinite
from urllib.parse import SplitResult, urlencode, urlsplit, urlunsplit

from opspilot.adapters.processing_records import (
    ProcessingFailure,
    ProcessingRecordEvidence,
    UnavailableProcessingEvidence,
    select_processing_evidence,
    unavailable_processing,
)
from opspilot.core.diagnosis import EmptyProcessingEvidence

_HTTP_FAILURES: dict[int, ProcessingFailure] = {
    401: "authentication_failed",
    403: "access_denied",
}


@dataclass(frozen=True)
class HttpProcessingAdapter:
    endpoint: str
    queue: str
    vhost: str
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
            raise ValueError(
                "Use an HTTP(S) processing URL without credentials, a query, or a fragment."
            )
        if url.port == 0:
            raise ValueError("Processing URL port must be positive.")
        if not isfinite(self.timeout_seconds) or self.timeout_seconds <= 0:
            raise ValueError("Timeout must be a finite positive number.")
        object.__setattr__(self, "_url", url)

    def inspect_processing(
        self, *, message_id: str | None = None
    ) -> ProcessingRecordEvidence | EmptyProcessingEvidence | UnavailableProcessingEvidence:
        parameters = {"queue": self.queue, "vhost": self.vhost}
        if message_id is not None:
            parameters["message_id"] = message_id
        query = urlencode(parameters)
        url = self._url
        path = url.path or "/"
        source = urlunsplit((url.scheme, url.netloc, path, query, ""))
        connection_type = HTTPSConnection if url.scheme == "https" else HTTPConnection
        connection = connection_type(url.hostname, url.port, timeout=self.timeout_seconds)

        try:
            with closing(connection):
                connection.request("GET", f"{path}?{query}", headers={"Accept": "application/json"})
                with connection.getresponse() as response:
                    status = response.status
                    if status == 404:
                        reason = "record_not_found" if message_id is None else "message_not_found"
                        return unavailable_processing(source, reason, http_status=status)
                    if status != 200:
                        reason = _HTTP_FAILURES.get(status, "http_error")
                        return unavailable_processing(source, reason, http_status=status)
                    body = response.read()
        except TimeoutError:
            return unavailable_processing(source, "request_timeout")
        except (OSError, HTTPException):
            return unavailable_processing(source, "transport_error")

        try:
            payload = json.loads(body)
        except ValueError:
            return unavailable_processing(source, "invalid_record", http_status=status)

        evidence = select_processing_evidence(
            payload, source=source, queue=self.queue, vhost=self.vhost, message_id=message_id
        )
        if evidence["outcome"] != "observed" or evidence.get("row_count") == 0:
            evidence["http_status"] = status
        return evidence
