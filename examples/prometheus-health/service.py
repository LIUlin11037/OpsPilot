"""Serve deterministic HTTP and metrics observations for the isolated example."""

from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Lock
from time import monotonic, sleep

FAULT_FILE = Path("/tmp/opspilot-fault")
BUCKETS = (0.01, 0.05, 0.1, 0.25, 0.5, 1)
COUNTS = Counter({200: 0, 503: 0})
LATENCY_BUCKETS = Counter()
LATENCY_SUM = 0.0
LOCK = Lock()


def record(status: int, latency: float) -> None:
    global LATENCY_SUM
    with LOCK:
        COUNTS[status] += 1
        LATENCY_SUM += latency
        for bucket in BUCKETS:
            if latency <= bucket:
                LATENCY_BUCKETS[bucket] += 1


def metrics() -> bytes:
    with LOCK:
        requests = sum(COUNTS.values())
        lines = [
            "# TYPE http_requests_total counter",
            *(f'http_requests_total{{status="{status}"}} {n}' for status, n in COUNTS.items()),
            "# TYPE http_request_duration_seconds histogram",
            *(f'http_request_duration_seconds_bucket{{le="{b:g}"}} {LATENCY_BUCKETS[b]}' for b in BUCKETS),
            f'http_request_duration_seconds_bucket{{le="+Inf"}} {requests}',
            f"http_request_duration_seconds_sum {LATENCY_SUM}",
            f"http_request_duration_seconds_count {requests}",
        ]
    return ("\n".join(lines) + "\n").encode("ascii")


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        status = 200
        body = b""
        if self.path == "/ready":
            status = 503 if FAULT_FILE.exists() else 200
        elif self.path == "/work":
            started = monotonic()
            status = 503 if FAULT_FILE.exists() else 200
            sleep(0.32 if status == 503 else 0.02)
            record(status, monotonic() - started)
        elif self.path == "/metrics":
            body = metrics()
        else:
            status = 404
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; version=0.0.4")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
