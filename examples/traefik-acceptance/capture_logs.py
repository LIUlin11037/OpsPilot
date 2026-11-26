"""Convert the isolated Traefik access JSONL on stdin into scoped service logs."""

import json
import re
import sys

_UTC = re.compile(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?Z")


def main() -> None:
    for line in sys.stdin:
        if not line.strip():
            continue
        record = json.loads(line)
        service = record.get("ServiceName")
        if service not in ("orders@file", "neighbor@file"):
            continue
        status = record.get("DownstreamStatus")
        timestamp = _UTC.fullmatch(record.get("StartUTC") or "")
        if type(status) is not int or timestamp is None:
            raise ValueError("The isolated Traefik access log is missing its status or UTC timestamp.")
        seconds, fraction = timestamp.groups()
        micros = (fraction or "")[:6].ljust(6, "0")
        print(json.dumps({
            "timestamp": f"{seconds}.{micros}+00:00",
            "service": service.removesuffix("@file"),
            "level": "ERROR" if 500 <= status <= 599 else "INFO",
            "http_status": status,
        }))


if __name__ == "__main__":
    main()
