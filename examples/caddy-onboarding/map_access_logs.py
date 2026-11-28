"""Convert a native Caddy access log snapshot to OpsPilot JSONL."""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path


def map_access_logs(source: Path, destination: Path, service: str) -> None:
    if source.resolve() == destination.resolve():
        raise ValueError("Source and destination must be different paths.")
    if not service.strip():
        raise ValueError("Service must not be empty.")
    rows = []
    with source.open(encoding="utf-8-sig") as file:
        for line_number, line in enumerate(file, 1):
            if not line.strip():
                continue
            try:
                entry = json.loads(line)
                if not isinstance(entry, dict):
                    raise ValueError("Expected a JSON object.")
                logger = entry.get("logger", "")
                if not isinstance(logger, str) or not logger.startswith("http.log.access"):
                    continue
                stamp, status = entry["ts"], entry["status"]
                if type(stamp) not in (int, float) or type(status) is not int or not 0 <= status <= 599:
                    raise ValueError("Expected a timestamp and HTTP status.")
                mapped = {
                    "timestamp": datetime.fromtimestamp(stamp, timezone.utc).isoformat(), "service": service,
                    "level": "ERROR" if status >= 500 else "INFO", "http_status": status,
                }
                if "traceID" in entry:
                    mapped["trace_id"] = entry["traceID"]
                rows.append(json.dumps(mapped) + "\n")
            except (KeyError, OSError, OverflowError, ValueError) as error:
                raise ValueError(f"Invalid Caddy log at line {line_number}.") from error
    destination.write_text("".join(rows), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    parser.add_argument("--service", required=True)
    args = parser.parse_args()
    try:
        map_access_logs(args.source, args.destination, args.service)
    except (OSError, ValueError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
