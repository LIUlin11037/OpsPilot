"""Send bounded GET traffic to the two routes of the isolated Traefik example."""

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from time import monotonic, sleep
from urllib.error import HTTPError
from urllib.request import urlopen

BASE = "http://127.0.0.1:18771"


def status(path: str) -> int:
    try:
        with urlopen(f"{BASE}/{path}/", timeout=3) as response:
            return response.status
    except HTTPError as error:
        with error:
            return error.code


def whole_seconds() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seconds", type=int, default=90)
    seconds = parser.parse_args().seconds
    if not 1 <= seconds <= 300:
        parser.error("--seconds must be between 1 and 300")
    start = whole_seconds()
    deadline = monotonic() + seconds
    counts = {"orders": Counter(), "neighbor": Counter()}
    while monotonic() < deadline:
        for route, codes in counts.items():
            codes[status(route)] += 1
        sleep(0.25)
    print(json.dumps({"start": start, "end": whole_seconds(), "responses": counts}, indent=2))


if __name__ == "__main__":
    main()
