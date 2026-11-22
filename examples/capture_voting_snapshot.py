"""Export read-only evidence from an isolated Docker Example Voting App project."""

import argparse
import json
import subprocess
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import TypedDict

from opspilot.adapters.voting_snapshot import parse_voter_id
from opspilot.core.diagnosis import ObservationOutcome

_QUEUE_LIMIT = 1000
_READ_TIMEOUT = 15


class _ObservationFields(TypedDict):
    observed_at: str
    outcome: ObservationOutcome


class _Observation(_ObservationFields, total=False):
    row_count: int


def _read_output(command: list[str], *, sql: str = "") -> str:
    return subprocess.run(
        command, input=sql, capture_output=True, encoding="utf-8", check=True, timeout=_READ_TIMEOUT,
    ).stdout


def _redis_count(compose: list[str], voter_id: str) -> int:
    output = _read_output([
        *compose, "exec", "-T", "redis", "redis-cli", "--json", "LRANGE", "votes", "0", str(_QUEUE_LIMIT),
    ])
    entries = json.loads(output)
    # LRANGE includes the last index, so a full response can reveal an incomplete sample.
    if not isinstance(entries, list) or len(entries) > _QUEUE_LIMIT:
        raise ValueError("The Redis response is unavailable or exceeds the sample limit.")

    count = 0
    for raw in entries:
        if not isinstance(raw, str):
            raise ValueError("Expected a JSON vote message.")
        vote = json.loads(raw)
        if not isinstance(vote, dict) or not isinstance(vote.get("voter_id"), str):
            raise ValueError("Expected a vote with a voter ID.")
        if vote["voter_id"] == voter_id:
            count += 1
    return count


def _postgres_count(compose: list[str], voter_id: str) -> int:
    output = _read_output([
        *compose, "exec", "-T", "-e", "PGOPTIONS=-c default_transaction_read_only=on", "db",
        "psql", "--no-psqlrc", "--no-password", "--tuples-only", "--no-align", "--quiet",
        "--username=postgres", "--dbname=postgres", "--set=ON_ERROR_STOP=1", f"--set=voter_id={voter_id}",
    ], sql="SELECT count(*) FROM public.votes WHERE id = :'voter_id';\n")
    count = int(output.strip())
    if count < 0:
        raise ValueError("Expected a nonnegative matching row count.")
    return count


def _observe(read: Callable[[list[str], str], int], compose: list[str], voter_id: str) -> _Observation:
    evidence: _Observation = {"observed_at": datetime.now(timezone.utc).isoformat(), "outcome": "missing"}
    try:
        evidence["row_count"] = read(compose, voter_id)
    except subprocess.TimeoutExpired:
        evidence["outcome"] = "timeout"
    except (OSError, ValueError, RecursionError, subprocess.CalledProcessError):
        pass
    else:
        evidence["outcome"] = "observed"
    return evidence


def capture_snapshot(compose: list[str], environment: str, voter_id: str) -> dict[str, object]:
    queued = _observe(_redis_count, compose, voter_id)
    stored = _observe(_postgres_count, compose, voter_id)
    return {
        "environment": environment, "voter_id": voter_id, "captured_at": datetime.now(timezone.utc).isoformat(),
        "queued_vote": queued, "stored_vote": stored,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-name", required=True, help="Name of the running Compose sample project.")
    parser.add_argument("--compose-file", type=Path, action="append", required=True, help="Compose file. Repeatable.")
    parser.add_argument("--environment", required=True, help="Environment label to record in the snapshot.")
    parser.add_argument("--voter-id", required=True, help="Exact voter ID from the sample app.")
    args = parser.parse_args()
    if not args.project_name.strip():
        parser.error("--project-name must not be empty")
    if not args.environment.strip():
        parser.error("--environment must not be empty")
    if parse_voter_id(args.voter_id) is None:
        parser.error("--voter-id must contain 1 to 255 hexadecimal digits")
    compose = ["docker", "compose", "--project-name", args.project_name]
    for path in args.compose_file:
        compose.extend(["--file", str(path)])
    print(json.dumps(capture_snapshot(compose, args.environment, args.voter_id), indent=2))


if __name__ == "__main__":
    main()
