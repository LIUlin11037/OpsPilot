import json
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from examples import capture_voting_snapshot
from opspilot.adapters.voting_snapshot import VotingSnapshotAdapter

COMPOSE = ["docker", "compose", "--project-name", "example"]
VOTER_ID = "001abc"


def test_capture_uses_read_only_commands_and_round_trips_the_snapshot(monkeypatch) -> None:
    def run(command, *, input, **options):
        assert command[:len(COMPOSE)] == COMPOSE
        assert options["check"] and options["timeout"] > 0
        if "redis-cli" in command:
            assert command[-5:-1] == ["--json", "LRANGE", "votes", "0"]
            return subprocess.CompletedProcess(command, 0, json.dumps([
                json.dumps({"voter_id": VOTER_ID, "vote": "raw-vote"}),
                json.dumps({"voter_id": "001ABC", "vote": "raw-vote"}),
                json.dumps({"voter_id": "1abc", "vote": "raw-vote"}),
            ]))
        assert "PGOPTIONS=-c default_transaction_read_only=on" in command
        assert f"--set=voter_id={VOTER_ID}" in command
        assert input == "SELECT count(*) FROM public.votes WHERE id = :'voter_id';\n"
        return subprocess.CompletedProcess(command, 0, "0\n")

    monkeypatch.setattr(capture_voting_snapshot.subprocess, "run", run)
    snapshot = capture_voting_snapshot.capture_snapshot(COMPOSE, "voting-test", VOTER_ID)
    payload = json.dumps(snapshot)
    assert "raw-vote" not in payload

    with TemporaryDirectory(prefix="voting-capture-") as directory:
        path = Path(directory) / "snapshot.json"
        path.write_text(payload, encoding="utf-8")
        queued, stored = VotingSnapshotAdapter(path).inspect_trace(VOTER_ID)["checks"]
    assert (queued["outcome"], queued["row_count"]) == ("observed", 1)
    assert (stored["outcome"], stored["row_count"]) == ("observed", 0)


@pytest.mark.parametrize(("mode", "outcome", "reason"), [
    ("timeout", "timeout", "request_timeout"),
    ("over_limit", "missing", "queue_sample_limit_exceeded"),
    ("invalid_output", "missing", "invalid_source_response"),
    ("query_failed", "missing", "source_query_failed"),
    ("collector_failed", "missing", "collector_unavailable"),
])
def test_capture_preserves_redis_failure_reason_and_continues_to_postgres(monkeypatch, mode, outcome, reason) -> None:
    def run(command, *, timeout, **_options):
        if "redis-cli" not in command:
            return subprocess.CompletedProcess(command, 0, "1\n")
        if mode == "timeout":
            raise subprocess.TimeoutExpired(command, timeout)
        if mode == "query_failed":
            raise subprocess.CalledProcessError(1, command, stderr="raw-endpoint-error")
        if mode == "collector_failed":
            raise FileNotFoundError("raw-endpoint-error")
        output = json.dumps(["{}"] * 1001) if mode == "over_limit" else "raw-endpoint-error"
        return subprocess.CompletedProcess(command, 0, output)

    monkeypatch.setattr(capture_voting_snapshot.subprocess, "run", run)
    snapshot = capture_voting_snapshot.capture_snapshot(COMPOSE, "voting-test", VOTER_ID)
    assert snapshot["queued_vote"]["outcome"] == outcome
    assert snapshot["queued_vote"]["reason"] == reason
    assert "row_count" not in snapshot["queued_vote"]
    assert snapshot["stored_vote"]["outcome"] == "observed"
    assert snapshot["stored_vote"]["row_count"] == 1
    assert "raw-endpoint-error" not in json.dumps(snapshot)
