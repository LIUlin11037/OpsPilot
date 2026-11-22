"""Opt-in acceptance against an isolated Docker Example Voting App project."""

import json
import re
import subprocess
import sys
from collections.abc import Iterator
from contextlib import closing
from http.client import HTTPConnection
from http.cookies import SimpleCookie
from pathlib import Path
from tempfile import TemporaryDirectory
from time import monotonic, sleep
from uuid import uuid4

import pytest

from examples.capture_voting_snapshot import capture_snapshot
from opspilot.adapters.voting_snapshot import parse_voter_id

_ENVIRONMENT = "docker-voting-acceptance"


def _run(command: list[str], *, timeout: int = 25) -> str:
    result = subprocess.run(command, capture_output=True, encoding="utf-8", timeout=timeout)
    assert result.returncode == 0, f"{command[0]} exited with {result.returncode}\n{result.stderr[-1500:]}"
    return result.stdout


@pytest.fixture
def voting_project(pytestconfig: pytest.Config) -> Iterator[tuple[list[str], Path, int]]:
    compose_file = pytestconfig.getoption("--voting-live")
    if compose_file is None:
        pytest.skip("Pass --voting-live with the upstream Compose file to run the voting acceptance.")
    assert compose_file.is_file(), "Supply the upstream Example Voting App docker-compose.images.yml."

    project = f"opspilot-voting-{uuid4().hex[:12]}"
    with TemporaryDirectory(prefix="opspilot-voting-live-") as output:
        directory = Path(output)
        overlay = directory / "loopback.yaml"
        overlay.write_text('services:\n  vote:\n    ports: !override\n      - "127.0.0.1::80"\n', encoding="utf-8")
        compose = ["docker", "compose", "--project-name", project, "-f", str(compose_file.resolve()), "-f", str(overlay)]
        config = json.loads(_run([*compose, "config", "--format", "json"]))
        services = config["services"]
        vote_port, = services["vote"]["ports"]
        assert vote_port["host_ip"] == "127.0.0.1" and vote_port["target"] == 80
        for name in ("redis", "db", "worker"):
            assert not services[name].get("ports")
        assert all(not service.get("container_name") for service in services.values())
        for resources in (config.get("networks", {}), config.get("volumes", {})):
            assert all(item["name"].startswith(f"{project}_") and not item.get("external") for item in resources.values())

        try:
            _run([*compose, "up", "-d", "--wait", "--wait-timeout", "90", "vote", "worker"], timeout=300)
            address = _run([*compose, "port", "vote", "80"]).strip()
            match = re.fullmatch(r"127\.0\.0\.1:(\d+)", address)
            assert match is not None, f"Expected a loopback vote endpoint, got {address!r}"
            yield compose, directory, int(match[1])
        finally:
            _run([*compose, "down", "--volumes"], timeout=75)


def _get_voter_id(port: int) -> str:
    deadline = monotonic() + 15
    while monotonic() < deadline:
        try:
            with closing(HTTPConnection("127.0.0.1", port, timeout=2)) as connection:
                connection.request("GET", "/")
                with connection.getresponse() as response:
                    cookie = SimpleCookie(response.getheader("Set-Cookie") or "")
                    if response.status == 200 and "voter_id" in cookie:
                        voter_id = cookie["voter_id"].value
                        assert parse_voter_id(voter_id) is not None
                        return voter_id
        except OSError:
            pass
        sleep(0.5)
    pytest.fail("The voting endpoint did not issue a voter ID.")


def _wait_for_counts(compose: list[str], voter_id: str, queued: int, stored: int) -> dict:
    deadline = monotonic() + 25
    while True:
        snapshot = capture_snapshot(compose, _ENVIRONMENT, voter_id)
        counts = (snapshot["queued_vote"].get("row_count"), snapshot["stored_vote"].get("row_count"))
        if counts == (queued, stored):
            return snapshot
        if monotonic() >= deadline:
            pytest.fail(f"Expected Redis/PostgreSQL counts {(queued, stored)}, got {counts}.")
        sleep(0.5)


def _cli(directory: Path, *arguments: str) -> dict:
    return json.loads(_run([sys.executable, "-m", "opspilot", "--db", str(directory / "reports.sqlite3"), *arguments]))


def _save_report(directory: Path, phase: str, snapshot: dict, voter_id: str) -> dict:
    snapshot_path = directory / f"{phase}.json"
    snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
    config = directory / f"{phase}-target.json"
    config.write_text(json.dumps({"voting": {"snapshot": snapshot_path.name}}), encoding="utf-8")
    report = _cli(directory, "diagnose-auto", "--config", str(config), "--voter-id", voter_id, "Was the vote stored?")
    assert report["routing"]["selected_by"] == "voter_id"
    assert str(report["target_trace"]["entity_id"]) == voter_id
    return report


def test_voting_live_reports_queue_storage_and_unavailable_redis(voting_project: tuple[list[str], Path, int]) -> None:
    compose, directory, port = voting_project
    voter_id = _get_voter_id(port)
    _wait_for_counts(compose, voter_id, 0, 0)
    _run([*compose, "stop", "--timeout", "3", "worker"])
    with closing(HTTPConnection("127.0.0.1", port, timeout=10)) as connection:
        connection.request("POST", "/", "vote=a", {
            "Cookie": f"voter_id={voter_id}", "Content-Type": "application/x-www-form-urlencoded",
        })
        with connection.getresponse() as response:
            assert response.status == 200
    pending = _save_report(directory, "pending", _wait_for_counts(compose, voter_id, 1, 0), voter_id)
    assert pending["target_trace"]["branches"] == {"queued_vote": "observed", "stored_vote": "not_observed"}

    _run([*compose, "start", "--wait", "--wait-timeout", "45", "worker"], timeout=60)
    stored = _save_report(directory, "stored", _wait_for_counts(compose, voter_id, 0, 1), voter_id)
    assert stored["target_trace"]["branches"] == {"queued_vote": "not_observed", "stored_vote": "observed"}
    transition = _cli(directory, "compare-voting", pending["task_id"], stored["task_id"])
    assert {"field": "queued_vote.row_count", "before": 1, "after": 0} in transition["changes"]
    assert {"field": "stored_vote.row_count", "before": 0, "after": 1} in transition["changes"]

    _run([*compose, "stop", "--timeout", "3", "redis"])
    partial = _save_report(directory, "redis-unavailable", capture_snapshot(compose, _ENVIRONMENT, voter_id), voter_id)
    queued, stored_evidence = partial["evidence"]
    assert queued["outcome"] == "missing" and queued["reason"] == "source_query_failed"
    assert "row_count" not in queued
    assert stored_evidence["outcome"] == "observed" and stored_evidence["row_count"] == 1

    latest = _cli(directory, "compare-voting-latest", "--environment", _ENVIRONMENT, "--voter-id", voter_id)
    assert latest["before_task_id"] == stored["task_id"]
    assert latest["after_task_id"] == partial["task_id"]
    assert {"field": "queued_vote.row_count", "before": 0, "after": None} in latest["changes"]
    assert {"field": "queued_vote.reason", "before": None, "after": "source_query_failed"} in latest["changes"]
