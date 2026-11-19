"""Opt-in acceptance for a prepared HireHub notification case."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

_PREFIX = "OPSPILOT_TEST_HIREHUB_"
_NOTIFICATION_STATES = {
    "not_observed": ("observed", 0, 1, 0),
    "unknown": ("missing", None, 1, 0),
    "observed": ("observed", 1, 0, 1),
}


@pytest.fixture
def hirehub_case(pytestconfig: pytest.Config) -> tuple[list[str], str, str]:
    if not pytestconfig.getoption("--hirehub-live"):
        pytest.skip("Pass --hirehub-live to use an isolated HireHub test delivery.")

    names = (
        f"{_PREFIX}MANAGEMENT_URL", f"{_PREFIX}DELIVERY_ID", f"{_PREFIX}RECIPIENT_ID",
        f"{_PREFIX}MESSAGE_ID", "OPSPILOT_MYSQL_USER", "OPSPILOT_MYSQL_PASSWORD",
        "OPSPILOT_RABBITMQ_USER", "OPSPILOT_RABBITMQ_PASSWORD",
    )
    missing = [name for name in names if not os.environ.get(name)]
    if missing:
        pytest.fail(f"Set {', '.join(missing)} for HireHub acceptance.")

    state = os.getenv(f"{_PREFIX}NOTIFICATION", "not_observed")
    if state not in _NOTIFICATION_STATES:
        pytest.fail(f"Set {_PREFIX}NOTIFICATION to not_observed, unknown, or observed.")
    arguments = [
        "--environment", "hirehub-acceptance",
        "--management-url", os.environ[f"{_PREFIX}MANAGEMENT_URL"],
        "--delivery-id", os.environ[f"{_PREFIX}DELIVERY_ID"],
        "--recipient-id", os.environ[f"{_PREFIX}RECIPIENT_ID"],
        "--mysql-host", os.getenv(f"{_PREFIX}MYSQL_HOST", "127.0.0.1"),
        "--mysql-port", os.getenv(f"{_PREFIX}MYSQL_PORT", "3306"),
        "--vhost", os.getenv(f"{_PREFIX}VHOST", "/"),
    ]
    return arguments, os.environ[f"{_PREFIX}MESSAGE_ID"], state


def _run_cli(directory: Path, *arguments: str) -> dict:
    result = subprocess.run(
        [sys.executable, "-m", "opspilot", *arguments], cwd=directory,
        capture_output=True, text=True, encoding="utf-8", timeout=35,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def test_hirehub_live_diagnosis_and_report(
    hirehub_case: tuple[list[str], str, str], tmp_path: Path
) -> None:
    arguments, message_id, state = hirehub_case
    report = _run_cli(
        tmp_path, "diagnose-hirehub", *arguments,
        "Delivery succeeded, but HR did not receive a notification.",
    )

    trace = report["target_trace"]
    assert trace["message_id"] == message_id
    assert trace["branches"] == {"job": "observed", "notification": state}
    assert report["status"] == "undetermined"
    assert report["processing_correlation"] == "unknown"
    checks = {evidence["checkpoint"]: evidence for evidence in report["evidence"]}
    assert checks["delivery"]["status"] == "PENDING"
    assert checks["outbox"]["status"] == "CONFIRMED"
    assert checks["outbox"]["source"] == "MySQL/hirehub_delivery.local_message"
    assert checks["outbox"]["expected_recipient_listed"] is True
    assert checks["job"]["row_count"] == 1
    outcome, rows, pending, consumers = _NOTIFICATION_STATES[state]
    assert checks["notification"]["outcome"] == outcome
    assert ("row_count" in checks["notification"]) == (rows is not None)
    assert checks["notification"].get("row_count") == rows
    if state == "unknown":
        assert checks["notification"]["reason"] == "access_denied"
        assert checks["notification"]["mysql_error_code"] in (1044, 1142, 1143)
    assert checks["queue"]["outcome"] == "observed"
    assert checks["queue"]["pending_messages"] == pending
    assert checks["queue"]["unacknowledged_messages"] == 0
    assert checks["queue"]["registered_consumers"] == consumers
    assert checks["queue"]["source"].startswith(("http://", "https://"))

    assert _run_cli(tmp_path, "report", report["task_id"]) == report
    snapshot = _run_cli(tmp_path, "capture-hirehub", *arguments)
    assert snapshot["delivery"]["rows"] == [{
        "delivery_id": trace["entity_id"], "status": checks["delivery"]["status"],
    }]
