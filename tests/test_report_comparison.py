import json
import subprocess
import sys
from collections.abc import Iterator
from copy import deepcopy
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from opspilot.adapters.hirehub_snapshot import HireHubSnapshotAdapter
from opspilot.connectors.rabbitmq import RabbitMQUnavailableEvidence
from opspilot.core.diagnosis import DiagnosisReport, HealthEvidence, QueueEvidence
from opspilot.core.target_trace import run_target_trace
from opspilot.report_comparison import compare_health_reports, compare_queue_reports, compare_trace_reports
from opspilot.report_markdown import render_health_comparison_markdown, render_queue_comparison_markdown
from opspilot.task_store import StoredReport, list_reports, save_report

EXAMPLE = Path(__file__).resolve().parents[1] / "examples/hirehub-notification-snapshot.json"
SERVICE_URL = "http://127.0.0.1:8080/health"
REDIS_URL = "redis://127.0.0.1:6379"
QUEUE_SOURCE = "http://127.0.0.1:15672/api/queues/%2F/orders"
CONSUMER_URL = "http://127.0.0.1:8081/health"


def _compare(database: Path, command: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "opspilot", "--db", str(database), command, *args],
        cwd=database.parent, capture_output=True, text=True, encoding="utf-8", timeout=20,
    )


@pytest.fixture
def saved_queue_pair() -> Iterator[tuple[Path, StoredReport, StoredReport]]:
    queue: QueueEvidence = {
        "tool": "inspect_queue", "source": QUEUE_SOURCE,
        "observed_at": "2026-11-20T06:00:00+00:00", "outcome": "observed",
        "pending_messages": 6, "unacknowledged_messages": 3, "registered_consumers": 1,
        "sampled_pending_message_ids": [],
    }
    partial_queue: RabbitMQUnavailableEvidence = {
        "tool": "inspect_queue", "source": QUEUE_SOURCE,
        "observed_at": "2026-11-20T06:01:00+00:00", "outcome": "missing",
        "reason": "metric_missing", "http_status": 200,
        "unacknowledged_messages": 0, "registered_consumers": 0,
    }
    consumer: HealthEvidence = {
        "tool": "inspect_consumer_endpoint", "source": CONSUMER_URL,
        "observed_at": "2026-11-20T06:00:01+00:00", "outcome": "observed",
        "health": "unhealthy", "http_status": 503,
    }
    before: DiagnosisReport = {
        "question": "Check the queue", "status": "undetermined",
        "conclusion": "Pending messages were observed.", "processing_correlation": None,
        "evidence": [queue, consumer],
    }
    after: DiagnosisReport = {
        **before, "conclusion": "Pending message count is unavailable.", "evidence": [partial_queue],
    }
    with TemporaryDirectory(prefix="opspilot-queue-comparison-") as directory:
        database = Path(directory) / "reports.sqlite3"
        yield database, save_report(database, before), save_report(database, after)


def test_compare_queue_preserves_partial_observations_and_zero_counts(saved_queue_pair) -> None:
    database, before, after = saved_queue_pair
    original = database.read_bytes()

    result = _compare(database, "compare-queue", before["task_id"], after["task_id"])

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "before_task_id": before["task_id"], "after_task_id": after["task_id"],
        "queue_source": QUEUE_SOURCE,
        "changes": [
            {"field": "queue.http_status", "before": None, "after": 200},
            {"field": "queue.outcome", "before": "observed", "after": "missing"},
            {"field": "queue.pending_messages", "before": 6, "after": None},
            {"field": "queue.reason", "before": None, "after": "metric_missing"},
            {"field": "queue.registered_consumers", "before": 1, "after": 0},
            {"field": "queue.unacknowledged_messages", "before": 3, "after": 0},
        ],
    }
    assert database.read_bytes() == original


def test_compare_queue_can_include_a_skipped_consumer_check(saved_queue_pair) -> None:
    database, before, after = saved_queue_pair
    original = database.read_bytes()

    result = _compare(
        database, "compare-queue", before["task_id"], after["task_id"], "--include-consumer-health",
    )

    assert result.returncode == 0, result.stderr
    comparison = json.loads(result.stdout)
    baseline = compare_queue_reports(before, after)
    assert comparison == {
        **baseline,
        "consumer_health_changes": [{
            "role": "consumer", "source": CONSUMER_URL, "occurrence": 1,
            "before": {"health": "unhealthy", "outcome": "observed", "http_status": 503, "reason": None},
            "after": None,
        }],
    }
    assert database.read_bytes() == original


@pytest.mark.parametrize("include_consumer_health", [False, True])
def test_compare_queue_renders_markdown(saved_queue_pair, include_consumer_health: bool) -> None:
    database, before, after = saved_queue_pair
    original = database.read_bytes()
    options = ["--format", "markdown"]
    if include_consumer_health:
        options.append("--include-consumer-health")

    result = _compare(database, "compare-queue", before["task_id"], after["task_id"], *options)

    assert result.returncode == 0, result.stderr
    assert f"**Queue source** {QUEUE_SOURCE}" in result.stdout
    assert f"**Baseline task ID** {before['task_id']}" in result.stdout
    assert f"**Follow-up task ID** {after['task_id']}" in result.stdout
    assert "| Pending messages | 6 | Unavailable |" in result.stdout
    assert "| Unacknowledged messages | 3 | 0 |" in result.stdout
    assert ("## Consumer endpoint changes" in result.stdout) == include_consumer_health
    if include_consumer_health:
        assert (
            f"| Consumer | {CONSUMER_URL} | 1 | Outcome observed, Health unhealthy, HTTP status 503 "
            "| No observation recorded |"
        ) in result.stdout
    assert database.read_bytes() == original


def test_queue_comparison_markdown_describes_unchanged_observations(saved_queue_pair) -> None:
    _, before, _ = saved_queue_pair
    comparison = compare_queue_reports(before, before, include_consumer_health=True)

    markdown = render_queue_comparison_markdown(comparison)

    assert "No changes in recorded queue observations." in markdown
    assert "No changes in recorded endpoint observations." in markdown


@pytest.mark.parametrize("source", [CONSUMER_URL, CONSUMER_URL + "/replacement"])
def test_compare_queue_matches_consumer_checks_by_source(saved_queue_pair, source: str) -> None:
    _, before, after = saved_queue_pair
    timed_out: HealthEvidence = {
        "tool": "inspect_consumer_endpoint", "source": source,
        "observed_at": "2026-11-20T06:01:01+00:00", "outcome": "timeout",
        "health": "unknown", "reason": "request_timeout",
    }
    after["evidence"].append(timed_out)

    comparison = compare_queue_reports(before, after, include_consumer_health=True)

    changes = comparison["consumer_health_changes"]
    if source == CONSUMER_URL:
        change, = changes
        assert change["before"]["health"] == "unhealthy"
    else:
        prior, change = changes
        assert prior["source"] == CONSUMER_URL and prior["after"] is None
        assert change["before"] is None
    assert change["source"] == source
    assert change["after"] == {
        "health": "unknown", "outcome": "timeout", "http_status": None, "reason": "request_timeout",
    }


def test_compare_queue_requires_the_same_queue_source(saved_queue_pair, saved_health_pair) -> None:
    _, before, after = saved_queue_pair
    after["evidence"][0]["source"] = QUEUE_SOURCE.replace("orders", "returns")
    with pytest.raises(ValueError, match="must match on the queue evidence source"):
        compare_queue_reports(before, after)

    _, health_report, _ = saved_health_pair
    with pytest.raises(ValueError, match="reports with one queue observation"):
        compare_queue_reports(before, health_report)


def test_compare_queue_latest_selects_two_matches_in_save_order(saved_queue_pair) -> None:
    database, first, second = saved_queue_pair
    latest = save_report(database, {**first, "question": "Latest queue check"})
    foreign = deepcopy(first)
    foreign["evidence"][0]["source"] = QUEUE_SOURCE.replace("%2F", "other-vhost")
    foreign["evidence"][1]["source"] = QUEUE_SOURCE
    save_report(database, foreign)
    original = database.read_bytes()

    summaries = list_reports(database, limit=2, queue_source=QUEUE_SOURCE)
    assert [(row["task_id"], row["queue_source"]) for row in summaries] == [
        (latest["task_id"], QUEUE_SOURCE), (second["task_id"], QUEUE_SOURCE),
    ]

    result = _compare(
        database, "compare-queue-latest", "--queue-source", QUEUE_SOURCE, "--include-consumer-health",
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == compare_queue_reports(
        second, latest, include_consumer_health=True,
    )
    assert database.read_bytes() == original


def test_compare_queue_latest_requires_two_reports_for_the_selected_queue(saved_queue_pair) -> None:
    database, first, _ = saved_queue_pair
    one_report = deepcopy(first)
    source = QUEUE_SOURCE + "-other"
    one_report["evidence"][0]["source"] = source
    save_report(database, one_report)

    result = _compare(database, "compare-queue-latest", "--queue-source", source)

    assert result.returncode == 1
    assert "fewer than two saved queue reports" in result.stderr
    assert result.stdout == ""


@pytest.fixture
def saved_health_pair() -> Iterator[tuple[Path, StoredReport, StoredReport]]:
    service: HealthEvidence = {
        "tool": "inspect_service_health", "source": SERVICE_URL,
        "observed_at": "2026-11-20T06:00:00+00:00", "outcome": "observed",
        "health": "unhealthy", "http_status": 503,
    }
    redis: HealthEvidence = {
        "tool": "inspect_dependency_health", "source": REDIS_URL,
        "observed_at": "2026-11-20T06:00:01+00:00", "outcome": "timeout",
        "health": "unknown", "reason": "request_timeout",
    }
    before: DiagnosisReport = {
        "question": "Check availability", "status": "undetermined",
        "conclusion": "Service unavailable", "processing_correlation": None,
        "evidence": [service, redis],
    }
    after: DiagnosisReport = {
        **before, "status": "no_failure_observed", "conclusion": "Service is healthy",
        "evidence": [{**service, "health": "healthy", "http_status": 204}],
    }
    with TemporaryDirectory(prefix="opspilot-health-comparison-") as directory:
        database = Path(directory) / "reports.sqlite3"
        yield database, save_report(database, before), save_report(database, after)


def test_compare_http_distinguishes_a_skipped_dependency_from_failed_health(saved_health_pair) -> None:
    database, before, after = saved_health_pair
    original = database.read_bytes()
    result = _compare(database, "compare-http", before["task_id"], after["task_id"])
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "before_task_id": before["task_id"], "after_task_id": after["task_id"],
        "service_url": SERVICE_URL,
        "changes": [
            {
                "role": "service", "source": SERVICE_URL, "occurrence": 1,
                "before": {"health": "unhealthy", "outcome": "observed", "http_status": 503, "reason": None},
                "after": {"health": "healthy", "outcome": "observed", "http_status": 204, "reason": None},
            },
            {
                "role": "dependency", "source": REDIS_URL, "occurrence": 1,
                "before": {"health": "unknown", "outcome": "timeout", "http_status": None, "reason": "request_timeout"},
                "after": None,
            },
        ],
    }
    assert database.read_bytes() == original


def test_compare_http_renders_markdown_with_missing_observations(saved_health_pair) -> None:
    database, before, after = saved_health_pair
    original = database.read_bytes()
    result = _compare(database, "compare-http", before["task_id"], "--format", "markdown", after["task_id"])

    assert result.returncode == 0, result.stderr
    assert f"**Baseline task ID** {before['task_id']}" in result.stdout
    assert f"**Follow-up task ID** {after['task_id']}" in result.stdout
    assert (
        f"| Service | {SERVICE_URL} | 1 | Outcome observed, Health unhealthy, HTTP status 503 | "
        "Outcome observed, Health healthy, HTTP status 204 |"
    ) in result.stdout
    assert (
        f"| Dependency | {REDIS_URL} | 1 | Outcome timeout, Health unknown, "
        r"Reason request\_timeout | No observation recorded |"
    ) in result.stdout
    assert database.read_bytes() == original


def test_compare_http_latest_selects_reports_for_the_main_service(saved_health_pair) -> None:
    database, before, after = saved_health_pair
    latest = save_report(database, deepcopy(before))
    other_report = deepcopy(before)
    other_report["evidence"][0]["source"] = f"{SERVICE_URL}/other"
    other_report["evidence"].append({**before["evidence"][0], "tool": "inspect_dependency_health"})
    save_report(database, other_report)
    original = database.read_bytes()

    result = _compare(database, "compare-http-latest", "--service-url", SERVICE_URL)

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == compare_health_reports(after, latest)
    assert database.read_bytes() == original


def test_compare_http_latest_requires_two_matching_reports(saved_health_pair) -> None:
    database, before, _ = saved_health_pair
    one_report = deepcopy(before)
    service_url = f"{SERVICE_URL}/other"
    one_report["evidence"][0]["source"] = service_url
    save_report(database, one_report)

    result = _compare(
        database, "compare-http-latest", "--service-url", service_url, "--format", "markdown",
    )

    assert result.returncode == 1
    assert "fewer than two saved health reports" in result.stderr
    assert result.stdout == ""


def test_compare_http_latest_rejects_blank_service_url() -> None:
    with TemporaryDirectory(prefix="opspilot-compare-latest-test-") as directory:
        database = Path(directory) / "reports.sqlite3"
        result = _compare(database, "compare-http-latest", "--service-url", " ")

        assert result.returncode == 2
        assert "--service-url must not be empty" in result.stderr
        assert not database.exists()


def test_health_comparison_markdown_describes_unchanged_observations(saved_health_pair) -> None:
    _, before, _ = saved_health_pair
    markdown = render_health_comparison_markdown(compare_health_reports(before, before))

    assert "No changes in recorded endpoint observations." in markdown
    assert "healthy" not in markdown


def test_compare_http_preserves_repeated_dependency_occurrences(saved_health_pair) -> None:
    _, before, _ = saved_health_pair
    before["evidence"].append(dict(before["evidence"][1]))
    after = deepcopy(before)
    after["evidence"][1]["observed_at"] = "2026-11-20T06:01:00+00:00"
    after["evidence"][2].update(outcome="observed", health="healthy")
    after["evidence"][2].pop("reason")

    change, = compare_health_reports(before, after)["changes"]
    assert change["source"] == REDIS_URL and change["occurrence"] == 2
    assert change["before"]["outcome"] == "timeout"
    assert change["after"]["health"] == "healthy"


def test_compare_http_rejects_different_service_or_report_kind(saved_health_pair) -> None:
    _, before, after = saved_health_pair
    after["evidence"][0]["source"] = "http://127.0.0.1:8081/health"
    with pytest.raises(ValueError, match="service endpoint URL"):
        compare_health_reports(before, after)

    after["evidence"] = []
    with pytest.raises(ValueError, match="must refer to HTTP health reports"):
        compare_health_reports(before, after)


def test_reports_filters_by_main_service_before_applying_limit(saved_health_pair) -> None:
    database, before, after = saved_health_pair
    other_service: HealthEvidence = {
        **before["evidence"][0], "source": "http://127.0.0.1:8081/health",
    }
    shared_dependency: HealthEvidence = {
        **before["evidence"][0], "tool": "inspect_dependency_health",
    }
    other_report: DiagnosisReport = {
        "question": "Check another service", "status": "undetermined",
        "conclusion": "Service unavailable", "processing_correlation": None,
        "evidence": [other_service, shared_dependency],
    }
    save_report(database, other_report)
    original = database.read_bytes()

    result = subprocess.run(
        [
            sys.executable, "-m", "opspilot", "--db", str(database), "reports",
            "--service-url", SERVICE_URL, "--limit", "2",
        ],
        cwd=database.parent, capture_output=True, text=True, encoding="utf-8", timeout=20,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == [
        {
            "task_id": report["task_id"], "question": report["question"],
            "status": report["status"], "service_url": SERVICE_URL,
        }
        for report in (after, before)
    ]
    assert list_reports(database, limit=2, service_url=SERVICE_URL + "/other") == []
    assert database.read_bytes() == original


@pytest.fixture
def saved_pair() -> Iterator[tuple[Path, StoredReport, StoredReport]]:
    with TemporaryDirectory(prefix="opspilot-comparison-test-") as directory:
        path = Path(directory)
        database = path / "reports.sqlite3"
        snapshot = json.loads(EXAMPLE.read_text(encoding="utf-8"))
        baseline = path / "baseline.json"
        baseline.write_text(json.dumps(snapshot), encoding="utf-8")
        before = run_target_trace("Check notification", 1001, HireHubSnapshotAdapter(baseline))
        snapshot["notification"]["rows"] = [{
            "message_id": snapshot["notification"]["message_id"],
            "consumer": "notification",
            "recorded_at": "2026-11-19T03:11:00Z",
        }]
        snapshot["queue"].update(pending_messages=0, registered_consumers=1)
        follow_up = path / "follow-up.json"
        follow_up.write_text(json.dumps(snapshot), encoding="utf-8")
        after = run_target_trace("Check notification again", 1001, HireHubSnapshotAdapter(follow_up))
        yield database, save_report(database, before), save_report(database, after)


def test_compare_hirehub_reports_returns_the_changed_observations(saved_pair) -> None:
    database, before, after = saved_pair
    original = database.read_bytes()
    result = subprocess.run(
        [
            sys.executable, "-m", "opspilot", "--db", str(database), "compare-hirehub",
            before["task_id"], after["task_id"],
        ],
        cwd=database.parent, capture_output=True, text=True, encoding="utf-8", timeout=20,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "before_task_id": before["task_id"],
        "after_task_id": after["task_id"],
        "target": {
            "environment": "hirehub-acceptance-sample",
            "vhost": "/",
            "entity_id": 1001,
            "message_id": "sample-delivery-event-1001",
        },
        "changes": [
            {"field": "branches.notification", "before": "not_observed", "after": "observed"},
            {"field": "queue.pending_messages", "before": 1, "after": 0},
            {"field": "queue.registered_consumers", "before": 0, "after": 1},
        ],
    }
    assert database.read_bytes() == original


def test_compare_keeps_unavailable_queue_distinct_from_observed_zero(saved_pair) -> None:
    _, _, after = saved_pair
    unavailable = deepcopy(after)
    queue = next(check for check in unavailable["evidence"] if check.get("checkpoint") == "queue")
    queue["outcome"] = "timeout"

    assert compare_trace_reports(after, unavailable)["changes"] == [
        {"field": "queue.outcome", "before": "observed", "after": "timeout"},
        {"field": "queue.pending_messages", "before": 0, "after": None},
        {"field": "queue.registered_consumers", "before": 1, "after": None},
        {"field": "queue.unacknowledged_messages", "before": 0, "after": None},
    ]
    assert compare_trace_reports(after, after)["changes"] == []


def test_compare_preserves_valid_counts_from_a_partial_queue_response(saved_pair) -> None:
    _, _, before = saved_pair
    partial = deepcopy(before)
    queue = next(check for check in partial["evidence"] if check.get("checkpoint") == "queue")
    queue.update(
        outcome="missing", reason="metric_missing", http_status=200,
        unacknowledged_messages=4, registered_consumers=0,
    )

    assert compare_trace_reports(before, partial)["changes"] == [
        {"field": "queue.outcome", "before": "observed", "after": "missing"},
        {"field": "queue.pending_messages", "before": 0, "after": None},
        {"field": "queue.registered_consumers", "before": 1, "after": 0},
        {"field": "queue.unacknowledged_messages", "before": 0, "after": 4},
    ]


@pytest.mark.parametrize(
    ("field", "value"), [
        ("environment", "other-environment"), ("vhost", "/other"),
        ("entity_id", 1002), ("message_id", "different-event"),
    ]
)
def test_compare_rejects_a_different_trace_scope(saved_pair, field, value) -> None:
    _, before, after = saved_pair
    after["target_trace"][field] = value

    with pytest.raises(ValueError, match="Reports must match"):
        compare_trace_reports(before, after)


def test_compare_requires_resolved_trace_identity(saved_pair) -> None:
    _, before, after = saved_pair
    after["target_trace"]["message_id"] = None
    with pytest.raises(ValueError, match="must resolve a message ID"):
        compare_trace_reports(before, after)

    after.pop("target_trace")
    with pytest.raises(ValueError, match="must refer to HireHub trace reports"):
        compare_trace_reports(before, after)


def test_reports_filters_hirehub_history_before_applying_limit(saved_pair) -> None:
    database, before, after = saved_pair
    other_environment = deepcopy(after)
    other_environment["target_trace"]["environment"] = "other-lab"
    foreign = save_report(database, other_environment)
    other_delivery = deepcopy(after)
    other_delivery["target_trace"]["entity_id"] = 1002
    save_report(database, other_delivery)
    save_report(database, {
        "question": "Check queue",
        "status": "undetermined",
        "conclusion": "Queue depth is unknown.",
        "processing_correlation": None,
        "evidence": [],
    })
    original = database.read_bytes()

    result = subprocess.run(
        [
            sys.executable, "-m", "opspilot", "--db", str(database), "reports",
            "--delivery-id", "1001", "--environment", "hirehub-acceptance-sample", "--limit", "2",
        ],
        cwd=database.parent, capture_output=True, text=True, encoding="utf-8", timeout=20,
    )

    assert result.returncode == 0, result.stderr
    rows = json.loads(result.stdout)
    assert [row["task_id"] for row in rows] == [after["task_id"], before["task_id"]]
    assert [row["target_trace"] for row in rows] == [after["target_trace"], before["target_trace"]]
    assert [row["task_id"] for row in list_reports(database, limit=1, delivery_id=1001)] == [
        foreign["task_id"]
    ]
    assert list_reports(database, limit=20, environment="missing-lab") == []
    assert database.read_bytes() == original
