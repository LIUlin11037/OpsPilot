import json
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import parse_qs, urlsplit

import pytest

from opspilot.target_config import load_target_config

HIREHUB_EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "hirehub-notification-snapshot.json"


def _cli(directory: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "opspilot", *args], cwd=directory,
        capture_output=True, encoding="utf-8", timeout=20,
    )


@pytest.mark.parametrize("target_file", [False, True])
@pytest.mark.parametrize(
    ("question", "workflow", "terms", "expected_path", "tool"),
    [
        ("服务返回 503", "http_health", ["返回 503"], "/health", "inspect_service_health"),
        ("RabbitMQ 队列积压", "queue", ["rabbitmq", "队列", "积压"],
         "/api/queues/%2F/orders", "inspect_queue"),
    ],
)
def test_auto_diagnosis_runs_and_saves_the_selected_workflow(
    processing_service, monkeypatch, question, workflow, terms, expected_path, tool, target_file,
) -> None:
    queue_metrics = b'{"messages_ready": 3, "messages_unacknowledged": 0, "consumers": 1}'

    def respond(path):
        if path in ("/health", "/dependency"):
            return (503 if path == "/health" else 204), b""
        return 200, queue_metrics

    if workflow == "queue":
        monkeypatch.setenv("OPSPILOT_RABBITMQ_USER", "test-reader")
        monkeypatch.setenv("OPSPILOT_RABBITMQ_PASSWORD", "test-password")
    else:
        monkeypatch.delenv("OPSPILOT_RABBITMQ_USER", raising=False)
        monkeypatch.delenv("OPSPILOT_RABBITMQ_PASSWORD", raising=False)

    with TemporaryDirectory(prefix="opspilot-auto-diagnosis-") as directory:
        expected_requests = [expected_path]
        with processing_service(respond) as (endpoint, requests):
            base = endpoint.removesuffix("/processing")
            health = {"service_url": f"{base}/health"}
            if target_file:
                config = Path(directory) / "target.json"
                config.write_text(json.dumps({
                    "health": {**health, "dependency_urls": [f"{base}/dependency"]},
                    "rabbitmq": {"management_url": base, "queue": "orders", "vhost": "staging /"},
                }), encoding="utf-8-sig")
                options = ("--config", str(config))
                if workflow == "http_health":
                    expected_requests.append("/dependency")
                else:
                    expected_requests = ["/api/queues/staging%20%2F/orders"]
            else:
                config = Path(directory) / "health.json"
                config.write_text(json.dumps(health), encoding="utf-8")
                options = ("--health-config", str(config), "--management-url", base, "--queue", "orders")
            result = _cli(directory, "diagnose-auto", *options, question)

        assert result.returncode == 0, result.stderr
        report = json.loads(result.stdout)
        assert report["routing"] == {"workflow": workflow, "matched_terms": terms}
        assert report["question"] == question
        assert report["tool_calls"][0]["tool"] == tool
        assert requests == expected_requests

        stored = _cli(directory, "report", report["task_id"])
        assert stored.returncode == 0 and json.loads(stored.stdout) == report
        markdown = _cli(directory, "report", report["task_id"], "--format", "markdown")
        assert markdown.returncode == 0, markdown.stderr
        assert "**Selected workflow**" in markdown.stdout
        assert f"**Matched terms** {', '.join(terms)}" in markdown.stdout


@pytest.mark.parametrize("force_dependencies", [False, True])
def test_auto_diagnosis_tracks_each_dependency_of_a_healthy_service(processing_service, force_dependencies) -> None:
    with TemporaryDirectory(prefix="opspilot-auto-recheck-") as directory:
        config = Path(directory) / "health.json"
        with processing_service(lambda path: (503 if path == "/dependency" else 204, b"")) as (endpoint, requests):
            base = endpoint.removesuffix("/processing")
            dependencies = [f"{base}/dependency", f"{base}/dependency", f"{base}/second"]
            config.write_text(json.dumps({
                "service_url": f"{base}/health", "dependency_urls": dependencies,
            }), encoding="utf-8")
            options = ["diagnose-auto", "--health-config", str(config), "Check HTTP health"]
            if force_dependencies:
                options.append("--check-dependencies")
            preview = _cli(directory, *options, "--plan")
            assert preview.returncode == 0, preview.stderr
            checks = json.loads(preview.stdout)["checks"]
            assert checks == [
                {"tool": "inspect_service_health", "when": "always", "count": 1, "sources": [f"{base}/health"]},
                {
                    "tool": "inspect_dependency_health", "sources": dependencies, "count": 3,
                    "when": "always" if force_dependencies else "service_unhealthy_or_unknown",
                },
            ]
            assert requests == []
            result = _cli(directory, *options)

        assert result.returncode == 0, result.stderr
        assert requests == (["/health", "/dependency", "/dependency", "/second"] if force_dependencies else ["/health"])
        report = json.loads(result.stdout)
        assert report["status"] == ("undetermined" if force_dependencies else "no_failure_observed")
        assert report["planned_checks"] == checks
        assert [item["http_status"] for item in report["evidence"]] == ([204, 503, 503, 204] if force_dependencies else [204])
        markdown = _cli(directory, "report", report["task_id"], "--format", "markdown")
        assert markdown.returncode == 0, markdown.stderr
        condition = "Always" if force_dependencies else "Service unhealthy or unknown"
        recorded = 2 if force_dependencies else 0
        assert fr"| inspect\_dependency\_health | {base}/dependency | {condition} | 2 | {recorded} |" in markdown.stdout
        recorded = 1 if force_dependencies else 0
        assert fr"| inspect\_dependency\_health | {base}/second | {condition} | 1 | {recorded} |" in markdown.stdout


@pytest.mark.parametrize(
    ("options", "question", "error"),
    [
        ([], "Something is wrong", "Describe a queue symptom or an HTTP health symptom"),
        ([], "队列积压，HTTP 5xx", "Both queue and HTTP symptoms matched"),
        (["--health-config", "unused.json"], "Queue backlog", "Supply --management-url and --queue"),
        ([], "HTTP health check", "Supply --health-config"),
        (["--config", "unused.json", "--health-config", "health.json"],
         "HTTP health check", "--config cannot be combined with direct target options"),
        (["--message-id", "msg-101"], "Check this delivery", "--message-id requires --config"),
        (["--config", "unused.json", "--message-id", " "],
         "Check this delivery", "--message-id must not be empty"),
        (["--config", "unused.json", "--message-id", "msg-101"],
         "HTTP health check", "HTTP health symptoms conflict with --message-id"),
        (["--delivery-id", "1001"], "投递通知没收到", "--delivery-id requires --config"),
        (["--delivery-id", "0"], "投递通知没收到", "--delivery-id must be positive"),
        (["--message-id", "msg-101", "--delivery-id", "1001"],
         "投递通知没收到", "not allowed with argument --message-id"),
        (["--config", "unused.json", "--message-id", "msg-101", "--workflow", "http_health"],
         "Check delivery", "--message-id requires --workflow queue"),
        (["--config", "unused.json", "--delivery-id", "1001", "--workflow", "queue"],
         "Trace delivery", "cannot be combined with --workflow"),
        (["--check-dependencies", "--workflow", "queue"],
         "Check backlog", "--check-dependencies requires the HTTP health workflow"),
        (["--check-consumer-health", "--workflow", "http_health"],
         "Check HTTP health", "--check-consumer-health requires the RabbitMQ queue workflow"),
        (["--check-dependencies"], "Queue backlog", "--check-dependencies requires the HTTP health workflow"),
        (["--check-consumer-health"], "HTTP health", "--check-consumer-health requires the RabbitMQ queue workflow"),
        (["--check-consumer-health", "--check-dependencies"],
         "Check again", "--check-consumer-health cannot be combined with --check-dependencies"),
        (["--check-consumer-health", "--management-url", "http://localhost", "--queue", "orders"],
         "Check backlog", "--check-consumer-health requires rabbitmq.consumer_health_url in --config"),
        (["--check-dependencies", "--config", "unused.json", "--delivery-id", "1001"],
         "Trace delivery", "--check-dependencies requires the HTTP health workflow"),
        (["--voter-id", "001abc"], "Trace voter", "--voter-id requires --config"),
        (["--voter-id", "not-hex", "--config", "unused.json"],
         "Trace voter", "--voter-id must contain 1 to 255 hexadecimal digits"),
        (["--voter-id", "001abc", "--workflow", "http_health"],
         "Trace voter", "cannot be combined with --workflow"),
        (["--voter-id", "001abc", "--delivery-id", "1001"],
         "Trace voter", "not allowed with argument --voter-id"),
    ],
)
def test_auto_diagnosis_requires_clear_and_configured_symptoms(options, question, error) -> None:
    with TemporaryDirectory(prefix="opspilot-auto-diagnosis-") as directory:
        result = _cli(directory, "diagnose-auto", *options, question)

        assert result.returncode == 2
        assert error in result.stderr
        assert not (Path(directory) / ".opspilot").exists()


def test_auto_diagnosis_requires_the_selected_section_in_target_file() -> None:
    with TemporaryDirectory(prefix="opspilot-target-config-") as directory:
        config = Path(directory) / "target.json"
        config.write_text(
            '{"rabbitmq": {"management_url": "http://localhost:15672", "queue": "orders"}}',
            encoding="utf-8",
        )
        result = _cli(directory, "diagnose-auto", "--config", str(config), "HTTP health check")

        assert result.returncode == 2
        assert "a health section in --config" in result.stderr
        assert not (Path(directory) / ".opspilot").exists()


@pytest.mark.parametrize(
    ("workflow", "terms", "expected_request"),
    [
        ("http_health", ["http"], "/health"),
        ("queue", ["队列", "积压"], "/api/queues/%2F/orders"),
    ],
)
def test_auto_explicit_workflow_limits_requests_and_is_recorded_in_report(
    processing_service, monkeypatch, workflow, terms, expected_request,
) -> None:
    question = "HTTP 503，队列积压"
    metrics = b'{"messages_ready": 3, "messages_unacknowledged": 0, "consumers": 1}'

    def respond(path):
        return (503, b"") if path == "/health" else (200, metrics)

    monkeypatch.setenv("OPSPILOT_RABBITMQ_USER", "test-reader")
    monkeypatch.setenv("OPSPILOT_RABBITMQ_PASSWORD", "test-password")
    with TemporaryDirectory(prefix="opspilot-explicit-workflow-") as directory:
        config = Path(directory) / "target.json"
        with processing_service(respond) as (endpoint, requests):
            base = endpoint.removesuffix("/processing")
            config.write_text(json.dumps({
                "health": {"service_url": f"{base}/health"},
                "rabbitmq": {"management_url": base, "queue": "orders"},
            }), encoding="utf-8")
            options = ("diagnose-auto", "--config", str(config), "--workflow", workflow)
            preview = _cli(directory, *options, "--plan", question)
            assert preview.returncode == 0, preview.stderr
            assert requests == []
            result = _cli(directory, *options, question)

        assert result.returncode == 0, result.stderr
        plan = json.loads(preview.stdout)
        report = json.loads(result.stdout)
        assert report["routing"] == plan["routing"] == {
            "workflow": workflow, "matched_terms": terms, "selected_by": "workflow",
        }
        assert report["planned_scope"] == plan["scope"]
        source_key = "service_url" if workflow == "http_health" else "queue_source"
        assert report["planned_scope"][source_key] == report["evidence"][0]["source"]
        assert report["planned_checks"] == plan["checks"]
        assert requests == [expected_request]
        markdown = _cli(directory, "report", report["task_id"], "--format", "markdown")
        assert markdown.returncode == 0, markdown.stderr
        assert "**Selection basis** Explicit workflow" in markdown.stdout
        assert "## Planned scope" in markdown.stdout
        assert report["planned_scope"][source_key] in markdown.stdout


@pytest.mark.parametrize(
    ("option", "question", "workflow", "terms", "tool", "expected_requests"),
    [
        ("--check-consumer-health", "复查消费者健康检查和队列积压", "queue",
         ["消费者", "队列", "积压"], "inspect_consumer_endpoint",
         ["/api/queues/%2F/orders", "/consumer-health"]),
        ("--check-dependencies", "复查依赖", "http_health", [], "inspect_dependency_health",
         ["/health", "/dependency"]),
        ("--check-service-log", "队列积压，HTTP 5xx", "http_health", ["http", "5xx"], "inspect_service_log",
         ["/health"]),
    ],
)
def test_auto_diagnosis_uses_requested_checks_to_resolve_the_workflow(
    processing_service, monkeypatch, option, question, workflow, terms, tool, expected_requests,
) -> None:
    def respond(path: str) -> tuple[int, bytes]:
        if path == "/api/queues/%2F/orders":
            return 200, b'{"messages_ready": 0, "messages_unacknowledged": 0, "consumers": 1}'
        return (503 if path == "/consumer-health" else 204), b""

    monkeypatch.setenv("OPSPILOT_RABBITMQ_USER", "test-reader")
    monkeypatch.setenv("OPSPILOT_RABBITMQ_PASSWORD", "test-password")
    with TemporaryDirectory(prefix="opspilot-requested-check-") as directory:
        config = Path(directory) / "target.json"
        (config.parent / "service.jsonl").write_text("", encoding="utf-8")
        with processing_service(respond) as (endpoint, requests):
            base = endpoint.removesuffix("/processing")
            config.write_text(json.dumps({
                "health": {
                    "service_url": f"{base}/health", "dependency_urls": [f"{base}/dependency"],
                    "service_log_path": "service.jsonl",
                },
                "rabbitmq": {
                    "management_url": base, "queue": "orders", "consumer_health_url": f"{base}/consumer-health",
                },
            }), encoding="utf-8")
            options = ("diagnose-auto", "--config", str(config), option, question)
            preview = _cli(directory, *options, "--plan")
            assert preview.returncode == 0, preview.stderr
            assert requests == []
            assert not (config.parent / ".opspilot").exists()
            diagnosis = _cli(directory, *options)

        assert diagnosis.returncode == 0, diagnosis.stderr
        plan = json.loads(preview.stdout)
        report = json.loads(diagnosis.stdout)
        assert report["routing"] == plan["routing"] == {
            "workflow": workflow, "matched_terms": terms,
            "selected_by": option.removeprefix("--").replace("-", "_"),
        }
        assert report["planned_checks"] == plan["checks"]
        check, = [check for check in plan["checks"] if check["tool"] == tool]
        assert check["when"] == "always"
        assert len(report["tool_calls"]) == 2
        assert report["tool_calls"][1]["tool"] == tool
        assert requests == expected_requests
        markdown = _cli(directory, "report", report["task_id"], "--format", "markdown")
        assert markdown.returncode == 0, markdown.stderr
        assert f"**Selection basis** {option}" in markdown.stdout


@pytest.mark.parametrize(
    ("options", "question", "workflow", "scope", "checks"),
    [
        ([], "服务返回 503", "http_health", {}, [
            ("inspect_service_health", "always", 1),
            ("inspect_dependency_health", "service_unhealthy_or_unknown", 2),
        ]),
        ([], "队列积压", "queue", {"queue": "orders", "vhost": "staging"}, [
            ("inspect_queue", "always", 1),
            ("inspect_consumer_endpoint", "queue_activity", 1),
            ("inspect_processing", "queue_activity", 1),
        ]),
        (["--message-id", "msg-101"], "这条消息处理了吗？", "queue",
         {"queue": "orders", "vhost": "staging", "message_id": "msg-101"}, [
             ("inspect_queue", "always", 1),
             ("inspect_consumer_endpoint", "queue_activity", 1),
             ("inspect_processing", "always", 1),
         ]),
        (["--delivery-id", "1001"], "投递通知没收到", "target_trace", {"delivery_id": 1001}, [
            ("inspect_target_trace", "always", 1),
        ]),
    ],
)
def test_auto_plan_previews_checks_without_running_or_saving_a_diagnosis(
    processing_service, monkeypatch, options, question, workflow, scope, checks,
) -> None:
    for name in ("OPSPILOT_RABBITMQ", "OPSPILOT_MYSQL"):
        monkeypatch.delenv(f"{name}_USER", raising=False)
        monkeypatch.delenv(f"{name}_PASSWORD", raising=False)

    with TemporaryDirectory(prefix="opspilot-auto-plan-") as directory:
        config = Path(directory) / "target.json"
        database = Path(directory) / "reports.sqlite3"
        with processing_service(lambda path: (503, b"")) as (endpoint, requests):
            base = endpoint.removesuffix("/processing")
            config.write_text(json.dumps({
                "health": {
                    "service_url": f"{base}/health", "dependency_urls": [f"{base}/dependency"],
                    "mysql_dependency_urls": ["mysql://localhost:3306"],
                },
                "rabbitmq": {
                    "management_url": base, "queue": "orders", "vhost": "staging",
                    "consumer_health_url": f"{base}/consumer-health", "processing_url": endpoint,
                },
                "hirehub": {"snapshot": "unavailable.json"},
            }), encoding="utf-8")
            result = _cli(directory, "--db", str(database), "diagnose-auto", "--config", str(config),
                          "--plan", *options, question)

        assert result.returncode == 0, result.stderr
        plan = json.loads(result.stdout)
        assert plan["question"] == question and plan["routing"]["workflow"] == workflow
        if options:
            assert plan["routing"]["selected_by"] == options[0].removeprefix("--").replace("-", "_")
        if workflow == "http_health":
            source = {"service_url": f"{base}/health"}
            assert plan["checks"][1]["sources"] == [f"{base}/dependency", "mysql://localhost:3306"]
        elif workflow == "queue":
            source = {"queue_source": f"{base}/api/queues/staging/orders"}
        else:
            source = {"snapshot_source": (config.parent / "unavailable.json").as_uri()}
        assert plan["scope"] == {**scope, **source}
        assert [(item["tool"], item["when"], item["count"]) for item in plan["checks"]] == checks
        assert requests == []
        assert not database.exists()


@pytest.mark.parametrize("endpoint_kind", ["service", "queue", "mysql"])
def test_auto_plan_rejects_embedded_credentials_before_showing_sources(endpoint_kind) -> None:
    url = "http://admin:fixture-secret@127.0.0.1:8000"
    expected_error = "without credentials"
    question = "HTTP health"
    if endpoint_kind == "queue":
        config_data = {"rabbitmq": {"management_url": url, "queue": "orders"}}
        question = "Queue backlog"
    elif endpoint_kind == "mysql":
        config_data = {"health": {
            "service_url": "http://127.0.0.1/health", "mysql_dependency_urls": [url.replace("http:", "mysql:")],
        }}
        expected_error = "Use a MySQL health URL"
    else:
        config_data = {"health": {"service_url": url}}
    with TemporaryDirectory(prefix="opspilot-auto-plan-url-") as directory:
        config = Path(directory) / "target.json"
        config.write_text(json.dumps(config_data), encoding="utf-8")
        result = _cli(directory, "diagnose-auto", "--config", str(config), "--plan", question)
        assert result.returncode == 2
        assert result.stdout == ""
        assert expected_error in result.stderr
        assert "fixture-secret" not in result.stderr
        assert not (Path(directory) / ".opspilot").exists()


@pytest.mark.parametrize(
    ("pending", "unacknowledged", "processing_source", "force_consumer"),
    [
        (3, 0, "processing_url", False), (None, 2, "processing_record", False),
        (0, 0, "processing_url", False), (0, 0, "processing_url", True),
    ],
)
def test_target_queue_checks_follow_each_configured_condition(
    processing_service, monkeypatch, pending, unacknowledged, processing_source, force_consumer,
) -> None:
    metrics = json.dumps({
        "messages_ready": pending, "messages_unacknowledged": unacknowledged, "consumers": 1,
    }).encode()
    record = json.dumps({
        "queue": "orders", "vhost": "staging", "message_id": "msg-101",
        "result": "succeeded", "attempted_at": "2026-11-19T03:00:00Z",
    })
    queue_path = "/api/queues/staging/orders"
    consumer_status = 403 if pending is None else 503

    def respond(path):
        if path == queue_path:
            return 200, metrics
        if path == "/consumer-health":
            return consumer_status, b""
        return 200, record.encode()

    monkeypatch.setenv("OPSPILOT_RABBITMQ_USER", "test-reader")
    monkeypatch.setenv("OPSPILOT_RABBITMQ_PASSWORD", "test-password")
    with TemporaryDirectory(prefix="opspilot-target-config-") as directory:
        config_dir = Path(directory) / "configs"
        config_dir.mkdir()
        config = config_dir / "target.json"
        record_path = config_dir / "processing record.json"
        record_path.write_text(record, encoding="utf-8")
        with processing_service(respond) as (endpoint, requests):
            base = endpoint.removesuffix("/processing")
            processing_value = endpoint if processing_source == "processing_url" else record_path.name
            config.write_text(json.dumps({
                "rabbitmq": {
                    "management_url": base, "queue": "orders", "vhost": "staging",
                    "consumer_health_url": f"{base}/consumer-health",
                    processing_source: processing_value,
                },
            }), encoding="utf-8")
            options = ["--check-consumer-health"] if force_consumer else []
            if force_consumer:
                preview = _cli(directory, "diagnose-auto", "--config", str(config), *options, "--plan", "队列积压")
                assert preview.returncode == 0, preview.stderr
                checks = json.loads(preview.stdout)["checks"]
                assert [check["when"] for check in checks] == ["always", "always", "queue_activity"]
                assert requests == []
            result = _cli(directory, "diagnose-auto", "--config", str(config), *options, "队列积压")
        assert result.returncode == 0, result.stderr
        report = json.loads(result.stdout)
        config.unlink()
        markdown = _cli(directory, "report", report["task_id"], "--format", "markdown")

    assert markdown.returncode == 0, markdown.stderr
    assert report["routing"]["workflow"] == "queue"
    record_source = f"{endpoint}?queue=orders&vhost=staging" if processing_source == "processing_url" else record_path.as_uri()
    assert report["planned_checks"] == [
        {"tool": "inspect_queue", "when": "always", "count": 1, "sources": [base + queue_path]},
        {
            "tool": "inspect_consumer_endpoint", "when": "always" if force_consumer else "queue_activity",
            "count": 1, "sources": [f"{base}/consumer-health"],
        },
        {"tool": "inspect_processing", "when": "queue_activity", "count": 1, "sources": [record_source]},
    ]
    expected_calls = 0 if pending == 0 and unacknowledged == 0 else 1
    assert "## Planned checks" in markdown.stdout
    assert fr"| inspect\_queue | {base}{queue_path} | Always | 1 | 1 |" in markdown.stdout
    for tool in ("inspect_consumer_endpoint", "inspect_processing"):
        label = tool.replace("_", "\\_")
        forced = tool == "inspect_consumer_endpoint" and force_consumer
        condition = "Always" if forced else "Pending or unacknowledged messages observed"
        assert any(
            line.startswith(f"| {label} | ")
            and line.endswith(f" | {condition} | 1 | {1 if forced else expected_calls} |")
            for line in markdown.stdout.splitlines()
        )
    if pending == 0 and unacknowledged == 0:
        expected_paths = [queue_path]
        expected_tools = ["inspect_queue"]
        if force_consumer:
            expected_paths.append("/consumer-health")
            expected_tools.append("inspect_consumer_endpoint")
            assert report["evidence"][1]["health"] == "unhealthy"
        assert requests == expected_paths
        assert [call["tool"] for call in report["tool_calls"]] == expected_tools
        return

    queue, health, processing = report["evidence"]
    assert queue["unacknowledged_messages"] == unacknowledged
    assert health["tool"] == "inspect_consumer_endpoint" and health["http_status"] == consumer_status
    assert health["outcome"] == ("missing" if consumer_status == 403 else "observed")
    assert processing["tool"] == "inspect_processing" and processing["message_id"] == "msg-101"
    assert [call["tool"] for call in report["tool_calls"]] == [
        "inspect_queue", "inspect_consumer_endpoint", "inspect_processing",
    ]
    if processing_source == "processing_url":
        queue_request, health_request, processing_request = requests
        assert (queue_request, health_request) == (queue_path, "/consumer-health")
        url = urlsplit(processing_request)
        assert url.path == "/processing"
        assert parse_qs(url.query) == {"queue": ["orders"], "vhost": ["staging"]}
    else:
        assert requests == [queue_path, "/consumer-health"]
        assert processing["source"] == record_path.as_uri()


@pytest.mark.parametrize("queue_status", [200, 403])
def test_auto_diagnosis_can_lookup_exact_message_independently_of_queue_activity(
    processing_service, monkeypatch, queue_status,
) -> None:
    message_id = "Order+42 /雪"
    explicit_workflow = queue_status == 403
    options = ("--workflow", "queue") if explicit_workflow else ()
    question = "HTTP 503，这条消息处理了吗？" if explicit_workflow else "这条消息处理了吗？"
    selection_basis = "workflow" if explicit_workflow else "message_id"
    queue_path = "/api/queues/%2F/orders"
    metrics = b'{"messages_ready": 0, "messages_unacknowledged": 0, "consumers": 1}'
    records = json.dumps([
        {
            "queue": "orders", "vhost": "/", "message_id": message_id,
            "result": "failed", "attempted_at": "2026-11-19T03:00:00Z",
        },
        {
            "queue": "orders", "vhost": "/", "message_id": "another-message",
            "result": "succeeded", "attempted_at": "2026-11-19T03:01:00Z",
        },
    ]).encode()

    def respond(path):
        if path == queue_path:
            return queue_status, metrics
        return 200, records

    monkeypatch.setenv("OPSPILOT_RABBITMQ_USER", "test-reader")
    monkeypatch.setenv("OPSPILOT_RABBITMQ_PASSWORD", "test-password")
    with TemporaryDirectory(prefix="opspilot-target-message-") as directory:
        config = Path(directory) / "target.json"
        with processing_service(respond) as (endpoint, requests):
            base = endpoint.removesuffix("/processing")
            config.write_text(json.dumps({
                "rabbitmq": {
                    "management_url": base, "queue": "orders", "processing_url": endpoint,
                    "consumer_health_url": f"{base}/health",
                },
            }), encoding="utf-8")
            result = _cli(
                directory, "diagnose-auto", "--config", str(config), "--message-id", message_id, *options, question,
            )

        assert result.returncode == 0, result.stderr
        report = json.loads(result.stdout)
        assert report["routing"] == {"workflow": "queue", "matched_terms": [], "selected_by": selection_basis}
        assert report["requested_message_id"] == message_id
        queue, processing = report["evidence"]
        assert report["planned_checks"][-1] == {
            "tool": "inspect_processing", "when": "always", "count": 1, "sources": [processing["source"]],
        }
        assert queue["outcome"] == ("observed" if queue_status == 200 else "missing")
        assert processing["message_id"] == message_id and processing["result"] == "failed"
        queue_request, processing_request = requests
        assert queue_request == queue_path
        url = urlsplit(processing_request)
        assert url.path == "/processing"
        assert parse_qs(url.query) == {"queue": ["orders"], "vhost": ["/"], "message_id": [message_id]}
        markdown = _cli(directory, "report", report["task_id"], "--format", "markdown")
        assert markdown.returncode == 0, markdown.stderr
        expected_basis = "Explicit workflow" if explicit_workflow else "Requested message ID"
        assert f"**Selection basis** {expected_basis}" in markdown.stdout
        assert f"**Requested message ID** {message_id}" in markdown.stdout


def test_auto_message_lookup_requires_processing_source() -> None:
    with TemporaryDirectory(prefix="opspilot-target-message-") as directory:
        config = Path(directory) / "target.json"
        config.write_text(
            '{"rabbitmq": {"management_url": "http://localhost:15672", "queue": "orders"}}',
            encoding="utf-8",
        )
        result = _cli(
            directory, "diagnose-auto", "--config", str(config), "--message-id", "msg-101", "Check delivery",
        )

        assert result.returncode == 2
        assert "--message-id requires rabbitmq.processing_url or rabbitmq.processing_record" in result.stderr
        assert not (Path(directory) / ".opspilot").exists()


@pytest.mark.parametrize("delivery_id", [1001, 1002])
def test_auto_delivery_trace_reuses_the_snapshot_workflow(delivery_id: int) -> None:
    with TemporaryDirectory(prefix="opspilot-target-delivery-") as directory:
        config_dir = Path(directory) / "configs"
        config_dir.mkdir()
        snapshot = config_dir / "delivery evidence.json"
        snapshot.write_bytes(HIREHUB_EXAMPLE.read_bytes())
        config = config_dir / "target.json"
        config.write_text(json.dumps({"hirehub": {"snapshot": snapshot.name}}), encoding="utf-8")
        question = "投递成功后没收到通知，队列是否积压？"
        result = _cli(
            directory, "diagnose-auto", "--config", str(config), "--delivery-id", str(delivery_id), question,
        )
        explicit = _cli(
            directory, "diagnose-hirehub", "--snapshot", str(snapshot), "--delivery-id", str(delivery_id), question,
        )

        assert result.returncode == 0, result.stderr
        assert explicit.returncode == 0, explicit.stderr
        report = json.loads(result.stdout)
        baseline = json.loads(explicit.stdout)
        assert report["routing"] == {"workflow": "target_trace", "matched_terms": [], "selected_by": "delivery_id"}
        for field in ("question", "status", "conclusion", "target_trace", "next_checks"):
            assert report[field] == baseline[field]
        assert [(call["tool"], call["outcome"]) for call in report["tool_calls"]] == [
            (call["tool"], call["outcome"]) for call in baseline["tool_calls"]
        ]
        assert report["target_trace"]["entity_id"] == delivery_id
        if delivery_id == 1001:
            assert report["evidence"][0]["source"] == f"{snapshot.as_uri()}#delivery"
            assert report["evidence"] == baseline["evidence"]
            assert report["target_trace"]["branches"] == {"job": "observed", "notification": "not_observed"}
        else:
            assert report["evidence"][0]["source"] == snapshot.as_uri()
            assert report["evidence"][0]["reason"] == "delivery_id_mismatch"
        markdown = _cli(directory, "report", report["task_id"], "--format", "markdown")
        assert markdown.returncode == 0, markdown.stderr
        assert "**Selection basis** Requested delivery ID" in markdown.stdout
        assert f"**Entity ID** {delivery_id}" in markdown.stdout


def test_auto_delivery_trace_requires_hirehub_snapshot_configuration() -> None:
    with TemporaryDirectory(prefix="opspilot-target-delivery-") as directory:
        config = Path(directory) / "target.json"
        config.write_text('{"health": {"service_url": "http://localhost/health"}}', encoding="utf-8")
        result = _cli(directory, "diagnose-auto", "--config", str(config), "--delivery-id", "1001", "Trace delivery")

        assert result.returncode == 2
        assert "--delivery-id requires hirehub.snapshot in --config" in result.stderr
        assert not (Path(directory) / ".opspilot").exists()


def test_auto_voting_trace_previews_then_reads_the_configured_snapshot() -> None:
    with TemporaryDirectory(prefix="opspilot-target-voting-") as directory:
        config_dir = Path(directory) / "configs"
        config_dir.mkdir()
        snapshot = config_dir / "pending vote.json"
        config = config_dir / "target.json"
        config.write_text(json.dumps({
            "voting": {"snapshot": snapshot.name},
            "hirehub": {"snapshot": "no-delivery-file.json"},
            "health": {"service_url": "http://localhost/health"},
        }), encoding="utf-8")
        question = "HTTP 503，队列积压，投票是否已入库？"
        options = ("diagnose-auto", "--config", str(config), "--voter-id", "001abc", question)
        preview = _cli(directory, *options, "--plan")
        assert preview.returncode == 0, preview.stderr
        plan = json.loads(preview.stdout)
        assert plan == {
            "question": question,
            "routing": {"workflow": "target_trace", "matched_terms": [], "selected_by": "voter_id"},
            "scope": {"voter_id": "001abc", "snapshot_source": snapshot.as_uri()},
            "checks": [{"tool": "inspect_target_trace", "when": "always", "count": 1}],
        }
        assert not (Path(directory) / ".opspilot").exists()

        observed_at = "2026-11-22T05:00:00+00:00"
        snapshot.write_text(json.dumps({
            "environment": "voting-test", "voter_id": "001abc", "captured_at": observed_at,
            "queued_vote": {"observed_at": observed_at, "outcome": "observed", "row_count": 1},
            "stored_vote": {"observed_at": observed_at, "outcome": "observed", "row_count": 0},
        }), encoding="utf-8")
        result = _cli(directory, *options)
        assert result.returncode == 0, result.stderr
        report = json.loads(result.stdout)
        assert report["routing"] == plan["routing"]
        assert report["planned_scope"] == plan["scope"]
        assert report["planned_checks"] == plan["checks"]
        assert report["target_trace"]["entity_id"] == "001abc"
        assert report["target_trace"]["branches"] == {"queued_vote": "observed", "stored_vote": "not_observed"}
        assert report["evidence"][0]["source"] == f"{snapshot.as_uri()}#queued_vote"
        markdown = _cli(directory, "report", report["task_id"], "--format", "markdown")
        assert markdown.returncode == 0, markdown.stderr
        assert "**Selection basis** Requested voter ID" in markdown.stdout
        source_line, = (
            line for line in markdown.stdout.splitlines() if line.startswith("- **Snapshot source** ")
        )
        assert source_line.endswith("/configs/pending%20vote.json")


def test_auto_voting_trace_requires_voting_snapshot_configuration() -> None:
    with TemporaryDirectory(prefix="opspilot-target-voting-") as directory:
        config = Path(directory) / "target.json"
        config.write_text('{"hirehub": {"snapshot": "delivery.json"}}', encoding="utf-8")
        result = _cli(directory, "diagnose-auto", "--config", str(config), "--voter-id", "001abc", "Trace voter")
        assert result.returncode == 2
        assert "--voter-id requires voting.snapshot in --config" in result.stderr
        assert not (Path(directory) / ".opspilot").exists()


@pytest.mark.parametrize(
    ("data", "error"),
    [
        ({}, "requires a health, rabbitmq, hirehub, or voting section"),
        ({"health": None}, "health must be a JSON object"),
        ({"hirehub": {"snapshot": ""}}, "hirehub.snapshot must be a nonempty path"),
        ({"hirehub": {"snapshot": "snapshot.json", "management_url": "http://localhost"}},
         "HireHub configuration contains unsupported fields"),
        ({"voting": []}, "voting must be a JSON object"),
        ({"voting": {"snapshot": ""}}, r"voting\.snapshot must be a nonempty path"),
        ({"rabbitmq": {"management_url": "http://localhost"}}, "rabbitmq.queue must be a nonempty string"),
        ({"rabbitmq": {"management_url": "http://localhost", "queue": "orders", "password": "fixture"}},
         "unsupported fields"),
        ({"rabbitmq": {"management_url": "http://localhost", "queue": "orders", "timeout_seconds": True}},
         "timeout_seconds must be a finite positive number"),
        ({"rabbitmq": {
            "management_url": "http://localhost", "queue": "orders",
            "processing_url": "http://localhost/processing", "processing_record": "processing.json",
        }}, "processing_url and rabbitmq.processing_record are mutually exclusive"),
    ],
)
def test_target_config_rejects_invalid_fields(data, error) -> None:
    with TemporaryDirectory(prefix="opspilot-target-config-") as directory:
        config = Path(directory) / "target.json"
        config.write_text(json.dumps(data), encoding="utf-8")
        with pytest.raises(ValueError, match=error):
            load_target_config(config)
