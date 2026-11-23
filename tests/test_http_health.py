import json
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from time import sleep

import pytest

from opspilot.__main__ import main
from opspilot.connectors.http_health import HttpHealthConnector
from opspilot.connectors.mysql import MySQLConnector, MySQLRead
from opspilot.core.health_diagnosis import run_health_diagnosis
from opspilot.health_config import HealthConfig, load_health_config


def _cli(directory: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "opspilot", *args], cwd=directory,
        capture_output=True, text=True, encoding="utf-8", timeout=20,
    )


@pytest.mark.parametrize(
    ("service_status", "dependency_status", "service_health", "dependency_health"),
    [
        (204, 503, "healthy", None),
        (503, 204, "unhealthy", "healthy"),
        (503, 500, "unhealthy", "unhealthy"),
        (403, 204, "unknown", "healthy"),
        (503, None, "unhealthy", None),
    ],
)
def test_health_cli_chooses_read_only_checks_and_saves_report(
    processing_service, service_status, dependency_status, service_health, dependency_health,
) -> None:
    def respond(path):
        if path == "/service/health":
            return service_status, b""
        assert path == "/dependency/health"
        assert dependency_status is not None
        return dependency_status, b""

    with TemporaryDirectory(prefix="opspilot-health-test-") as directory:
        with processing_service(respond) as (endpoint, requests):
            base = endpoint.rsplit("/", 1)[0]
            args = ["diagnose-http", "--service-url", f"{base}/service/health"]
            if dependency_status is not None:
                args.extend(("--dependency-url", f"{base}/dependency/health"))
            result = _cli(directory, *args, "Check HTTP service health")
        assert result.returncode == 0, result.stderr
        report = json.loads(result.stdout)
        assert len(report["evidence"]) == len(report["tool_calls"])
        service, *remaining = report["evidence"]
        assert service["http_status"] == service_status
        assert service["health"] == service_health
        assert service["source"] == f"{base}/service/health"
        assert service["tool"] == "inspect_service_health"
        if dependency_health is None:
            assert requests == ["/service/health"]
            assert not remaining
        else:
            assert requests == ["/service/health", "/dependency/health"]
            dependency, = remaining
            assert dependency["health"] == dependency_health
            assert dependency["http_status"] == dependency_status
            assert dependency["tool"] == "inspect_dependency_health"
        assert report["status"] == (
            "no_failure_observed" if service_health == "healthy" else "undetermined"
        )
        assert report["processing_correlation"] is None
        if service_status == 403:
            assert service["outcome"] == "missing"
            assert service["reason"] == "access_denied"
        assert all(call["duration_ms"] >= 0 for call in report["tool_calls"])
        stored = _cli(directory, "report", report["task_id"])
        assert stored.returncode == 0 and json.loads(stored.stdout) == report
        if service_status == 503 and dependency_health == "healthy":
            markdown = _cli(directory, "report", report["task_id"], "--format", "markdown")
            assert markdown.returncode == 0, markdown.stderr
            assert "Health unhealthy, HTTP status 503" in markdown.stdout
            assert "Health healthy, HTTP status 204" in markdown.stdout
            assert (
                f"- **Service endpoint {base}/service/health** Read request and error logs"
                in markdown.stdout
            )


@pytest.mark.parametrize(
    ("dependency_status", "dependency_health", "expected_status", "expected_action"),
    [
        (503, "unhealthy", "undetermined", "inspect_endpoint_logs"),
        (403, "unknown", "undetermined", "verify_read_access"),
        (204, "healthy", "no_failure_observed", None),
    ],
)
def test_health_cli_checks_dependencies_when_service_is_healthy(
    processing_service, dependency_status, dependency_health, expected_status, expected_action,
) -> None:
    def respond(path):
        status = 204 if path == "/service/health" else dependency_status
        return status, b""

    with TemporaryDirectory(prefix="opspilot-health-test-") as directory:
        with processing_service(respond) as (endpoint, requests):
            base = endpoint.rsplit("/", 1)[0]
            result = _cli(
                directory, "diagnose-http", "--service-url", f"{base}/service/health",
                "--dependency-url", f"{base}/dependency/health", "--check-dependencies",
                "Recheck dependencies",
            )
        assert result.returncode == 0, result.stderr
        report = json.loads(result.stdout)
        if dependency_health == "unhealthy":
            markdown = _cli(directory, "report", report["task_id"], "--format", "markdown")
            assert markdown.returncode == 0, markdown.stderr
            assert (
                f"- **Dependency endpoint {base}/dependency/health** Read request and error logs"
                in markdown.stdout
            )

    assert requests == ["/service/health", "/dependency/health"]
    service, dependency = report["evidence"]
    assert service["health"] == "healthy"
    assert dependency["health"] == dependency_health
    assert dependency["http_status"] == dependency_status
    assert report["status"] == expected_status
    assert [(check["evidence_index"], check["action"]) for check in report["next_checks"]] == (
        [] if expected_action is None else [(1, expected_action)]
    )


@pytest.mark.parametrize("entry", ["direct", "config", "auto", "plan"])
def test_health_cli_requires_a_dependency_for_an_explicit_recheck(processing_service, entry) -> None:
    automatic = entry in ("auto", "plan")
    command = "diagnose-auto" if automatic else "diagnose-http"
    with TemporaryDirectory(prefix="opspilot-required-dependency-") as directory:
        database = Path(directory) / "reports.sqlite3"
        with processing_service(lambda _path: (204, b"")) as (endpoint, requests):
            if entry == "direct":
                options = ("--service-url", endpoint)
            else:
                config = Path(directory) / "health.json"
                health = {
                    "service_url": endpoint, "dependency_urls": [],
                    "redis_dependency_urls": [], "mysql_dependency_urls": [],
                }
                config.write_text(json.dumps({"health": health} if automatic else health), encoding="utf-8")
                options = ("--config", str(config))
            if entry == "plan":
                options += ("--plan",)
            result = _cli(
                directory, "--db", str(database), command, *options, "--check-dependencies", "Recheck dependencies",
            )

        assert result.returncode == 2
        assert "--check-dependencies requires at least one" in result.stderr
        assert result.stdout == ""
        assert requests == []
        assert not database.exists()


def test_health_workflow_requires_a_dependency_tool_for_an_explicit_recheck(processing_service) -> None:
    with processing_service(lambda _path: (204, b"")) as (endpoint, requests):
        service = HttpHealthConnector(endpoint)
        with pytest.raises(ValueError, match="at least one dependency"):
            run_health_diagnosis("Recheck dependencies", service, [], check_dependencies=True)

    assert requests == []


def test_health_connector_distinguishes_socket_timeout(processing_service) -> None:
    def respond(path):
        sleep(.2)
        return 200, b""

    with processing_service(respond) as (endpoint, _):
        evidence = HttpHealthConnector(endpoint, timeout_seconds=.05).inspect_health()
    assert evidence["outcome"] == "timeout"
    assert evidence["health"] == "unknown"
    assert evidence["reason"] == "request_timeout"
    assert "http_status" not in evidence


def test_health_cli_preserves_all_observations_when_a_dependency_raises(
    processing_service, monkeypatch, capsys,
) -> None:
    original = HttpHealthConnector.inspect_health

    def inspect_health(self):
        if self.endpoint.endswith("/offline"):
            raise ConnectionResetError("audit fixture")
        return original(self)

    def respond(path):
        return (503 if path == "/service" else 204), b""

    monkeypatch.setattr(HttpHealthConnector, "inspect_health", inspect_health)
    with TemporaryDirectory(prefix="opspilot-partial-health-") as directory:
        database = Path(directory) / "reports.sqlite3"
        with processing_service(respond) as (endpoint, requests):
            base = endpoint.rsplit("/", 1)[0]
            monkeypatch.setattr(sys, "argv", [
                "opspilot", "--db", str(database), "diagnose-http",
                "--service-url", f"{base}/service", "--dependency-url", f"{base}/offline",
                "--dependency-url", f"{base}/健康/%2F", "Check service",
            ])
            main()

        captured = capsys.readouterr()
        assert captured.err == ""
        report = json.loads(captured.out)
        assert requests == ["/service", "/%E5%81%A5%E5%BA%B7/%2F"]
        assert [item["outcome"] for item in report["tool_calls"]] == ["observed", "missing", "observed"]
        assert [item["source"] for item in report["tool_calls"]] == [
            f"{base}/service", f"{base}/offline", f"{base}/健康/%2F",
        ]
        service, offline, later = report["evidence"]
        assert service["http_status"] == 503
        assert offline["source"] == f"{base}/offline"
        assert offline["reason"] == "transport_error" and offline["health"] == "unknown"
        assert later["http_status"] == 204 and later["source"] == f"{base}/健康/%2F"
        stored = _cli(directory, "--db", str(database), "report", report["task_id"])
        assert stored.returncode == 0 and json.loads(stored.stdout) == report
        markdown = _cli(directory, "--db", str(database), "report", report["task_id"], "--format", "markdown")
        assert markdown.returncode == 0, markdown.stderr
        calls_table = markdown.stdout.split("## Tool calls", 1)[1]
        assert fr"| inspect\_dependency\_health | {base}/offline | missing |" in calls_table
        assert fr"| inspect\_dependency\_health | {base}/健康/%2F | observed |" in calls_table


def test_invalid_health_url_does_not_create_report_store() -> None:
    with TemporaryDirectory(prefix="opspilot-health-invalid-") as directory:
        result = _cli(
            directory, "diagnose-http", "--service-url", "http://reader:secret@127.0.0.1/health",
            "Check health",
        )
        assert not (Path(directory) / ".opspilot").exists()
    assert result.returncode == 2
    assert "without credentials" in result.stderr
    assert "secret" not in result.stderr


def test_health_config_accepts_a_service_without_dependencies() -> None:
    with TemporaryDirectory(prefix="opspilot-health-config-test-") as directory:
        path = Path(directory) / "health.json"
        path.write_text('{"service_url":"http://127.0.0.1:8080/health"}', encoding="utf-8")
        assert load_health_config(path) == HealthConfig("http://127.0.0.1:8080/health")


@pytest.mark.parametrize(
    ("contents", "message"),
    [
        ("{", "valid UTF-8 JSON"),
        ("{}", "service_url"),
        ('{"service_url":"http://127.0.0.1", "dependencies":[]}', "unsupported fields"),
        ('{"service_url":"http://127.0.0.1", "dependency_urls":"http://127.0.0.1"}', "dependency_urls"),
        ('{"service_url":"http://127.0.0.1", "timeout_seconds":true}', "timeout_seconds"),
    ],
)
def test_health_config_rejects_invalid_contents(contents: str, message: str) -> None:
    with TemporaryDirectory(prefix="opspilot-health-config-test-") as directory:
        path = Path(directory) / "health.json"
        path.write_text(contents, encoding="utf-8")
        with pytest.raises(ValueError, match=message):
            load_health_config(path)


def test_health_cli_rejects_mixed_configuration_and_cli_options() -> None:
    with TemporaryDirectory(prefix="opspilot-health-config-test-") as directory:
        result = _cli(directory, "diagnose-http", "--config", "health.json", "--timeout", "5", "Check")
        assert not (Path(directory) / ".opspilot").exists()
    assert result.returncode == 2
    assert "--config takes dependency URLs and timeout from the configuration file" in result.stderr


@pytest.mark.parametrize(
    ("service_status", "from_config", "check_dependencies"),
    [(204, False, False), (503, False, False), (503, True, False), (204, True, True)],
)
def test_health_cli_checks_mysql_after_service_anomaly_or_when_requested(
    monkeypatch, capsys, processing_service, service_status, from_config, check_dependencies,
) -> None:
    queries = []

    def select(self, query, parameters):
        assert self.username == "reader" and self.password == "ephemeral-secret"
        queries.append((query, parameters))
        return MySQLRead("observed", "2026-11-20T06:00:00+00:00", ({"ok": 1},))

    monkeypatch.setattr(MySQLConnector, "select", select)
    monkeypatch.setenv("OPSPILOT_MYSQL_USER", "reader")
    monkeypatch.setenv("OPSPILOT_MYSQL_PASSWORD", "ephemeral-secret")
    mysql_url = "mysql://127.0.0.1:13306"
    with TemporaryDirectory(prefix="opspilot-health-mysql-test-") as directory:
        database = Path(directory) / "reports.sqlite3"
        with processing_service(lambda path: (service_status, b"")) as (url, requests):
            if from_config:
                config = Path(directory) / "health.json"
                config.write_text(json.dumps({
                    "service_url": url, "mysql_dependency_urls": [mysql_url],
                }), encoding="utf-8")
                options = ["--config", str(config)]
            else:
                options = ["--service-url", url, "--mysql-dependency-url", mysql_url]
            if check_dependencies:
                options.append("--check-dependencies")
            monkeypatch.setattr(sys, "argv", [
                "opspilot", "--db", str(database), "diagnose-http", *options, "Check database",
            ])
            main()
        output = capsys.readouterr().out
        assert database.is_file()
    assert requests == ["/processing"]
    assert "ephemeral-secret" not in output
    report = json.loads(output)
    if service_status == 204 and not check_dependencies:
        assert queries == []
        assert len(report["evidence"]) == 1
    else:
        assert queries == [("SELECT 1 AS ok", ())]
        assert report["status"] == (
            "no_failure_observed" if service_status == 204 else "undetermined"
        )
        assert report["evidence"][1]["source"] == mysql_url
        assert report["evidence"][1]["health"] == "healthy"
