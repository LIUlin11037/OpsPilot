import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
from contextlib import contextmanager
from urllib.parse import urlencode

import pytest


EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "diagnostic-regression"


def run_regression(tmp_path, *args):
    environment = {
        key: value for key, value in os.environ.items()
        if not key.startswith(("OPSPILOT_", "LANGCHAIN_", "LANGSMITH_"))
    }
    return subprocess.run(
        [sys.executable, str(EXAMPLE / "run.py"), *map(str, args)],
        cwd=tmp_path, env=environment, capture_output=True, encoding="utf-8", timeout=90,
    )


def test_regression_runs_all_cases_from_an_empty_directory(tmp_path):
    result = run_regression(tmp_path)

    assert result.stdout, result.stderr
    verdict = json.loads(result.stdout)
    failures = {
        case["id"]: case.get("error") or [check for check in case["checks"] if not check["passed"]]
        for case in verdict["cases"] if not case["passed"]
    }
    assert result.returncode == 0, failures
    assert verdict["passed"] is True
    cases = {case["id"]: case for case in verdict["cases"]}
    assert set(cases) == {
        "healthy", "http-fault", "recovered-history", "history-source-timeout",
        "scope-isolation", "unrelated-message",
    }
    assert all(case["passed"] and case["checks"] for case in cases.values())
    assert [case["origin"] for case in cases.values()].count("traefik") == 3
    observed = {check["field"]: check["actual"] for check in cases["recovered-history"]["checks"]}
    assert [observed[f"incident.anomaly_evidence_indexes.{i}"] for i in range(3)] == [1, 2, 3]
    assert list(tmp_path.iterdir()) == []


def test_regression_detects_a_wrong_expectation_after_diagnosis(tmp_path):
    healthy, *_ = json.loads((EXAMPLE / "cases.json").read_text(encoding="utf-8"))
    healthy["expected"].update(status="undetermined", missing_field=None, evidence=[])
    override = tmp_path / "incorrect-expectations.json"
    override.write_text(json.dumps([healthy]), encoding="utf-8")

    result = run_regression(tmp_path, "--cases", override)

    assert result.returncode == 1, result.stderr or result.stdout
    verdict = json.loads(result.stdout)
    assert verdict["passed"] is False
    case, = verdict["cases"]
    assert case["id"] == "healthy"
    assert "error" not in case
    failed = {check["field"]: check for check in case["checks"] if not check["passed"]}
    assert set(failed) == {"status", "missing_field", "evidence.length"}
    assert failed["status"]["expected"] == "undetermined"
    assert failed["status"]["actual"] == "no_failure_observed"
    assert failed["missing_field"]["expected"] is None
    assert failed["missing_field"]["actual_present"] is False
    assert failed["evidence.length"]["actual"] == 1


def test_regression_reports_case_when_diagnosis_command_fails(tmp_path):
    cases = json.loads((EXAMPLE / "cases.json").read_text(encoding="utf-8"))
    case = next(item for item in cases if item["id"] == "recovered-history")
    case["inputs"]["window"][0] = "not-a-timestamp"
    override = tmp_path / "invalid-input.json"
    override.write_text(json.dumps([case]), encoding="utf-8")

    result = run_regression(tmp_path, "--cases", override)

    assert result.returncode == 1, result.stderr or result.stdout
    case, = json.loads(result.stdout)["cases"]
    assert case["id"] == "recovered-history"
    assert case["passed"] is False
    assert "--incident-start" in case["error"]


@pytest.mark.parametrize("wrong_selector", ['{job="neighbor"}', '{status=~"5.."}', ""])
def test_fixture_rejects_queries_with_any_wrongly_scoped_selector(wrong_selector):
    runner = runpy.run_path(str(EXAMPLE / "run.py"))
    connected = runner["FixtureServer"]({"metrics": {"error_rate": [[0, "0.5"]]}})
    metric = "traefik_service_requests_total"
    query = f'sum(rate({metric}{wrong_selector}[1m])) / sum(rate({metric}{{job="orders"}}[1m]))'
    path = "/api/v1/query_range?" + urlencode({"query": query, "start": 5, "end": 35, "step": 5})

    with connected, pytest.raises(ValueError, match="scope"):
        connected.reply(path)


@pytest.mark.parametrize("stage", ["creation", "cleanup"])
def test_regression_reports_temp_failure_and_continues(stage, tmp_path, monkeypatch, capsys):
    runner = runpy.run_path(str(EXAMPLE / "run.py"))
    main = runner["main"]
    original_directory = main.__globals__["TemporaryDirectory"]
    healthy, *_ = json.loads((EXAMPLE / "cases.json").read_text(encoding="utf-8"))
    cases = tmp_path / "two-cases.json"
    cases.write_text(json.dumps([healthy, {**healthy, "id": "second-healthy"}]), encoding="utf-8")
    attempts = 0

    @contextmanager
    def case_directory(*, prefix):
        nonlocal attempts
        attempts += 1
        first = attempts == 1
        if first and stage == "creation":
            raise PermissionError("Fixture directory creation denied")
        with original_directory(prefix=prefix, dir=tmp_path) as directory:
            yield directory
        if first and stage == "cleanup":
            raise PermissionError("Fixture directory cleanup denied")

    monkeypatch.setitem(main.__globals__, "TemporaryDirectory", case_directory)

    assert main(["--cases", str(cases)]) == 1
    first, second = json.loads(capsys.readouterr().out)["cases"]
    assert first["id"] == "healthy" and first["passed"] is False
    assert stage in first["error"]
    assert second["id"] == "second-healthy" and second["passed"] is True
