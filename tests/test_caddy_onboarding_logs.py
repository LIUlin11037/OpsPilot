import json
import subprocess
import sys
from pathlib import Path


MAPPER = Path(__file__).resolve().parents[1] / "examples" / "caddy-onboarding" / "map_access_logs.py"
TRACE = "a" * 32


def run(source, destination):
    return subprocess.run(
        [sys.executable, str(MAPPER), str(source), str(destination), "--service", "caddy-gateway"],
        capture_output=True, encoding="utf-8", timeout=10,
    )


def test_maps_only_native_access_fields_and_keeps_missing_trace_distinct(tmp_path):
    source, output = tmp_path / "raw.jsonl", tmp_path / "mapped.jsonl"
    rows = [
        {"logger": "http", "ts": 0, "msg": "started"},
        {"logger": "http.log.access.log0", "ts": 1.125, "status": 503, "traceID": TRACE, "request": {"headers": {}}},
        {"logger": "http.log.access.log0", "ts": 2, "status": 200},
    ]
    source.write_text("\n".join(json.dumps(row) for row in rows), encoding="utf-8")
    result = run(source, output)
    assert result.returncode == 0, result.stderr
    assert [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()] == [
        {"timestamp": "1970-01-01T00:00:01.125000+00:00", "service": "caddy-gateway", "level": "ERROR",
         "http_status": 503, "trace_id": TRACE},
        {"timestamp": "1970-01-01T00:00:02+00:00", "service": "caddy-gateway", "level": "INFO", "http_status": 200},
    ]


def test_rejects_malformed_access_row_without_overwriting_an_existing_export(tmp_path):
    source, output = tmp_path / "raw.jsonl", tmp_path / "mapped.jsonl"
    source.write_text('{"logger":"http"}\n{"logger":"http.log.access.log0","status":503}\n', encoding="utf-8")
    output.write_text("existing snapshot", encoding="utf-8")
    result = run(source, output)
    assert result.returncode == 2 and "line 2" in result.stderr
    assert output.read_text(encoding="utf-8") == "existing snapshot"
    assert "different paths" in run(source, source).stderr
