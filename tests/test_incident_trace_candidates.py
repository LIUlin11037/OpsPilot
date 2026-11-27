import json

from opspilot.connectors.jsonl_service_log import JsonlServiceLogConnector
from opspilot.core.incident_window import parse_incident_window


WINDOW = parse_incident_window("2024-01-01T00:00:00Z", "2024-01-01T00:01:30Z")
A, B, C = "a" * 32, "b" * 32, "c" * 32
D = "0123456789abcdef"


def row(second, **fields):
    return {"timestamp": f"2024-01-01T00:{second // 60:02d}:{second % 60:02d}Z",
            "service": "orders", "level": "ERROR", **fields}


def test_scoped_log_ranks_distinct_error_ids_by_latest_time_then_first_line(tmp_path):
    path = tmp_path / "service.jsonl"
    data = [
        row(0, trace_id=A), row(90, service="neighbor", trace_id="1" * 32),
        row(91, trace_id="2" * 32), row(90, level="INFO", trace_id="3" * 32),
        row(70, trace_id=B.upper()), row(85, trace_id=B, level=" critical "),
        row(85, trace_id=C), row(90, trace_id=D, level="FATAL"),
        row(68, trace_id=A), row(89), row(88, trace_id=123),
        row(87, service=None, trace_id="4" * 32),
        row(90, trace_id=("0" * 16 + D).upper()),
    ]
    path.write_text("\n".join(json.dumps(item) for item in data), encoding="utf-8")
    connector = JsonlServiceLogConnector(path)

    normal = connector.inspect_window(WINDOW, "orders")
    read = connector.inspect_window(WINDOW, "orders", include_trace_candidates=True)

    assert "trace_candidates" not in normal
    assert read["error_count"] == normal["error_count"] == 9
    assert read["matched_entries"] == normal["matched_entries"] == 10
    assert read["unscoped_entries"] == normal["unscoped_entries"] == 1
    assert read["trace_candidates"] == {
        "candidates": [
            {"trace_id": "0" * 16 + D, "line_number": 8, "level": "FATAL", "timestamp": "2024-01-01T00:01:30+00:00"},
            {"trace_id": B, "line_number": 6, "level": "CRITICAL", "timestamp": "2024-01-01T00:01:25+00:00"},
            {"trace_id": C, "line_number": 7, "level": "ERROR", "timestamp": "2024-01-01T00:01:25+00:00"},
        ],
        "missing_ids": 1, "invalid_ids": 1, "duplicate_rows": 3, "over_limit": 1,
    }


def test_errors_without_trace_ids_are_valid_legacy_rows(tmp_path):
    log = tmp_path / "service.jsonl"
    rows = [row(4), row(6, trace_id=None), row(7, trace_id="0" * 32), row(12, trace_id="not a trace ID")]
    log.write_text("\n".join(json.dumps(entry) for entry in rows), encoding="utf-8")

    read = JsonlServiceLogConnector(log).inspect_window(WINDOW, "orders", include_trace_candidates=True)

    assert read["outcome"] == "observed" and read["error_count"] == 4
    assert read["trace_candidates"] == {
        "candidates": [], "missing_ids": 2, "invalid_ids": 2, "duplicate_rows": 0, "over_limit": 0,
    }


def test_log_failure_does_not_publish_candidates_from_an_unverified_file(tmp_path):
    path = tmp_path / "service.jsonl"
    path.write_text(json.dumps(row(20, trace_id=A)) + "\n\nnot JSON", encoding="utf-8")

    read = JsonlServiceLogConnector(path).inspect_window(WINDOW, "orders", include_trace_candidates=True)

    assert read["outcome"] == "missing" and read["reason"] == "invalid_log_record"
    assert "trace_candidates" not in read
