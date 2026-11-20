import sys
from types import SimpleNamespace

import pytest

from opspilot.connectors.mysql import MySQLConnector, MySQLHealthConnector, MySQLRead


@pytest.mark.parametrize(
    ("code", "cause", "outcome", "reason"),
    [
        (1045, None, "missing", "authentication_failed"),
        (1142, None, "missing", "access_denied"),
        (2003, ConnectionRefusedError(), "missing", "transport_error"),
        (2013, TimeoutError(), "timeout", "request_timeout"),
        (1146, None, "missing", "query_error"),
    ],
)
def test_mysql_projects_driver_failure_without_its_raw_message(
    monkeypatch, code, cause, outcome, reason
) -> None:
    class DriverError(Exception):
        pass

    def connect(**kwargs):
        error = DriverError(code, "Raw driver message")
        if cause is not None:
            error.original_exception = cause
        raise error

    monkeypatch.setitem(sys.modules, "pymysql", SimpleNamespace(
        connect=connect, MySQLError=DriverError, cursors=SimpleNamespace(DictCursor=object()),
    ))

    read = MySQLConnector("127.0.0.1", 3306, "reader", "fixture").select("SELECT 1", ())

    assert read.outcome == outcome
    assert read.reason == reason
    assert read.mysql_error_code == code
    assert not read.rows
    assert "Raw driver message" not in repr(read)


@pytest.mark.parametrize(
    ("read", "outcome", "health", "reason", "error_code"),
    [
        (MySQLRead("observed", "fixture", ({"ok": 1},)), "observed", "healthy", None, None),
        (MySQLRead("observed", "fixture"), "missing", "unknown", "invalid_health_result", None),
        (
            MySQLRead("missing", "fixture", reason="authentication_failed", mysql_error_code=1045),
            "missing", "unknown", "authentication_failed", 1045,
        ),
        (
            MySQLRead("timeout", "fixture", reason="request_timeout"),
            "timeout", "unknown", "request_timeout", None,
        ),
    ],
)
def test_mysql_health_projects_a_read_only_constant_query(
    monkeypatch, read, outcome, health, reason, error_code
) -> None:
    def select(self, query, parameters):
        assert (self.host, self.port, self.username) == ("127.0.0.1", 13306, "reader")
        assert query == "SELECT 1 AS ok" and parameters == ()
        return read

    monkeypatch.setattr(MySQLConnector, "select", select)
    endpoint = "mysql://127.0.0.1:13306"
    evidence = MySQLHealthConnector(endpoint, "reader", "fixture").inspect_health()

    assert evidence["source"] == endpoint and evidence["observed_at"] == read.observed_at
    assert evidence["outcome"] == outcome and evidence["health"] == health
    assert evidence.get("reason") == reason and evidence.get("mysql_error_code") == error_code
    assert "rows" not in evidence


def test_mysql_health_rejects_credentials_in_endpoint() -> None:
    with pytest.raises(ValueError, match="mysql://HOST"):
        MySQLHealthConnector("mysql://reader:secret@127.0.0.1", "reader", "secret")
