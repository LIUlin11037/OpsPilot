import sys
from types import SimpleNamespace

import pytest

from opspilot.connectors.mysql import MySQLConnector


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
