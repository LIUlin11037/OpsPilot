"""Read rows through the optional PyMySQL driver."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from math import ceil, isfinite
from typing import Any, Literal
from urllib.parse import SplitResult, urlsplit

from opspilot.core.diagnosis import HealthRead, ObservationOutcome

MySQLFailureReason = Literal[
    "authentication_failed", "access_denied", "transport_error", "request_timeout", "query_error"
]
_MYSQL_ERROR_REASONS: dict[int, MySQLFailureReason] = {
    1044: "access_denied",
    1045: "authentication_failed",
    1142: "access_denied",
    1143: "access_denied",
    2002: "transport_error",
    2003: "transport_error",
    2005: "transport_error",
    2006: "transport_error",
    2013: "transport_error",
}


@dataclass(frozen=True)
class MySQLRead:
    outcome: ObservationOutcome
    observed_at: str
    rows: tuple[dict[str, Any], ...] = ()
    reason: MySQLFailureReason | None = None
    mysql_error_code: int | None = None


@dataclass(frozen=True)
class MySQLConnector:
    host: str
    port: int
    username: str
    password: str = field(repr=False)
    timeout_seconds: float = 5.0

    def __post_init__(self) -> None:
        if not self.host or not self.username or not 1 <= self.port <= 65535:
            raise ValueError("MySQL requires a host, username and a port from 1 to 65535.")
        if not isfinite(self.timeout_seconds) or not 0 < self.timeout_seconds <= 31_536_000:
            raise ValueError("MySQL timeout must be positive and at most 31536000 seconds.")

    def select(self, query: str, parameters: tuple[int | str, ...]) -> MySQLRead:
        try:
            import pymysql
        except ModuleNotFoundError as error:
            if error.name != "pymysql":
                raise
            raise ValueError('Install MySQL support with pip install ".[hirehub]".') from error

        def timestamp() -> str:
            return datetime.now(timezone.utc).isoformat()

        try:
            with pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                charset="utf8mb4",
                autocommit=True,
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=ceil(self.timeout_seconds),
                read_timeout=self.timeout_seconds,
                write_timeout=self.timeout_seconds,
            ) as connection, connection.cursor() as cursor:
                cursor.execute(query, parameters)
                rows = tuple(cursor.fetchall())
        except TimeoutError:
            return MySQLRead("timeout", timestamp(), reason="request_timeout")
        except pymysql.MySQLError as error:
            cause = getattr(error, "original_exception", None) or error.__context__
            code = error.args[0] if error.args and type(error.args[0]) is int else None
            if isinstance(cause, TimeoutError):
                return MySQLRead(
                    "timeout", timestamp(), reason="request_timeout", mysql_error_code=code
                )
            reason = (
                _MYSQL_ERROR_REASONS.get(code, "query_error") if code is not None else "query_error"
            )
            return MySQLRead("missing", timestamp(), reason=reason, mysql_error_code=code)
        return MySQLRead("observed", timestamp(), rows)


def mysql_health_endpoint(endpoint: str) -> SplitResult:
    url = urlsplit(endpoint)
    if (
        url.scheme != "mysql"
        or not url.hostname
        or url.username is not None
        or url.path not in ("", "/")
        or url.query
        or url.fragment
    ):
        raise ValueError("Use a MySQL health URL in the form mysql://HOST[:PORT].")
    return url


@dataclass(frozen=True)
class MySQLHealthConnector:
    endpoint: str
    username: str
    password: str = field(repr=False)
    timeout_seconds: float = 5.0
    _mysql: MySQLConnector = field(init=False, repr=False)

    def __post_init__(self) -> None:
        url = mysql_health_endpoint(self.endpoint)
        port = 3306 if url.port is None else url.port
        object.__setattr__(
            self, "_mysql",
            MySQLConnector(url.hostname, port, self.username, self.password, self.timeout_seconds),
        )

    def inspect_health(self) -> HealthRead:
        read = self._mysql.select("SELECT 1 AS ok", ())
        evidence: HealthRead = {
            "source": self.endpoint,
            "observed_at": read.observed_at,
            "outcome": read.outcome,
            "health": "unknown",
        }
        if read.outcome == "observed":
            if read.rows == ({"ok": 1},):
                evidence["health"] = "healthy"
            else:
                evidence["outcome"] = "missing"
                evidence["reason"] = "invalid_health_result"
        else:
            if read.reason is not None:
                evidence["reason"] = read.reason
            if read.mysql_error_code is not None:
                evidence["mysql_error_code"] = read.mysql_error_code
        return evidence
