"""Read fulfillment checkpoints from service-owned SQLite databases."""

import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from opspilot.core.diagnosis import TraceEvidence
from opspilot.core.target_trace import TargetTraceRead

_Table = Literal["orders", "inventory_reservations", "shipments"]


@dataclass(frozen=True)
class OrderTraceAdapter:
    environment: str
    orders_db: Path
    inventory_db: Path
    shipping_db: Path

    def _checkpoints(self) -> tuple[tuple[str, _Table, Path], ...]:
        return (
            ("order_accepted", "orders", self.orders_db),
            ("stock_reserved", "inventory_reservations", self.inventory_db),
            ("shipment_created", "shipments", self.shipping_db),
        )

    @property
    def branches(self) -> tuple[str, ...]:
        return tuple(branch for branch, _, _ in self._checkpoints())

    def sources(self) -> dict[str, str]:
        return {
            branch: f"{database.absolute().as_uri()}#{table}"
            for branch, table, database in self._checkpoints()
        }

    def inspect_trace(self, entity_id: str) -> TargetTraceRead:
        checks = [
            _check_order(database.absolute().as_uri(), table, branch, entity_id)
            for branch, table, database in self._checkpoints()
        ]
        return {
            "environment": self.environment,
            "vhost": None,
            "message_id": None,
            "outcome": "observed" if any(check["outcome"] == "observed" for check in checks) else "missing",
            "checks": checks,
        }


def _check_order(uri: str, table: _Table, branch: str, order_id: str) -> TraceEvidence:
    count: int | None
    try:
        with closing(sqlite3.connect(f"{uri}?mode=ro", uri=True, timeout=1.0)) as connection:
            # Table names are assigned by the adapter. Bind the exact ID for each lookup.
            row = connection.execute(
                f"SELECT COUNT(*) FROM {table} WHERE order_id COLLATE BINARY = ?", (order_id,),
            ).fetchone()
            count = row[0]
    except sqlite3.Error:
        count = None

    evidence: TraceEvidence = {
        "tool": "inspect_target_trace",
        "checkpoint": branch,
        "branch": branch,
        "origin": table,
        "source": f"{uri}#{table}",
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "outcome": "missing" if count is None else "observed",
    }
    if count is None:
        evidence["reason"] = "source_query_failed"
    else:
        evidence["row_count"] = count
    return evidence
