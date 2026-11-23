import json
import sqlite3
import subprocess
import sys
from contextlib import closing
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from opspilot.__main__ import main
from opspilot.adapters.order_trace import OrderTraceAdapter


ORDER_ID = "ORD-0042"


def _write_rows(path, table, *order_ids):
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute(f"CREATE TABLE {table} (order_id TEXT COLLATE NOCASE NOT NULL)")
        connection.executemany(f"INSERT INTO {table} VALUES (?)", [(order_id,) for order_id in order_ids])


@pytest.fixture
def order_directory():
    with TemporaryDirectory(prefix="opspilot-order-") as directory:
        yield Path(directory)


@pytest.fixture
def order_target(order_directory):
    path = order_directory / "target.json"
    path.write_text(json.dumps({
        "order": {
            "environment": "fulfillment-test",
            "orders_db": "orders.sqlite3",
            "inventory_db": "inventory.sqlite3",
            "shipping_db": "shipping #1.sqlite3",
        },
    }), encoding="utf-8")
    return path


def _cli(config, *args, returncode=0):
    db = config.with_name("reports.sqlite3")
    result = subprocess.run(
        [sys.executable, "-m", "opspilot", "--db", str(db), *args],
        capture_output=True, encoding="utf-8", timeout=20,
    )
    assert result.returncode == returncode, result.stderr
    return result


def test_order_plan_lists_sources_without_opening_them(order_target, monkeypatch, capsys):
    def unexpected_read(*args):
        pytest.fail("Plan preview attempted a database connection")

    monkeypatch.setattr(sqlite3, "connect", unexpected_read)
    db = order_target.with_name("reports.sqlite3")
    monkeypatch.setattr(sys, "argv", [
        "opspilot", "--db", str(db), "diagnose-auto", "--config", str(order_target),
        "--order-id", ORDER_ID, "--plan", "Was this order shipped?",
    ])
    main()
    plan = json.loads(capsys.readouterr().out)
    assert plan["routing"] == {"workflow": "target_trace", "matched_terms": [], "selected_by": "entity_id"}
    assert plan["scope"] == {
        "environment": "fulfillment-test", "entity_id": ORDER_ID,
        "order_accepted": f"{order_target.with_name('orders.sqlite3').as_uri()}#orders",
        "stock_reserved": f"{order_target.with_name('inventory.sqlite3').as_uri()}#inventory_reservations",
        "shipment_created": f"{order_target.with_name('shipping #1.sqlite3').as_uri()}#shipments",
    }
    assert not db.exists()


@pytest.mark.parametrize(("shipping_rows", "status", "outcome", "count"), [
    (None, "unknown", "missing", None),
    (("ORD-0043", "ord-0042"), "not_observed", "observed", 0),
    ((ORDER_ID, "ORD-0043", "ord-0042"), "observed", "observed", 1),
])
def test_order_cli_saves_exact_checkpoint_observations(
    order_target, shipping_rows, status, outcome, count,
):
    orders = order_target.with_name("orders.sqlite3")
    inventory = order_target.with_name("inventory.sqlite3")
    shipping = order_target.with_name("shipping #1.sqlite3")
    _write_rows(orders, "orders", ORDER_ID, "ORD-0043")
    _write_rows(inventory, "inventory_reservations", ORDER_ID, "ORD-0043")
    if shipping_rows is not None:
        _write_rows(shipping, "shipments", *shipping_rows)
    originals = {path: path.read_bytes() for path in (orders, inventory, shipping) if path.exists()}

    report = json.loads(_cli(
        order_target, "diagnose-auto", "--config", str(order_target),
        "--order-id", ORDER_ID, "Was this order shipped?",
    ).stdout)
    assert report["target_trace"]["entity_id"] == ORDER_ID
    assert report["target_trace"]["branches"] == {
        "order_accepted": "observed", "stock_reserved": "observed", "shipment_created": status,
    }
    assert report["tool_calls"][0]["outcome"] == "observed"
    for observation in report["evidence"]:
        assert report["planned_scope"][observation["branch"]] == observation["source"]
        assert datetime.fromisoformat(observation["observed_at"]).utcoffset() is not None
    accepted, reserved, shipped = report["evidence"]
    assert accepted["row_count"] == reserved["row_count"] == 1
    assert shipped["outcome"] == outcome
    assert shipped.get("row_count") == count
    assert shipped.get("reason") == ("source_query_failed" if shipping_rows is None else None)

    assert json.loads(_cli(order_target, "report", report["task_id"]).stdout) == report
    markdown = _cli(order_target, "report", report["task_id"], "--format", "markdown").stdout
    assert f"**Entity ID** {ORDER_ID}" in markdown
    assert r"**shipment\_created branch** " + status.replace("_", r"\_") in markdown
    assert {path: path.read_bytes() for path in originals} == originals
    assert shipping.exists() == (shipping_rows is not None)


def test_order_trace_keeps_later_evidence_after_a_query_error(order_directory):
    orders = order_directory / "orders.sqlite3"
    inventory = order_directory / "inventory.sqlite3"
    shipping = order_directory / "shipping.sqlite3"
    _write_rows(orders, "orders", ORDER_ID)
    _write_rows(inventory, "unexpected_table", ORDER_ID)
    _write_rows(shipping, "shipments", ORDER_ID)

    trace = OrderTraceAdapter("fulfillment-test", orders, inventory, shipping).inspect_trace(ORDER_ID)
    assert trace["outcome"] == "observed"
    assert [item["outcome"] for item in trace["checks"]] == ["observed", "missing", "observed"]
    assert trace["checks"][1]["reason"] == "source_query_failed"
    assert "row_count" not in trace["checks"][1]
    assert trace["checks"][2]["row_count"] == 1


@pytest.mark.parametrize(("options", "message"), [
    (("--order-id", " "), "--order-id must not be empty"),
    (("--order-id", ORDER_ID, "--workflow", "http_health"), "cannot be combined with --workflow"),
    (("--order-id", ORDER_ID), "requires --config with an order section"),
    (("--order-id", ORDER_ID, "--message-id", "message-1"), "not allowed with argument --order-id"),
])
def test_order_cli_rejects_ambiguous_or_incomplete_scope(order_target, options, message):
    result = _cli(order_target, "diagnose-auto", *options, "Was this order shipped?", returncode=2)
    assert message in result.stderr
