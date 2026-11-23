"""Produce fulfillment records in three isolated service databases."""

import argparse
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Literal

ORDER_ID = "ORD-0042"
OTHER_ORDER_ID = "ORD-0043"
Table = Literal["orders", "inventory_reservations", "shipments"]


def _record(database: Path, table: Table, order_id: str) -> None:
    with closing(sqlite3.connect(database)) as connection, connection:
        connection.execute(f"CREATE TABLE IF NOT EXISTS {table} (order_id TEXT PRIMARY KEY NOT NULL)")
        connection.execute(f"INSERT OR IGNORE INTO {table} (order_id) VALUES (?)", (order_id,))


def _dispatch(work: Path, order_id: str) -> None:
    stock_uri = (work / "inventory.sqlite3").absolute().as_uri()
    with closing(sqlite3.connect(f"{stock_uri}?mode=ro", uri=True)) as connection:
        reservation = connection.execute(
            "SELECT order_id FROM inventory_reservations WHERE order_id = ?", (order_id,),
        ).fetchone()
    if reservation is None:
        raise ValueError(f"Order {order_id} needs a stock reservation before dispatch.")
    _record(work / "shipping.sqlite3", "shipments", order_id)
    print(f"Dispatched {order_id}.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work", type=Path, required=True, help="Directory for the demo databases.")
    parser.add_argument("phase", choices=("seed", "restore-shipping", "ship"))
    args = parser.parse_args()

    if args.phase == "seed":
        args.work.mkdir(parents=True, exist_ok=True)
        if (args.work / "shipping.sqlite3").exists():
            parser.error("Use a fresh directory for a full demo. The shipping database already exists.")
        for order_id in (ORDER_ID, OTHER_ORDER_ID):
            _record(args.work / "orders.sqlite3", "orders", order_id)
            _record(args.work / "inventory.sqlite3", "inventory_reservations", order_id)
        print(f"Accepted {ORDER_ID} and {OTHER_ORDER_ID} and reserved their stock.")
        return

    try:
        _dispatch(args.work, OTHER_ORDER_ID if args.phase == "restore-shipping" else ORDER_ID)
    except (sqlite3.Error, ValueError) as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
