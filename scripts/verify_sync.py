"""Field-by-field reconciliation of PostgreSQL (source) and MongoDB (read model).

Usage (stack running):  uv run python scripts/verify_sync.py
Exit code 0 = fully synchronized, 1 = differences found.
"""

from __future__ import annotations

import os
import sys
from decimal import Decimal

import psycopg
from pymongo import MongoClient

PG_DSN = os.getenv("VERIFY_PG_DSN", "postgresql://catalog:catalog_pwd@127.0.0.1:5432/catalog")
MONGO_URI = os.getenv("VERIFY_MONGO_URI", "mongodb://root:root_pwd@127.0.0.1:27017/?authSource=admin")
MONGO_DB = os.getenv("VERIFY_MONGO_DB", "product_catalog")
FIELDS = ("sku", "name", "category", "price", "quantity")


def load_sql() -> dict[int, dict]:
    with psycopg.connect(PG_DSN) as conn:
        rows = conn.execute(
            "SELECT id, sku, name, category, price, quantity, updated_at FROM products"
        ).fetchall()
    return {r[0]: dict(zip((*FIELDS, "updated_at"), r[1:], strict=True)) for r in rows}


def load_mongo() -> dict[int, dict]:
    with MongoClient(MONGO_URI, tz_aware=True, serverSelectionTimeoutMS=3000) as client:
        docs = client[MONGO_DB]["products"].find({})
        return {
            d["_id"]: {
                **{f: d[f] for f in FIELDS},
                "price": d["price"].to_decimal(),
                "updated_at": d["updatedAt"],
            }
            for d in docs
        }


def main() -> int:
    sql, mongo = load_sql(), load_mongo()
    problems: list[str] = []
    for product_id in sorted(sql.keys() - mongo.keys()):
        problems.append(f"id={product_id}: missing in MongoDB")
    for product_id in sorted(mongo.keys() - sql.keys()):
        problems.append(f"id={product_id}: exists only in MongoDB")
    for product_id in sorted(sql.keys() & mongo.keys()):
        source, target = sql[product_id], mongo[product_id]
        for field in FIELDS:
            left, right = source[field], target[field]
            if field == "price":
                left, right = Decimal(left), Decimal(right)
            if left != right:
                problems.append(f"id={product_id}: {field} SQL={left!r} Mongo={right!r}")
        drift_ms = abs((source["updated_at"] - target["updated_at"]).total_seconds()) * 1000
        if drift_ms >= 1:
            problems.append(f"id={product_id}: updatedAt differs by {drift_ms:.0f} ms")

    print(f"PostgreSQL rows: {len(sql)}  |  MongoDB documents: {len(mongo)}")
    if problems:
        print(f"NOT synchronized ({len(problems)} difference(s)):")
        print("\n".join(f"  - {p}" for p in problems))
        return 1
    print("OK: every SQL row has exactly one identical MongoDB document.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
