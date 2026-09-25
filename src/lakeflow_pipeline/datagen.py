"""Synthetic e-commerce data (orders CSV, clickstream NDJSON, product catalog,
Debezium-style CDC). Pure Python + Faker: returns file contents as strings, so
notebooks/00_setup writes them to Unity Catalog volumes and tests can inspect
them locally.
"""
from __future__ import annotations

import json
import random
from datetime import datetime, timedelta

from faker import Faker

from lakeflow_pipeline.transforms import CHANNELS, EVENT_TYPES, ORDER_STATUSES, PAYMENT_TYPES

PRODUCTS = [
    ("P001", "Wireless Earbuds", "Electronics", 49.99),
    ("P002", "Running Shoes", "Sports", 89.95),
    ("P003", "Coffee Maker", "Appliances", 129.00),
    ("P004", "Python Cookbook", "Books", 39.99),
    ("P005", "Yoga Mat", "Sports", 25.50),
    ("P006", "Mechanical Keyboard", "Electronics", 149.99),
    ("P007", "Protein Powder", "Health", 55.00),
    ("P008", "LED Desk Lamp", "Office", 34.95),
    ("P009", "Water Bottle", "Sports", 18.99),
    ("P010", "Bluetooth Speaker", "Electronics", 79.99),
]

ORDER_COLUMNS = [
    "order_id", "customer_id", "product_id", "product_name", "category", "quantity",
    "unit_price", "total_amount", "order_date", "status", "payment_type", "channel",
    "customer_email", "customer_city", "customer_state",
]


class DataGenerator:
    """Seeded, so every run produces the same files."""

    def __init__(self, seed: int = 42):
        self.rng = random.Random(seed)
        self.fake = Faker("en_US")
        self.fake.seed_instance(seed)

    # ------------------------------------------------------------------ orders
    def orders_csv(self, n: int, start: datetime, id_offset: int = 0,
                   with_discount: bool = False) -> str:
        """Pipe-delimited CSV with a header. `with_discount` adds a
        `discount_pct` column, to exercise Auto Loader schema evolution."""
        cols = ORDER_COLUMNS + (["discount_pct"] if with_discount else [])
        lines = ["|".join(cols)]
        for i in range(n):
            pid, name, cat, price = self.rng.choice(PRODUCTS)
            qty = self.rng.randint(1, 5)
            unit = round(price * self.rng.uniform(0.85, 1.15), 2)
            ts = start + timedelta(days=self.rng.randint(0, 89), hours=self.rng.randint(0, 23))
            row = [
                f"ORD{id_offset + i + 1:07d}", f"CUST{self.rng.randint(1, 5000):06d}", pid, name, cat,
                qty, unit, round(qty * unit, 2), ts.strftime("%Y-%m-%d %H:%M:%S"),
                self.rng.choice(ORDER_STATUSES), self.rng.choice(PAYMENT_TYPES), self.rng.choice(CHANNELS),
                self.fake.email(), self.fake.city(), self.fake.state_abbr(),
            ]
            if with_discount:
                row.append(self.rng.choice([0, 5, 10, 15, 20]))
            lines.append("|".join(str(v) for v in row))
        return "\n".join(lines) + "\n"

    # ------------------------------------------------------------------ events
    def events_ndjson(self, n: int, start: datetime, id_offset: int = 0) -> str:
        lines = []
        for i in range(n):
            pid, name, cat, price = self.rng.choice(PRODUCTS)
            lines.append(json.dumps({
                "event_id": f"EVT{id_offset + i:09d}",
                "event_type": self.rng.choice(EVENT_TYPES),
                "event_time": (start + timedelta(seconds=self.rng.randint(0, 86400 * 30))).isoformat(),
                "platform": self.rng.choice(["web", "ios_app", "android_app"]),
                "user": {
                    "user_id": f"USR{self.rng.randint(1, 10000):07d}",
                    "session_id": self.fake.uuid4(),
                    "device_type": self.rng.choice(["mobile", "desktop", "tablet"]),
                    "browser": self.rng.choice(["Chrome", "Safari", "Firefox", "Edge"]),
                    "country": self.fake.country_code(),
                    "city": self.fake.city(),
                },
                "product": {
                    "product_id": pid, "product_name": name, "category": cat,
                    "price": price, "in_stock": self.rng.random() > 0.25,
                },
                "search_query": " ".join(self.fake.words(nb=self.rng.randint(1, 4)))
                if self.rng.random() > 0.6 else None,
                "referrer": self.rng.choice(["google", "facebook", "direct", "email", None]),
            }))
        return "\n".join(lines) + "\n"

    # ------------------------------------------------------------------ catalog
    def product_rows(self) -> list[dict]:
        rows = []
        for pid, name, cat, price in PRODUCTS:
            for _ in range(self.rng.randint(1, 3)):
                rows.append({
                    "product_id": pid, "product_name": name, "category": cat, "base_price": price,
                    "supplier_id": f"SUP{self.rng.randint(100, 999)}",
                    "supplier_name": self.fake.company(),
                    "warehouse_qty": self.rng.randint(0, 500),
                    "reorder_point": self.rng.randint(20, 100),
                    "lead_days": self.rng.randint(3, 30),
                    "is_active": self.rng.random() > 0.25,
                    "last_updated": (datetime(2025, 12, 31) - timedelta(days=self.rng.randint(0, 180))).strftime("%Y-%m-%d"),
                    "weight_kg": round(self.rng.uniform(0.1, 10.0), 2),
                    "country_origin": self.fake.country_code(),
                })
        return rows

    # ------------------------------------------------------------------ CDC
    def cdc_ndjson(self, n: int, start: datetime, key_space: int = 2_000) -> str:
        """Debezium-style change events (op, ts_ms, before, after, source) for an
        `orders` OLTP table. Timestamps strictly increase within a file."""
        lines = []
        ts = start
        for _ in range(n):
            ts += timedelta(milliseconds=self.rng.randint(1, 10_000))
            op = self.rng.choices(["INSERT", "UPDATE", "DELETE"], weights=[50, 35, 15])[0]
            order_id = f"ORD{self.rng.randint(1, key_space):07d}"
            image = lambda: {  # noqa: E731
                "order_id": order_id,
                "customer_id": f"CUST{self.rng.randint(1, 5000):06d}",
                "status": self.rng.choice(ORDER_STATUSES),
                "total_amount": round(self.rng.uniform(10, 500), 2),
                "updated_at": ts.isoformat(),
            }
            lines.append(json.dumps({
                "op": op,
                "ts_ms": int(ts.timestamp() * 1000),
                "before": None if op == "INSERT" else image(),
                "after": None if op == "DELETE" else image(),
                "source": {"db": "ecommerce_oltp", "schema": "public", "table": "orders"},
            }))
        return "\n".join(lines) + "\n"
