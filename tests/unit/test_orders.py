from datetime import date, datetime

import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

from lakeflow_pipeline.transforms import (
    build_customer_segments,
    build_daily_revenue,
    cast_order_columns,
    dedup_latest,
    filter_valid_orders,
    transform_silver_orders,
)


def order(order_id, customer_id="C1", qty="1", total="10.0", status="completed",
          channel="web", date_="2025-03-15 10:00:00", ingested=datetime(2025, 3, 15, 10, 1)):
    return Row(order_id=order_id, customer_id=customer_id, product_id="P001", category="Sports",
               quantity=qty, unit_price="10.0", total_amount=total, order_date=date_,
               status=status, payment_type="paypal", channel=channel, _ingest_timestamp=ingested)


@pytest.fixture
def bronze(spark):
    return spark.createDataFrame([
        order("O1", total="99.98", ingested=datetime(2025, 3, 15, 10, 1)),
        order("O1", total="99.98", ingested=datetime(2025, 3, 15, 10, 5)),        # duplicate, later
        order("O2", total="600", status="shipped", channel="mobile_app"),
        order("O3", total="387", status="pending", channel="marketplace"),
        order(None, total="40"),                                                  # null key
        order("O5", customer_id=None, total="20"),                                # null customer
        order("O6", total="-51"),                                                 # negative
        order("O7", qty="0", total="10"),                                         # zero quantity
    ])


def test_cast_sets_types_and_keeps_rows(bronze):
    out = cast_order_columns(bronze)
    assert out.schema["quantity"].dataType == IntegerType()
    assert out.schema["total_amount"].dataType == DoubleType()
    assert out.schema["order_date"].dataType == TimestampType()
    assert out.count() == bronze.count()


def test_filter_drops_every_invalid_row(bronze):
    ids = sorted(r.order_id for r in filter_valid_orders(cast_order_columns(bronze)).collect())
    assert ids == ["O1", "O1", "O2", "O3"]


def test_dedup_keeps_latest_ingest(bronze):
    out = dedup_latest(bronze.filter("order_id = 'O1'"), "order_id", "_ingest_timestamp").collect()
    assert len(out) == 1 and out[0]._ingest_timestamp == datetime(2025, 3, 15, 10, 5)


def test_silver_business_rules(bronze):
    rows = {r.order_id: r for r in transform_silver_orders(bronze).collect()}
    assert set(rows) == {"O1", "O2", "O3"}
    assert (rows["O1"].revenue_band, rows["O2"].revenue_band, rows["O3"].revenue_band) == ("LOW", "HIGH", "MEDIUM")
    assert rows["O2"].is_fulfilled and not rows["O3"].is_fulfilled
    assert (rows["O1"].channel_group, rows["O3"].channel_group) == ("DIGITAL", "MARKETPLACE")
    assert (rows["O1"].order_year, rows["O1"].order_quarter) == (2025, 1)


def test_daily_revenue_conserves_orders_and_revenue(bronze):
    silver = transform_silver_orders(bronze)
    gold = build_daily_revenue(silver)
    assert gold.agg(F.sum("order_count")).first()[0] == silver.count()
    assert round(gold.agg(F.sum("total_revenue")).first()[0], 2) == round(
        silver.agg(F.sum("total_amount")).first()[0], 2)


def test_rfm_segments_are_deterministic_and_complete(spark):
    rows = [order(f"O{i}", customer_id=f"C{i % 10}", total=str(10 + i),
                  date_=f"2025-0{1 + i % 9}-01 00:00:00") for i in range(50)]
    silver = transform_silver_orders(spark.createDataFrame(rows))
    seg = build_customer_segments(silver, as_of_date=date(2025, 12, 31))
    assert seg.count() == 10  # one row per customer
    assert set(r.customer_segment for r in seg.collect()) <= {"Champion", "Loyal", "Potential", "At Risk", "Lost"}
    assert seg.filter("recency_days < 0").count() == 0


def test_unparseable_values_become_null_and_are_dropped_even_in_ansi_mode(spark):
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        df = spark.createDataFrame([order("O1", qty="two"), order("O2", total="n/a"),
                                    order("O3", date_="not a date"), order("O4")])
        assert [r.order_id for r in transform_silver_orders(df).collect()] == ["O4"]
    finally:
        spark.conf.unset("spark.sql.ansi.enabled")
