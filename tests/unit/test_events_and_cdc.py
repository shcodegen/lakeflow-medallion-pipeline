from datetime import datetime

from pyspark.sql import Row

from lakeflow_pipeline.transforms import (
    build_scd2_versions,
    first_change_per_key,
    flatten_cdc,
    latest_change_per_key,
    transform_silver_events,
)


def event(event_id, user_id="U1", ingested=datetime(2025, 1, 1, 0, 0)):
    return Row(event_id=event_id, event_type="page_view", event_time="2025-01-01T10:00:00",
               platform="web",
               user=Row(user_id=user_id, session_id="s", device_type="mobile", browser="Chrome",
                        country="CA", city="Toronto"),
               product=Row(product_id="P001", product_name="Earbuds", category="Electronics",
                           price=49.99, in_stock=True),
               _ingest_timestamp=ingested)


def test_events_flattened_filtered_and_deduped(spark):
    df = spark.createDataFrame([
        event("E1", ingested=datetime(2025, 1, 1, 0, 0)),
        event("E1", ingested=datetime(2025, 1, 1, 0, 5)),
        event("E2", user_id=None),
    ])
    out = transform_silver_events(df).collect()
    assert [r.event_id for r in out] == ["E1"]
    assert out[0].user_id == "U1" and out[0].unit_price == 49.99
    assert "user" not in transform_silver_events(df).columns


def cdc(op, order_id, ts_ms, status="pending", amount=10.0):
    image = Row(order_id=order_id, customer_id="C1", status=status, total_amount=amount,
                updated_at="2025-06-01T00:00:00")
    return Row(op=op, ts_ms=ts_ms,
               before=None if op == "INSERT" else image,
               after=None if op == "DELETE" else image)


def changes(spark, rows):
    return flatten_cdc(spark.createDataFrame(rows, "op string, ts_ms long, "
        "before struct<order_id:string,customer_id:string,status:string,total_amount:double,updated_at:string>, "
        "after struct<order_id:string,customer_id:string,status:string,total_amount:double,updated_at:string>"))


def test_flatten_cdc_takes_key_from_before_on_delete(spark):
    out = {r.op: r for r in changes(spark, [cdc("INSERT", "A", 1000), cdc("DELETE", "B", 2000)]).collect()}
    assert out["INSERT"].order_id == "A" and out["DELETE"].order_id == "B"
    assert out["DELETE"].status is None


def test_latest_change_per_key_makes_merge_safe(spark):
    ch = changes(spark, [cdc("INSERT", "A", 1000, "pending"), cdc("UPDATE", "A", 3000, "shipped"),
                         cdc("UPDATE", "A", 2000, "completed")])
    out = latest_change_per_key(ch).collect()
    assert len(out) == 1 and out[0].status == "shipped"


def test_scd2_versions_chain_and_delete_closes(spark):
    ch = changes(spark, [cdc("INSERT", "A", 1000, "pending"), cdc("UPDATE", "A", 2000, "shipped"),
                         cdc("DELETE", "A", 3000), cdc("INSERT", "B", 1500)])
    v = {(r.order_id, r.status): r for r in build_scd2_versions(ch).collect()}
    assert len(v) == 3  # A pending, A shipped, B pending (DELETE is not a version)
    a1, a2, b = v[("A", "pending")], v[("A", "shipped")], v[("B", "pending")]
    assert a1.effective_end == a2.effective_start and not a1.is_current
    assert a2.effective_end is not None and not a2.is_current  # closed by the DELETE
    assert b.is_current and b.effective_end is None


def test_first_change_per_key(spark):
    ch = changes(spark, [cdc("UPDATE", "A", 5000), cdc("UPDATE", "A", 2000)])
    r = first_change_per_key(ch).collect()[0]
    assert r.first_change_ts == datetime(1970, 1, 1, 0, 0, 2)
