"""MERGE logic against real (open-source) Delta Lake.

Needs delta-spark (see tests/conftest.py); skipped when Delta is unavailable.
"""
import pytest
from pyspark.sql import Row

pytest.importorskip("delta")

from lakeflow_pipeline.cdc import apply_scd1, apply_scd2  # noqa: E402
from lakeflow_pipeline.transforms import flatten_cdc  # noqa: E402

SCHEMA = ("op string, ts_ms long, "
          "before struct<order_id:string,customer_id:string,status:string,total_amount:double,updated_at:string>, "
          "after struct<order_id:string,customer_id:string,status:string,total_amount:double,updated_at:string>")


def cdc(op, order_id, ts_ms, status="pending"):
    image = Row(order_id=order_id, customer_id="C1", status=status, total_amount=10.0,
                updated_at="2025-06-01T00:00:00")
    return Row(op=op, ts_ms=ts_ms, before=None if op == "INSERT" else image,
               after=None if op == "DELETE" else image)


def changes(spark, rows):
    return flatten_cdc(spark.createDataFrame(rows, SCHEMA))


def test_scd1_latest_wins_deletes_are_soft_and_replay_is_safe(delta_enabled):
    t = "scd1_test"
    delta_enabled.sql(f"DROP TABLE IF EXISTS {t}")
    batch1 = [cdc("INSERT", "A", 1000), cdc("UPDATE", "A", 2000, "shipped"),
              cdc("INSERT", "B", 1500), cdc("DELETE", "C", 1600)]  # delete of unknown key: ignored
    apply_scd1(delta_enabled, changes(delta_enabled, batch1), t)
    rows = {r.order_id: r for r in delta_enabled.table(t).collect()}
    assert set(rows) == {"A", "B"} and rows["A"].status == "shipped"

    apply_scd1(delta_enabled, changes(delta_enabled, [cdc("DELETE", "B", 3000)]), t)
    apply_scd1(delta_enabled, changes(delta_enabled, batch1), t)  # replay of an old batch
    rows = {r.order_id: r for r in delta_enabled.table(t).collect()}
    assert rows["B"]._is_deleted and rows["A"].status == "shipped"
    assert delta_enabled.table(t).count() == 2


def test_scd2_versions_across_batches(delta_enabled):
    t = "scd2_test"
    delta_enabled.sql(f"DROP TABLE IF EXISTS {t}")
    apply_scd2(delta_enabled, changes(delta_enabled, [cdc("INSERT", "A", 1000), cdc("INSERT", "B", 1000)]), t)
    apply_scd2(delta_enabled, changes(delta_enabled, [cdc("UPDATE", "A", 2000, "shipped"),
                                        cdc("UPDATE", "A", 3000, "completed"),
                                        cdc("DELETE", "B", 2500)]), t)
    a = delta_enabled.table(t).filter("order_id = 'A'").orderBy("effective_start").collect()
    assert [r.status for r in a] == ["pending", "shipped", "completed"]
    assert [r.is_current for r in a] == [False, False, True]
    assert a[0].effective_end == a[1].effective_start and a[1].effective_end == a[2].effective_start
    b = delta_enabled.table(t).filter("order_id = 'B'").collect()
    assert len(b) == 1 and not b[0].is_current and b[0].effective_end is not None
    assert delta_enabled.table(t).filter("is_current").groupBy("order_id").count().filter("count > 1").count() == 0
