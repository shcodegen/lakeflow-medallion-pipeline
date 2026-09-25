import json
from datetime import datetime

from pyspark.sql import functions as F

from lakeflow_pipeline.datagen import ORDER_COLUMNS, DataGenerator
from lakeflow_pipeline.transforms import flatten_cdc, transform_silver_orders


def test_generator_is_deterministic():
    a = DataGenerator(7).orders_csv(20, datetime(2025, 1, 1))
    b = DataGenerator(7).orders_csv(20, datetime(2025, 1, 1))
    assert a == b


def test_orders_csv_shape_and_discount_column():
    g = DataGenerator()
    plain = g.orders_csv(5, datetime(2025, 1, 1)).splitlines()
    evolved = g.orders_csv(5, datetime(2025, 4, 1), with_discount=True).splitlines()
    assert plain[0].split("|") == ORDER_COLUMNS and len(plain) == 6
    assert evolved[0].split("|") == ORDER_COLUMNS + ["discount_pct"]
    assert all(len(line.split("|")) == len(ORDER_COLUMNS) + 1 for line in evolved)


def test_generated_orders_pass_silver_contract(spark, local_files):
    path = local_files / "000.csv"
    path.write_text(DataGenerator().orders_csv(300, datetime(2025, 1, 1)))
    bronze = (spark.read.option("header", True).option("sep", "|").csv(str(path))
              .withColumn("_ingest_timestamp", F.current_timestamp()))
    assert transform_silver_orders(bronze).count() == 300  # every generated row is valid


def test_cdc_events_are_parseable_and_ordered(spark, local_files):
    text = DataGenerator().cdc_ndjson(200, datetime(2025, 6, 1))
    ts = [json.loads(line)["ts_ms"] for line in text.splitlines()]
    assert ts == sorted(ts) and len(set(ts)) == len(ts)
    path = local_files / "cdc.json"
    path.write_text(text)
    changes = flatten_cdc(spark.read.json(str(path)))
    assert changes.filter("order_id IS NULL").count() == 0
    assert {r.op for r in changes.select("op").distinct().collect()} <= {"INSERT", "UPDATE", "DELETE"}


def test_events_are_valid_ndjson():
    lines = DataGenerator().events_ndjson(50, datetime(2025, 1, 1)).splitlines()
    first = json.loads(lines[0])
    assert len(lines) == 50 and {"user", "product", "event_id"} <= set(first)
