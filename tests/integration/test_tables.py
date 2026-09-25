"""Checks against the real tables. Skipped unless LAKEFLOW_CATALOG and
LAKEFLOW_SCHEMA are set (notebooks/08_testing sets them on Databricks)."""
import os

import pytest
from pyspark.sql import functions as F

CATALOG = os.environ.get("LAKEFLOW_CATALOG")
SCHEMA = os.environ.get("LAKEFLOW_SCHEMA")
pytestmark = pytest.mark.skipif(not (CATALOG and SCHEMA), reason="no Databricks tables configured")


def table(spark, name):
    try:
        return spark.table(f"{CATALOG}.{SCHEMA}.{name}")
    except Exception:
        pytest.skip(f"{name} does not exist yet")


def test_silver_orders_contract(spark):
    df = table(spark, "silver_sales_orders")
    assert df.count() > 0
    assert df.filter(F.col("order_id").isNull() | (F.col("total_amount") <= 0)).count() == 0
    assert df.groupBy("order_id").count().filter("count > 1").count() == 0


def test_gold_revenue_matches_silver(spark):
    silver, gold = table(spark, "silver_sales_orders"), table(spark, "gold_daily_revenue")
    assert gold.agg(F.sum("order_count")).first()[0] == silver.count()


def test_scd1_has_one_row_per_key(spark):
    df = table(spark, "silver_orders_scd1")
    assert df.groupBy("order_id").count().filter("count > 1").count() == 0


def test_scd2_has_one_current_version_per_key(spark):
    df = table(spark, "silver_orders_scd2").filter("is_current")
    assert df.groupBy("order_id").count().filter("count > 1").count() == 0
