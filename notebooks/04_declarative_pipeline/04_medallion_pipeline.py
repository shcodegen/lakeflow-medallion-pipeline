# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Declarative medallion pipeline (Lakeflow Declarative Pipelines / DLT)
# MAGIC
# MAGIC The same Bronze → Silver → Gold flow as notebooks 01–05, declared instead of
# MAGIC orchestrated: the pipeline works out dependencies, checkpoints, retries and
# MAGIC incremental processing.
# MAGIC
# MAGIC * **Streaming tables** for append-only ingestion (Auto Loader) and Silver cleaning.
# MAGIC * **Expectations** as data-quality gates (`expect_or_drop` / `expect`), with metrics in the event log.
# MAGIC * **Materialized views** for Gold aggregates.
# MAGIC * **AUTO CDC** (`apply_changes`) to maintain SCD Type 1 and Type 2 tables from change events.
# MAGIC
# MAGIC This is a pipeline source file: it is not run cell by cell. Deploy it with the
# MAGIC bundle (`databricks bundle deploy`, see `databricks.yml`), which also sets the
# MAGIC configuration keys read below. The pipeline publishes to its own schema
# MAGIC (`<schema>_dlt`) so its tables don't collide with the notebook-built ones.

# COMMAND ----------

import sys

import dlt
from pyspark.sql import Window
from pyspark.sql import functions as F

# `bundle.sourcePath` points at the repo's src/ folder (set in databricks.yml).
sys.path.append(spark.conf.get("bundle.sourcePath", "."))
from lakeflow_pipeline.transforms import (  # noqa: E402
    CHANNELS, EVENT_TYPES, ORDER_STATUSES, cast_order_columns, derive_business_columns, flatten_cdc,
    flatten_events,
)

SOURCE_ROOT = f"/Volumes/{spark.conf.get('source_catalog')}/{spark.conf.get('source_schema')}"


def in_list(values):
    return "(" + ", ".join(f"'{v}'" for v in values) + ")"

# COMMAND ----------

# ---------------------------------------------------------------- Bronze


@dlt.table(comment="Raw orders from CSV files (Auto Loader).", table_properties={"quality": "bronze"})
def bronze_sales_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .option("sep", "|")
        .load(f"{SOURCE_ROOT}/csv_files_autoloader_source")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingest_timestamp", F.current_timestamp())
    )


@dlt.table(comment="Raw clickstream events, nested structs preserved.", table_properties={"quality": "bronze"})
def bronze_customer_events():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaHints", "event_time TIMESTAMP, product.price DOUBLE")
        .load(f"{SOURCE_ROOT}/json_raw_source")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingest_timestamp", F.current_timestamp())
    )


@dlt.table(comment="Raw Debezium-style change events for the orders table.", table_properties={"quality": "bronze"})
def bronze_orders_cdc():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"{SOURCE_ROOT}/cdc_source")
    )

# COMMAND ----------

# ---------------------------------------------------------------- Silver


@dlt.table(comment="Typed, validated orders with business columns.", table_properties={"quality": "silver"})
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_order_date", "order_date IS NOT NULL")
@dlt.expect_or_drop("positive_amount", "total_amount > 0")
@dlt.expect_or_drop("positive_quantity", "quantity > 0")
@dlt.expect("known_status", f"status IN {in_list(ORDER_STATUSES)}")
@dlt.expect("known_channel", f"channel IN {in_list(CHANNELS)}")
def silver_sales_orders():
    # Expectations are evaluated on the returned rows, so casting happens first.
    return derive_business_columns(cast_order_columns(dlt.read_stream("bronze_sales_orders")))


@dlt.table(comment="Flattened clickstream events.", table_properties={"quality": "silver"})
@dlt.expect_or_drop("valid_event_id", "event_id IS NOT NULL")
@dlt.expect_or_drop("valid_user", "user_id IS NOT NULL")
@dlt.expect_or_drop("valid_event_time", "event_time IS NOT NULL")
@dlt.expect("known_event_type", f"event_type IN {in_list(EVENT_TYPES)}")
def silver_customer_events():
    return flatten_events(dlt.read_stream("bronze_customer_events"))


@dlt.view(comment="CDC envelope flattened to one row per change.")
def orders_changes():
    return flatten_cdc(dlt.read_stream("bronze_orders_cdc"))


# AUTO CDC: ordering by the source commit time makes late/out-of-order events safe.
dlt.create_streaming_table("silver_orders_scd1", comment="Current state per order (SCD Type 1).")
dlt.apply_changes(
    target="silver_orders_scd1", source="orders_changes", keys=["order_id"],
    sequence_by=F.col("ts_ms"), apply_as_deletes=F.expr("op = 'DELETE'"),
    except_column_list=["op", "ts_ms"], stored_as_scd_type=1,
)

dlt.create_streaming_table("silver_orders_scd2", comment="Full version history per order (SCD Type 2).")
dlt.apply_changes(
    target="silver_orders_scd2", source="orders_changes", keys=["order_id"],
    sequence_by=F.col("ts_ms"), apply_as_deletes=F.expr("op = 'DELETE'"),
    except_column_list=["op", "ts_ms"], stored_as_scd_type=2,
)

# COMMAND ----------

# ---------------------------------------------------------------- Gold (materialized views)


@dlt.table(comment="Daily revenue KPIs by channel group.", table_properties={"quality": "gold"})
def gold_daily_revenue_by_channel():
    return (
        dlt.read("silver_sales_orders")
        .groupBy(F.to_date("order_date").alias("order_date"), "channel_group")
        .agg(F.count("*").alias("orders"),
             F.round(F.sum("total_amount"), 2).alias("revenue"),
             F.countDistinct("customer_id").alias("customers"),
             F.round(F.avg(F.col("is_fulfilled").cast("int")), 4).alias("fulfillment_rate"))
    )


@dlt.table(comment="Product revenue with rank inside its category per quarter.", table_properties={"quality": "gold"})
def gold_product_performance():
    w = Window.partitionBy("category", "order_year", "order_quarter").orderBy(F.desc("revenue"))
    return (
        dlt.read("silver_sales_orders")
        .groupBy("product_id", "product_name", "category", "order_year", "order_quarter")
        .agg(F.sum("quantity").alias("units"), F.round(F.sum("total_amount"), 2).alias("revenue"))
        .withColumn("rank_in_category", F.rank().over(w))
    )


@dlt.table(comment="Funnel: page views → add to cart → purchase, by device and day.", table_properties={"quality": "gold"})
def gold_event_funnel():
    counts = (
        dlt.read("silver_customer_events")
        .groupBy("device_type", F.to_date("event_time").alias("event_date"))
        .pivot("event_type", ["page_view", "add_to_cart", "purchase"])
        .count()
        .na.fill(0)
    )
    # Guard the divisions: with ANSI SQL mode (default on serverless) x / 0 is an error.
    rate = lambda num, den: F.when(F.col(den) > 0, F.round(F.col(num) / F.col(den), 4))  # noqa: E731
    return (counts
            .withColumn("cart_rate", rate("add_to_cart", "page_view"))
            .withColumn("purchase_rate", rate("purchase", "add_to_cart")))
