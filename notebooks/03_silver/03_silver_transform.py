# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver: validate, deduplicate, enrich
# MAGIC
# MAGIC * Data-quality metrics computed in **one pass** and appended to a DQ log table.
# MAGIC * Transform logic lives in `src/lakeflow_pipeline/transforms.py` and is unit-tested
# MAGIC   (`tests/unit/test_orders.py`). The notebook only does I/O.

# COMMAND ----------

import os
import sys

from pyspark.sql import functions as F

sys.path.append(os.path.abspath("../../src"))
from lakeflow_pipeline.transforms import (
    CHANNELS, ORDER_STATUSES, PAYMENT_TYPES, transform_silver_events, transform_silver_orders,
)

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "lakeflow_demo", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")
PRODUCT_CATALOG = f"/Volumes/{CATALOG}/{SCHEMA}/parquet_raw_source/product_catalog"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data-quality metrics on Bronze (one aggregation, not one job per check)

# COMMAND ----------

def dq_metrics(df, table_name, not_null, positive, accepted):
    """Count rule violations for all checks in a single Spark job."""
    checks = (
        [(f"not_null:{c}", F.col(c).isNull()) for c in not_null]
        + [(f"positive:{c}", F.col(c) <= 0) for c in positive]
        + [(f"accepted_values:{c}", ~F.col(c).isin(v)) for c, v in accepted.items()]
    )
    row = df.agg(F.count("*").alias("_total"),
                 *[F.sum(cond.cast("int")).alias(name) for name, cond in checks]).first()
    total = row["_total"]
    return spark.createDataFrame(
        [(table_name, name, int(row[name] or 0), total) for name, _ in checks],
        "table_name string, check string, bad_rows long, total_rows long",
    ).withColumn("status", F.when(F.col("bad_rows") == 0, "PASS").otherwise("WARN")) \
     .withColumn("run_ts", F.current_timestamp())


bronze_orders = spark.table("bronze_sales_orders")
dq = dq_metrics(
    bronze_orders, "bronze_sales_orders",
    not_null=["order_id", "customer_id", "product_id", "total_amount", "order_date"],
    positive=["quantity", "unit_price"],
    accepted={"status": ORDER_STATUSES, "payment_type": PAYMENT_TYPES, "channel": CHANNELS},
)
dq.write.mode("append").saveAsTable("silver_dq_log")
display(dq)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Silver tables

# COMMAND ----------

transform_silver_orders(bronze_orders).write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("silver_sales_orders")

transform_silver_events(spark.table("bronze_customer_events")).write.mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable("silver_customer_events")

# The catalog has one row per (product, supplier). Collapse to ONE row per product,
# otherwise any join on product_id downstream would multiply order revenue.
(spark.read.parquet(PRODUCT_CATALOG)
      .filter("is_active")
      .groupBy("product_id", "product_name", "category", "base_price")
      .agg(F.sum("warehouse_qty").alias("warehouse_qty"),
           F.countDistinct("supplier_id").alias("active_suppliers"),
           F.min("lead_days").alias("min_lead_days"))
      .withColumn("_silver_timestamp", F.current_timestamp())
      .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_products"))

# COMMAND ----------

for t in ["bronze_sales_orders", "silver_sales_orders", "bronze_customer_events",
          "silver_customer_events", "silver_products"]:
    print(f"{t:26} {spark.table(t).count():>8,} rows")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Revenue by channel group and quarter
# MAGIC SELECT channel_group, order_year, order_quarter,
# MAGIC        COUNT(*) AS orders, ROUND(SUM(total_amount), 2) AS revenue,
# MAGIC        ROUND(AVG(CAST(is_fulfilled AS INT)), 3) AS fulfilled_share
# MAGIC FROM silver_sales_orders
# MAGIC GROUP BY ALL
# MAGIC ORDER BY order_year, order_quarter, revenue DESC
