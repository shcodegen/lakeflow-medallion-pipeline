# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · CDC: SCD Type 1 and Type 2 with MERGE
# MAGIC
# MAGIC Change events from an OLTP `orders` table (Debezium-style `op` / `before` / `after` /
# MAGIC `ts_ms`) are ingested with Auto Loader, then applied **once per micro-batch** with
# MAGIC `foreachBatch`:
# MAGIC
# MAGIC * **SCD1** (`silver_orders_scd1`): latest state per order, soft deletes, and a sequence
# MAGIC   guard so replayed or out-of-order changes can't overwrite newer data.
# MAGIC * **SCD2** (`silver_orders_scd2`): one row per version with `effective_start` /
# MAGIC   `effective_end` / `is_current`.
# MAGIC
# MAGIC The MERGE logic is in `src/lakeflow_pipeline/cdc.py` and tested against Delta Lake
# MAGIC (`tests/unit/test_cdc_merge.py`). Notebook 04 does the same with declarative AUTO CDC.

# COMMAND ----------

import os
import sys

from pyspark.sql import functions as F

sys.path.append(os.path.abspath("../../src"))
from lakeflow_pipeline.cdc import apply_scd1, apply_scd2
from lakeflow_pipeline.transforms import flatten_cdc

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "lakeflow_demo", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

SOURCE = f"/Volumes/{CATALOG}/{SCHEMA}/cdc_source"
CKPT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"

# Fresh run
for t in ["bronze_orders_cdc", "silver_orders_scd1", "silver_orders_scd2"]:
    spark.sql(f"DROP TABLE IF EXISTS {t}")
for p in ["bronze_orders_cdc", "bronze_orders_cdc_schema", "orders_scd"]:
    dbutils.fs.rm(f"{CKPT}/{p}", recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: raw change events (append-only)

# COMMAND ----------

(spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", f"{CKPT}/bronze_orders_cdc_schema")
      .load(SOURCE)
      .withColumn("_source_file", F.col("_metadata.file_path"))
      .writeStream
      .option("checkpointLocation", f"{CKPT}/bronze_orders_cdc")
      .trigger(availableNow=True)
      .toTable("bronze_orders_cdc")
      .awaitTermination())

display(spark.table("bronze_orders_cdc").groupBy("op").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: apply each new batch of changes to SCD1 and SCD2
# MAGIC The streaming checkpoint guarantees each Bronze batch is applied exactly once,
# MAGIC which is what makes the append step of SCD2 safe.

# COMMAND ----------

def apply_changes(batch_df, batch_id):
    changes = flatten_cdc(batch_df)
    apply_scd1(batch_df.sparkSession, changes, "silver_orders_scd1")
    apply_scd2(batch_df.sparkSession, changes, "silver_orders_scd2")


(spark.readStream.table("bronze_orders_cdc")
      .writeStream
      .foreachBatch(apply_changes)
      .option("checkpointLocation", f"{CKPT}/orders_scd")
      .trigger(availableNow=True)
      .start()
      .awaitTermination())

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCD1: exactly one row per order
# MAGIC SELECT _is_deleted, status, COUNT(*) AS orders
# MAGIC FROM silver_orders_scd1 GROUP BY ALL ORDER BY _is_deleted, status;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCD2: the history of the most frequently changed orders
# MAGIC WITH busiest AS (
# MAGIC   SELECT order_id FROM silver_orders_scd2 GROUP BY order_id ORDER BY COUNT(*) DESC LIMIT 3
# MAGIC )
# MAGIC SELECT order_id, status, total_amount, effective_start, effective_end, is_current, _cdc_operation
# MAGIC FROM silver_orders_scd2 JOIN busiest USING (order_id)
# MAGIC ORDER BY order_id, effective_start;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Invariant: never more than one current version per order
# MAGIC SELECT COUNT(*) AS keys_with_multiple_current_versions
# MAGIC FROM (SELECT order_id FROM silver_orders_scd2 WHERE is_current GROUP BY order_id HAVING COUNT(*) > 1);
