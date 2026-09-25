# Databricks notebook source
# MAGIC %md
# MAGIC # 01b · Auto Loader: nested JSON → Bronze
# MAGIC
# MAGIC * One-shot batch load with `read_files` (CTAS) compared with incremental Auto Loader.
# MAGIC * `schemaHints` to pin types that inference would guess wrong.
# MAGIC * Bronze keeps the nested structs as they arrived; flattening happens in Silver.

# COMMAND ----------

import os
import sys

from pyspark.sql import functions as F

sys.path.append(os.path.abspath("../../src"))
from lakeflow_pipeline.transforms import flatten_events

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "lakeflow_demo", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

SOURCE = f"/Volumes/{CATALOG}/{SCHEMA}/json_raw_source"
CHECKPOINT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/bronze_customer_events"
SCHEMA_LOCATION = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/bronze_customer_events_schema"
BRONZE = "bronze_customer_events"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 1: batch snapshot with `read_files` (CTAS)

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE ctas_customer_events AS
SELECT event_id, event_type, CAST(event_time AS TIMESTAMP) AS event_time, platform,
       user.user_id, user.device_type, product.product_id, product.price AS product_price,
       _metadata.file_path AS _source_file
FROM read_files('{SOURCE}', format => 'json')
""")
display(spark.table("ctas_customer_events").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 2: incremental with Auto Loader (keeps the full nested record)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {BRONZE}")
dbutils.fs.rm(CHECKPOINT, recurse=True)
dbutils.fs.rm(SCHEMA_LOCATION, recurse=True)

(spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
      .option("cloudFiles.schemaHints", "event_time TIMESTAMP, product.price DOUBLE")
      .load(SOURCE)
      .withColumn("_source_file", F.col("_metadata.file_path"))
      .withColumn("_ingest_timestamp", F.current_timestamp())
      .writeStream
      .option("checkpointLocation", CHECKPOINT)
      .trigger(availableNow=True)
      .toTable(BRONZE)
      .awaitTermination())

print(f"{BRONZE}: {spark.table(BRONZE).count():,} rows")
spark.table(BRONZE).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview of the Silver flattening (same function the Silver notebook uses)

# COMMAND ----------

flat = flatten_events(spark.table(BRONZE))
display(flat.groupBy("event_type", "device_type").agg(
    F.count("*").alias("events"), F.countDistinct("user_id").alias("users")).orderBy(F.desc("events")))
