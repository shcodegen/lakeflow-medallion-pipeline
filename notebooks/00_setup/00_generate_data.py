# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Generate synthetic source data
# MAGIC
# MAGIC Creates the schema, the Unity Catalog volumes, and seeded synthetic e-commerce data
# MAGIC (see `src/lakeflow_pipeline/datagen.py`). Run once before any other notebook.
# MAGIC
# MAGIC | Volume | Files | Used by |
# MAGIC |---|---|---|
# MAGIC | `csv_files_autoloader_source` | `000.csv`: first orders batch | 01a, 02 |
# MAGIC | `auto_loader_staging_files` | `001.csv`, `002.csv` (adds `discount_pct`), `003.csv` | 01a (new-file arrivals) |
# MAGIC | `json_raw_source` | 3 × NDJSON clickstream files (nested structs) | 01b |
# MAGIC | `parquet_raw_source` | `product_catalog/` | 03, 05 |
# MAGIC | `cdc_source` | 3 × Debezium-style change files for an `orders` table | 04, 07 |
# MAGIC | `checkpoints` | streaming checkpoints and Auto Loader schema locations | 01a, 01b, 07 |

# COMMAND ----------

# MAGIC %pip install faker --quiet

# COMMAND ----------

import os
import sys
from datetime import datetime

sys.path.append(os.path.abspath("../../src"))  # repo's src/ when run from a Databricks Git folder
from lakeflow_pipeline.datagen import DataGenerator

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "lakeflow_demo", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}_dlt`")  # target of the declarative pipeline (04)
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

VOLUMES = ["csv_files_autoloader_source", "auto_loader_staging_files", "json_raw_source",
           "parquet_raw_source", "cdc_source", "checkpoints"]
for v in VOLUMES:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS `{v}`")
vol = {v: f"/Volumes/{CATALOG}/{SCHEMA}/{v}" for v in VOLUMES}
print(f"Target: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orders (CSV), clickstream (NDJSON), product catalog (Parquet), CDC (NDJSON)

# COMMAND ----------

gen = DataGenerator(seed=42)

files = {
    f"{vol['csv_files_autoloader_source']}/000.csv": gen.orders_csv(3149, datetime(2025, 1, 1), 0),
    f"{vol['auto_loader_staging_files']}/001.csv": gen.orders_csv(2932, datetime(2025, 4, 1), 3149),
    f"{vol['auto_loader_staging_files']}/002.csv": gen.orders_csv(1800, datetime(2025, 7, 1), 6081, with_discount=True),
    f"{vol['auto_loader_staging_files']}/003.csv": gen.orders_csv(2100, datetime(2025, 10, 1), 7881),
    f"{vol['json_raw_source']}/events_2025_q1.json": gen.events_ndjson(5000, datetime(2025, 1, 1), 0),
    f"{vol['json_raw_source']}/events_2025_q2.json": gen.events_ndjson(5000, datetime(2025, 4, 1), 5000),
    f"{vol['json_raw_source']}/events_2025_q3.json": gen.events_ndjson(4500, datetime(2025, 7, 1), 10000),
    f"{vol['cdc_source']}/cdc_orders_batch_01.json": gen.cdc_ndjson(2000, datetime(2025, 6, 1)),
    f"{vol['cdc_source']}/cdc_orders_batch_02.json": gen.cdc_ndjson(1500, datetime(2025, 6, 2)),
    f"{vol['cdc_source']}/cdc_orders_batch_03.json": gen.cdc_ndjson(1000, datetime(2025, 6, 3)),
}
for path, content in files.items():
    dbutils.fs.put(path, content, overwrite=True)
    print(f"wrote {path}")

(spark.createDataFrame(gen.product_rows())
     .write.mode("overwrite").parquet(f"{vol['parquet_raw_source']}/product_catalog"))
print(f"wrote {vol['parquet_raw_source']}/product_catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick look

# COMMAND ----------

display(spark.read.option("header", True).option("sep", "|").csv(f"{vol['csv_files_autoloader_source']}/000.csv").limit(5))

# COMMAND ----------

spark.read.json(f"{vol['json_raw_source']}/events_2025_q1.json").printSchema()
