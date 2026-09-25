# Databricks notebook source
# MAGIC %md
# MAGIC # 01a · Auto Loader: CSV → Bronze
# MAGIC
# MAGIC * Incremental file ingestion with `cloudFiles` (only new files are read on each run).
# MAGIC * Schema inference, and **schema evolution** when a later file adds a column.
# MAGIC * File-level provenance from the `_metadata` column.
# MAGIC * Checkpoints give exactly-once file processing across restarts.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "lakeflow_demo", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

SOURCE = f"/Volumes/{CATALOG}/{SCHEMA}/csv_files_autoloader_source"
STAGING = f"/Volumes/{CATALOG}/{SCHEMA}/auto_loader_staging_files"
CHECKPOINT = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/bronze_sales_orders"
SCHEMA_LOCATION = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints/bronze_sales_orders_schema"
BRONZE = "bronze_sales_orders"

# Start from a clean slate so the notebook can be re-run end to end.
spark.sql(f"DROP TABLE IF EXISTS {BRONZE}")
dbutils.fs.rm(CHECKPOINT, recurse=True)
dbutils.fs.rm(SCHEMA_LOCATION, recurse=True)
for f in dbutils.fs.ls(SOURCE):
    if f.name != "000.csv":
        dbutils.fs.rm(f.path)

# COMMAND ----------

def ingest_available_files():
    """Read every file not yet recorded in the checkpoint, append it to Bronze, stop.

    With `schemaEvolutionMode = addNewColumns`, Auto Loader deliberately stops the
    stream when it sees a new column, after recording the new schema. Restarting
    picks the column up. So on that specific error we rebuild the reader and run again.
    """
    def run():
        stream = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
            .option("header", "true")
            .option("sep", "|")
            .load(SOURCE)
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_file_mod_time", F.col("_metadata.file_modification_time"))
            .withColumn("_ingest_timestamp", F.current_timestamp())
        )
        (stream.writeStream
               .option("checkpointLocation", CHECKPOINT)
               .option("mergeSchema", "true")
               .trigger(availableNow=True)
               .toTable(BRONZE)
               .awaitTermination())

    try:
        run()
    except Exception as e:
        if "UNKNOWN_FIELD" not in str(e) and "UnknownFieldException" not in str(e):
            raise
        print("New column detected: schema updated, restarting the stream.")
        run()


def rows_per_file():
    display(spark.table(BRONZE).groupBy("_source_file").count().orderBy("_source_file"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initial load: `000.csv`

# COMMAND ----------

ingest_available_files()
rows_per_file()

# COMMAND ----------

# MAGIC %md
# MAGIC ## A new file arrives: only `001.csv` is read

# COMMAND ----------

dbutils.fs.cp(f"{STAGING}/001.csv", f"{SOURCE}/001.csv")
ingest_available_files()
rows_per_file()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema evolution: `002.csv` adds `discount_pct`
# MAGIC Earlier rows get `NULL` for the new column; no reload of old files is needed.

# COMMAND ----------

dbutils.fs.cp(f"{STAGING}/002.csv", f"{SOURCE}/002.csv")
ingest_available_files()
rows_per_file()
print("discount_pct in Bronze schema:", "discount_pct" in spark.table(BRONZE).columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rescued data and table history
# MAGIC Values that don't fit the inferred type land in `_rescued_data` instead of failing the file.

# COMMAND ----------

print("rows with rescued data:", spark.table(BRONZE).filter("_rescued_data IS NOT NULL").count())

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY bronze_sales_orders
