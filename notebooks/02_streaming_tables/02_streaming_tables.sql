-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 · Streaming tables in SQL
-- MAGIC
-- MAGIC `CREATE OR REFRESH STREAMING TABLE ... FROM STREAM read_files(...)` gives Auto Loader
-- MAGIC semantics (incremental, exactly-once per file) from plain SQL. Databricks runs the refresh
-- MAGIC on managed serverless compute. Compared with the older `COPY INTO` at the end.
-- MAGIC
-- MAGIC Run on a SQL warehouse or serverless compute. **If you used a different catalog/schema
-- MAGIC in `00_generate_data`, change the two `USE` lines and the volume paths below.**

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA lakeflow_demo;
SELECT current_catalog(), current_schema();

-- COMMAND ----------

-- DBTITLE 1,Preview the source files
SELECT *
FROM read_files('/Volumes/workspace/lakeflow_demo/csv_files_autoloader_source',
                format => 'csv', sep => '|', header => true)
LIMIT 10;

-- COMMAND ----------

-- DBTITLE 1,Create the streaming table (first refresh starts immediately)
CREATE OR REFRESH STREAMING TABLE sql_orders_bronze
COMMENT 'Orders ingested incrementally from the CSV volume (SQL Auto Loader)'
AS SELECT
  order_id, customer_id, product_id, product_name, category,
  CAST(quantity AS INT)         AS quantity,
  CAST(unit_price AS DOUBLE)    AS unit_price,
  CAST(total_amount AS DOUBLE)  AS total_amount,
  CAST(order_date AS TIMESTAMP) AS order_date,
  status, payment_type, channel, customer_email, customer_city, customer_state,
  _metadata.file_path           AS _source_file,
  current_timestamp()           AS _ingest_timestamp
FROM STREAM read_files('/Volumes/workspace/lakeflow_demo/csv_files_autoloader_source',
                       format => 'csv', sep => '|', header => true);

-- COMMAND ----------

-- DBTITLE 1,Rows per source file
SELECT _source_file, COUNT(*) AS rows
FROM sql_orders_bronze
GROUP BY _source_file
ORDER BY _source_file;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Incremental refresh
-- MAGIC Drop another file into the source volume, e.g. from any Python cell or notebook:
-- MAGIC ```python
-- MAGIC dbutils.fs.cp("/Volumes/workspace/lakeflow_demo/auto_loader_staging_files/003.csv",
-- MAGIC               "/Volumes/workspace/lakeflow_demo/csv_files_autoloader_source/003.csv")
-- MAGIC ```
-- MAGIC then refresh. Only the new file is processed, which `DESCRIBE HISTORY` shows as a small
-- MAGIC `numOutputRows` on the latest update.

-- COMMAND ----------

REFRESH STREAMING TABLE sql_orders_bronze;

-- COMMAND ----------

DESCRIBE HISTORY sql_orders_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## For comparison: `COPY INTO`
-- MAGIC Also idempotent per file, but a plain batch command: no streaming table, no managed
-- MAGIC refresh schedule, and the target table's schema is managed by you.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS copy_into_orders;

COPY INTO copy_into_orders
FROM '/Volumes/workspace/lakeflow_demo/csv_files_autoloader_source'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'sep' = '|', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

-- DBTITLE 1,Running COPY INTO again loads nothing: files already loaded are skipped
COPY INTO copy_into_orders
FROM '/Volumes/workspace/lakeflow_demo/csv_files_autoloader_source'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'sep' = '|', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

-- DBTITLE 1,Clean up
DROP TABLE IF EXISTS sql_orders_bronze;
DROP TABLE IF EXISTS copy_into_orders;
