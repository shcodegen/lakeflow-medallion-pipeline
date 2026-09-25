# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Gold: business aggregates, MERGE, table maintenance
# MAGIC
# MAGIC * Gold tables in PySpark and SQL (daily revenue, product ranking, RFM segments, executive summary).
# MAGIC * **Incremental refresh with `MERGE`**: recompute only the latest month and upsert it.
# MAGIC * `OPTIMIZE` + Z-ORDER, `DESCRIBE HISTORY`, and time travel.

# COMMAND ----------

import os
import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F

sys.path.append(os.path.abspath("../../src"))
from lakeflow_pipeline.transforms import build_customer_segments, build_daily_revenue

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "lakeflow_demo", "Schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

silver = spark.table("silver_sales_orders")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Daily revenue (PySpark) and product ranking (SQL)

# COMMAND ----------

(build_daily_revenue(silver).withColumn("_gold_timestamp", F.current_timestamp())
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_daily_revenue"))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_product_performance AS
# MAGIC WITH sales AS (
# MAGIC   SELECT o.product_id, o.product_name, o.category, o.order_year, o.order_quarter,
# MAGIC          COUNT(*) AS orders, SUM(o.quantity) AS units, ROUND(SUM(o.total_amount), 2) AS revenue,
# MAGIC          ANY_VALUE(p.warehouse_qty) AS current_stock   -- silver_products has one row per product
# MAGIC   FROM silver_sales_orders o
# MAGIC   LEFT JOIN silver_products p USING (product_id)
# MAGIC   GROUP BY o.product_id, o.product_name, o.category, o.order_year, o.order_quarter
# MAGIC )
# MAGIC SELECT *,
# MAGIC        RANK() OVER (PARTITION BY category, order_year, order_quarter ORDER BY revenue DESC) AS rank_in_category,
# MAGIC        ROUND(revenue / SUM(revenue) OVER (PARTITION BY category, order_year, order_quarter), 4) AS category_share
# MAGIC FROM sales;
# MAGIC
# MAGIC SELECT * FROM gold_product_performance WHERE rank_in_category <= 3
# MAGIC ORDER BY order_year, order_quarter, category, rank_in_category;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Customer segments (RFM)
# MAGIC Recency is measured against the latest order in the data, not today's date, so results are reproducible.

# COMMAND ----------

segments = build_customer_segments(silver)
segments.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_customer_segments")
display(spark.table("gold_customer_segments").groupBy("customer_segment").count().orderBy(F.desc("count")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Incremental refresh with MERGE
# MAGIC When only recent data changes, recompute the affected slice (here: the latest month)
# MAGIC and upsert it, instead of rewriting the whole table.

# COMMAND ----------

latest = silver.agg(F.max("order_year").alias("y")).first()["y"]
latest_month = silver.filter(F.col("order_year") == latest).agg(F.max("order_month")).first()[0]
recomputed = (build_daily_revenue(silver.filter((F.col("order_year") == latest) & (F.col("order_month") == latest_month)))
              .withColumn("_gold_timestamp", F.current_timestamp()))

keys = ["order_date", "channel_group", "category", "revenue_band"]
(DeltaTable.forName(spark, "gold_daily_revenue").alias("t")
    .merge(recomputed.alias("s"), " AND ".join(f"t.{k} = s.{k}" for k in keys))
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute())
print(f"merged {recomputed.count()} rows for {latest}-{latest_month:02d}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Executive summary
# MAGIC Quarterly revenue comes from Silver directly; segment counts are joined per customer's
# MAGIC last-order quarter. (Joining every revenue row to every customer would multiply the totals.)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_executive_summary AS
# MAGIC WITH revenue AS (
# MAGIC   SELECT order_year, order_quarter,
# MAGIC          ROUND(SUM(total_amount), 2) AS revenue, COUNT(*) AS orders,
# MAGIC          COUNT(DISTINCT customer_id) AS active_customers,
# MAGIC          ROUND(AVG(CAST(is_fulfilled AS INT)), 4) AS fulfillment_rate
# MAGIC   FROM silver_sales_orders GROUP BY ALL
# MAGIC ), segments AS (
# MAGIC   SELECT year(last_order_date) AS order_year, quarter(last_order_date) AS order_quarter,
# MAGIC          COUNT_IF(customer_segment = 'Champion') AS champions,
# MAGIC          COUNT_IF(customer_segment = 'At Risk')  AS at_risk
# MAGIC   FROM gold_customer_segments GROUP BY ALL
# MAGIC )
# MAGIC SELECT r.*, COALESCE(s.champions, 0) AS champions_last_active, COALESCE(s.at_risk, 0) AS at_risk_last_active
# MAGIC FROM revenue r LEFT JOIN segments s USING (order_year, order_quarter)
# MAGIC ORDER BY order_year, order_quarter;
# MAGIC
# MAGIC SELECT * FROM gold_executive_summary;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Maintenance and time travel

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE gold_daily_revenue ZORDER BY (order_date, channel_group);

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY gold_daily_revenue;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row count before the MERGE (version 0) vs now
# MAGIC SELECT (SELECT COUNT(*) FROM gold_daily_revenue VERSION AS OF 0) AS rows_v0,
# MAGIC        (SELECT COUNT(*) FROM gold_daily_revenue) AS rows_now;
