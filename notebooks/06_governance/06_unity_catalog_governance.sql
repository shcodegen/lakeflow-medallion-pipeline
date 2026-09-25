-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 06 · Unity Catalog governance
-- MAGIC
-- MAGIC Column masks for PII, row filters, tags for classification, lineage from system tables,
-- MAGIC and least-privilege grants. **If you used a different catalog/schema in
-- MAGIC `00_generate_data`, change the two `USE` lines.**
-- MAGIC
-- MAGIC The group names (`data_engineers`, `us_east_analysts`) are examples. In a workspace
-- MAGIC without those groups, `is_member()` is false, so you see the masked or filtered view.

-- COMMAND ----------

USE CATALOG workspace;
USE SCHEMA lakeflow_demo;
SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Column mask: hide customer emails from everyone but data engineers

-- COMMAND ----------

CREATE OR REPLACE FUNCTION mask_email(email STRING)
RETURNS STRING
RETURN CASE WHEN is_member('data_engineers') THEN email
            ELSE concat(left(email, 2), '***@', split_part(email, '@', 2)) END;

ALTER TABLE silver_sales_orders ALTER COLUMN customer_email SET MASK mask_email;

SELECT order_id, customer_email FROM silver_sales_orders LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Row filter: regional analysts see only their states

-- COMMAND ----------

CREATE OR REPLACE FUNCTION us_east_only(state STRING)
RETURNS BOOLEAN
RETURN is_member('data_engineers')
       OR NOT is_member('us_east_analysts')
       OR state IN ('NY', 'NJ', 'CT', 'MA', 'PA', 'MD', 'VA', 'NC', 'SC', 'GA', 'FL');

ALTER TABLE silver_sales_orders SET ROW FILTER us_east_only ON (customer_state);

SELECT customer_state, COUNT(*) AS rows FROM silver_sales_orders GROUP BY ALL ORDER BY rows DESC LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Tags: classify tables and PII columns, then query them

-- COMMAND ----------

ALTER TABLE bronze_sales_orders SET TAGS ('quality_tier' = 'bronze', 'pii_level' = 'high');
ALTER TABLE silver_sales_orders SET TAGS ('quality_tier' = 'silver', 'pii_level' = 'medium');
ALTER TABLE gold_daily_revenue  SET TAGS ('quality_tier' = 'gold',   'pii_level' = 'none');
ALTER TABLE silver_sales_orders ALTER COLUMN customer_email SET TAGS ('pii' = 'email');
ALTER TABLE silver_sales_orders ALTER COLUMN customer_city  SET TAGS ('pii' = 'location');

-- COMMAND ----------

SELECT table_name, tag_name, tag_value
FROM information_schema.table_tags
WHERE schema_name = current_schema()
ORDER BY table_name, tag_name;

-- COMMAND ----------

SELECT table_name, column_name, tag_value AS pii_type
FROM information_schema.column_tags
WHERE schema_name = current_schema() AND tag_name = 'pii';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Lineage (system tables; needs access to `system.access`)

-- COMMAND ----------

SELECT source_table_full_name, target_table_full_name, entity_type, event_time
FROM system.access.table_lineage
WHERE target_table_full_name LIKE concat(current_catalog(), '.', current_schema(), '.%')
ORDER BY event_time DESC
LIMIT 50;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Least-privilege grants (examples: need the groups to exist)
-- MAGIC ```sql
-- MAGIC GRANT USE CATALOG ON CATALOG workspace TO `analysts`;
-- MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA workspace.lakeflow_demo TO `analysts`;  -- masks/filters still apply
-- MAGIC GRANT ALL PRIVILEGES ON SCHEMA workspace.lakeflow_demo TO `data_engineers`;
-- MAGIC ```

-- COMMAND ----------

-- DBTITLE 1,Remove the mask and filter so later notebooks and tests see all rows
ALTER TABLE silver_sales_orders ALTER COLUMN customer_email DROP MASK;
ALTER TABLE silver_sales_orders DROP ROW FILTER;
