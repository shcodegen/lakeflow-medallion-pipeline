# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Run the test suite on Databricks
# MAGIC
# MAGIC The same pytest suite that runs in GitHub Actions (`.github/workflows/ci.yml`), run here
# MAGIC against the cluster's Spark session:
# MAGIC
# MAGIC * `tests/unit`: transform logic on small in-memory DataFrames (no tables needed).
# MAGIC * `tests/integration`: data contracts on the real tables built by notebooks 01–07
# MAGIC   (no duplicate keys, revenue conserved Bronze→Gold, one current SCD2 version per key).
# MAGIC
# MAGIC The job in `databricks.yml` runs this notebook last, so a broken contract fails the job.

# COMMAND ----------

# MAGIC %pip install pytest faker --quiet

# COMMAND ----------

import os
import sys

import pytest

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "lakeflow_demo", "Schema")
os.environ["LAKEFLOW_CATALOG"] = dbutils.widgets.get("catalog")
os.environ["LAKEFLOW_SCHEMA"] = dbutils.widgets.get("schema")

repo_root = os.path.abspath("../..")
sys.dont_write_bytecode = True  # workspace files are read-only for .pyc caches
exit_code = pytest.main([os.path.join(repo_root, "tests"), "-v", "-p", "no:cacheprovider",
                         "--rootdir", repo_root])
assert exit_code == 0, f"pytest failed with exit code {exit_code}"
