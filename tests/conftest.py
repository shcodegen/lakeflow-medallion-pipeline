import os
import sys

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


def _local_session(tmp_dir):
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("lakeflow-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", tmp_dir)
    )
    try:  # enable Delta Lake when delta-spark is installed (needed for MERGE tests)
        import delta
    except ImportError:
        return builder.getOrCreate()
    builder = (builder
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    jars = os.environ.get("DELTA_JARS")  # optional: pre-downloaded jars, comma separated
    if jars:
        return builder.config("spark.jars", jars).getOrCreate()
    return delta.configure_spark_with_delta_pip(builder).getOrCreate()


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Reuse the notebook's session on Databricks; start a local one elsewhere."""
    active = SparkSession.getActiveSession()
    if active is not None:
        yield active
        return
    session = _local_session(str(tmp_path_factory.mktemp("warehouse")))
    yield session
    session.stop()


ON_DATABRICKS = "DATABRICKS_RUNTIME_VERSION" in os.environ


@pytest.fixture(scope="session")
def delta_enabled(spark):
    """Delta is built into Databricks; locally it needs the extension configured."""
    if not ON_DATABRICKS and "Delta" not in spark.conf.get("spark.sql.extensions", ""):
        pytest.skip("Delta Lake not available in this Spark session")
    return spark


@pytest.fixture
def local_files(tmp_path):
    """A local temp dir Spark can read. Not available on Databricks, where
    executors (or serverless compute) can't see the driver's /tmp."""
    if ON_DATABRICKS:
        pytest.skip("reads driver-local files")
    return tmp_path
