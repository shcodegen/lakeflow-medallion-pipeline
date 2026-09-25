"""Apply a batch of CDC changes to Delta tables with MERGE.

Used by notebooks/07_cdc (batch or foreachBatch) and tested locally against
open-source Delta Lake in tests/unit/test_cdc_merge.py.
"""
from __future__ import annotations

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakeflow_pipeline.transforms import build_scd2_versions, first_change_per_key, latest_change_per_key

SCD1_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    order_id      STRING NOT NULL,
    customer_id   STRING,
    status        STRING,
    total_amount  DOUBLE,
    updated_at    TIMESTAMP,
    _is_deleted   BOOLEAN,
    _event_ts     TIMESTAMP,
    _last_updated TIMESTAMP
) USING DELTA
COMMENT 'SCD Type 1 orders: latest state per order, soft deletes'
"""

SCD2_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    order_id        STRING NOT NULL,
    customer_id     STRING,
    status          STRING,
    total_amount    DOUBLE,
    updated_at      TIMESTAMP,
    effective_start TIMESTAMP NOT NULL,
    effective_end   TIMESTAMP,
    is_current      BOOLEAN,
    _cdc_operation  STRING
) USING DELTA
COMMENT 'SCD Type 2 orders: one row per version with effective dates'
"""


def apply_scd1(spark: SparkSession, changes: DataFrame, table: str) -> None:
    """Upsert the latest change per key.

    * Only the newest change per key is merged (MERGE rejects several source
      rows matching one target row).
    * `source._event_ts > target._event_ts` makes replays and out-of-order
      batches harmless: an older change never overwrites a newer one.
    * DELETE sets `_is_deleted` (soft delete) so downstream jobs can see it.
    """
    spark.sql(SCD1_DDL.format(table=table))
    latest = latest_change_per_key(changes).withColumnRenamed("event_ts", "_event_ts")
    newer = "source._event_ts > target._event_ts"
    (
        DeltaTable.forName(spark, table).alias("target")
        .merge(latest.alias("source"), "target.order_id = source.order_id")
        .whenMatchedUpdate(
            condition=f"source.op = 'DELETE' AND {newer}",
            set={"_is_deleted": F.lit(True), "_event_ts": "source._event_ts",
                 "_last_updated": F.current_timestamp()},
        )
        .whenMatchedUpdate(
            condition=f"source.op != 'DELETE' AND {newer}",
            set={"customer_id": "source.customer_id", "status": "source.status",
                 "total_amount": "source.total_amount", "updated_at": "source.updated_at",
                 "_is_deleted": F.lit(False), "_event_ts": "source._event_ts",
                 "_last_updated": F.current_timestamp()},
        )
        .whenNotMatchedInsert(
            condition="source.op != 'DELETE'",
            values={"order_id": "source.order_id", "customer_id": "source.customer_id",
                    "status": "source.status", "total_amount": "source.total_amount",
                    "updated_at": "source.updated_at", "_is_deleted": F.lit(False),
                    "_event_ts": "source._event_ts", "_last_updated": F.current_timestamp()},
        )
        .execute()
    )


def apply_scd2(spark: SparkSession, changes: DataFrame, table: str) -> None:
    """Close the open version of every changed key, then append new versions.

    Step 1 merges ONE row per key (its earliest change in the batch) so the
    MERGE is always unambiguous. Step 2 appends the versions built by
    `build_scd2_versions`, whose effective ranges chain within the batch.
    Process each batch exactly once (e.g. via foreachBatch + checkpoint):
    re-applying the same batch would append its versions again.
    """
    spark.sql(SCD2_DDL.format(table=table))
    closes = first_change_per_key(changes)
    (
        DeltaTable.forName(spark, table).alias("target")
        .merge(closes.alias("source"), "target.order_id = source.order_id AND target.is_current")
        .whenMatchedUpdate(set={"effective_end": "source.first_change_ts", "is_current": F.lit(False)})
        .execute()
    )
    build_scd2_versions(changes).write.format("delta").mode("append").saveAsTable(table)
