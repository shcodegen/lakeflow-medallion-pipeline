"""Bronze -> Silver -> Gold transformations and CDC helpers.

Notebooks import these functions (see notebooks/03_silver, 05_gold, 07_cdc)
and tests/ exercises them against small in-memory DataFrames.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, TimestampType

ORDER_STATUSES = ["completed", "pending", "shipped", "cancelled", "returned"]
PAYMENT_TYPES = ["credit_card", "debit_card", "paypal", "apple_pay", "bank_transfer"]
CHANNELS = ["web", "mobile_app", "marketplace", "in_store"]
EVENT_TYPES = ["page_view", "add_to_cart", "purchase", "search", "wishlist_add", "review"]

# --------------------------------------------------------------------------- orders


def cast_order_columns(df: DataFrame) -> DataFrame:
    """Cast raw (string or inferred) order columns to their target types.

    `try_cast` returns NULL for values that don't parse. A plain CAST would raise
    under ANSI SQL mode (the default on Databricks serverless) and fail the
    whole batch; with NULLs, the validation step drops the row instead.
    """
    return (
        df.withColumn("quantity", F.expr("try_cast(quantity AS INT)"))
        .withColumn("unit_price", F.expr("try_cast(unit_price AS DOUBLE)"))
        .withColumn("total_amount", F.expr("try_cast(total_amount AS DOUBLE)"))
        .withColumn("order_date", F.expr("try_cast(order_date AS TIMESTAMP)"))
    )


def filter_valid_orders(df: DataFrame) -> DataFrame:
    """Keep rows that satisfy the critical constraints (the Silver 'contract')."""
    return df.filter(
        F.col("order_id").isNotNull()
        & F.col("customer_id").isNotNull()
        & F.col("order_date").isNotNull()
        & (F.col("total_amount") > 0)
        & (F.col("quantity") > 0)
    )


def dedup_latest(df: DataFrame, key: str | list[str], order_col: str) -> DataFrame:
    """Keep one row per key: the one with the greatest `order_col`."""
    keys = [key] if isinstance(key, str) else key
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def derive_business_columns(df: DataFrame) -> DataFrame:
    """Business rules: revenue band, fulfilment flag, channel group, date parts."""
    return (
        df.withColumn(
            "revenue_band",
            F.when(F.col("total_amount") >= 500, "HIGH")
            .when(F.col("total_amount") >= 100, "MEDIUM")
            .otherwise("LOW"),
        )
        .withColumn("is_fulfilled", F.col("status").isin("completed", "shipped"))
        .withColumn(
            "channel_group",
            F.when(F.col("channel").isin("web", "mobile_app"), "DIGITAL")
            .when(F.col("channel") == "marketplace", "MARKETPLACE")
            .otherwise("RETAIL"),
        )
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .withColumn("order_quarter", F.quarter("order_date"))
    )


def transform_silver_orders(bronze: DataFrame) -> DataFrame:
    """Full Bronze -> Silver step for orders: cast, validate, dedup, enrich."""
    return (
        derive_business_columns(
            dedup_latest(filter_valid_orders(cast_order_columns(bronze)), "order_id", "_ingest_timestamp")
        ).withColumn("_silver_timestamp", F.current_timestamp())
    )


# --------------------------------------------------------------------------- events


def flatten_events(df: DataFrame) -> DataFrame:
    """Flatten the nested `user` and `product` structs of the JSON events."""
    return (
        df.withColumn("user_id", F.col("user.user_id"))
        .withColumn("session_id", F.col("user.session_id"))
        .withColumn("device_type", F.col("user.device_type"))
        .withColumn("browser", F.col("user.browser"))
        .withColumn("country", F.col("user.country"))
        .withColumn("city", F.col("user.city"))
        .withColumn("product_id", F.col("product.product_id"))
        .withColumn("product_name", F.col("product.product_name"))
        .withColumn("category", F.col("product.category"))
        .withColumn("unit_price", F.col("product.price").cast(DoubleType()))
        .withColumn("in_stock", F.col("product.in_stock").cast("boolean"))
        .withColumn("event_time", F.col("event_time").cast(TimestampType()))
        .drop("user", "product")
    )


def transform_silver_events(bronze: DataFrame) -> DataFrame:
    flat = flatten_events(bronze).filter(
        F.col("event_id").isNotNull() & F.col("user_id").isNotNull() & F.col("event_time").isNotNull()
    )
    return dedup_latest(flat, "event_id", "_ingest_timestamp").withColumn(
        "_silver_timestamp", F.current_timestamp()
    )


# --------------------------------------------------------------------------- gold


def build_daily_revenue(silver_orders: DataFrame) -> DataFrame:
    return (
        silver_orders.groupBy(
            F.to_date("order_date").alias("order_date"),
            "order_year",
            "order_month",
            "order_quarter",
            "channel_group",
            "category",
            "revenue_band",
        )
        .agg(
            F.count("order_id").alias("order_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
            F.sum("quantity").alias("total_units_sold"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum(F.col("is_fulfilled").cast("int")).alias("fulfilled_orders"),
        )
        .withColumn("fulfillment_rate", F.round(F.col("fulfilled_orders") / F.col("order_count"), 4))
    )


def build_customer_segments(silver_orders: DataFrame, as_of_date=None) -> DataFrame:
    """RFM (recency / frequency / monetary) segmentation on fulfilled orders.

    `as_of_date` defaults to the latest order date in the data, so results are
    reproducible on a historical dataset instead of drifting with today's date.
    """
    fulfilled = silver_orders.filter(F.col("is_fulfilled"))
    rfm = (
        fulfilled.groupBy("customer_id")
        .agg(
            F.max(F.to_date("order_date")).alias("last_order_date"),
            F.count("order_id").alias("frequency"),
            F.round(F.sum("total_amount"), 2).alias("monetary"),
        )
    )
    if as_of_date is None:
        rfm = rfm.withColumn("_as_of", F.max("last_order_date").over(Window.partitionBy()))
    else:
        rfm = rfm.withColumn("_as_of", F.to_date(F.lit(str(as_of_date))))
    rfm = rfm.withColumn("recency_days", F.datediff("_as_of", "last_order_date")).drop("_as_of")

    # Quintiles: 5 = best. Recent = good, so recency is ordered descending.
    scored = (
        rfm.withColumn("r_score", F.ntile(5).over(Window.orderBy(F.col("recency_days").desc())))
        .withColumn("f_score", F.ntile(5).over(Window.orderBy("frequency")))
        .withColumn("m_score", F.ntile(5).over(Window.orderBy("monetary")))
        .withColumn("rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score"))
    )
    return scored.withColumn(
        "customer_segment",
        F.when(F.col("rfm_score") >= 13, "Champion")
        .when(F.col("rfm_score") >= 10, "Loyal")
        .when(F.col("rfm_score") >= 7, "Potential")
        .when(F.col("rfm_score") >= 4, "At Risk")
        .otherwise("Lost"),
    )


# --------------------------------------------------------------------------- CDC


def flatten_cdc(bronze_cdc: DataFrame) -> DataFrame:
    """Debezium-style envelope (op, ts_ms, before, after) -> one flat change row.

    The key comes from `after` for INSERT/UPDATE and from `before` for DELETE.
    """
    return bronze_cdc.select(
        F.coalesce(F.col("after.order_id"), F.col("before.order_id")).alias("order_id"),
        F.col("op"),
        F.col("ts_ms"),
        (F.col("ts_ms") / 1000).cast(TimestampType()).alias("event_ts"),
        F.col("after.customer_id").alias("customer_id"),
        F.col("after.status").alias("status"),
        F.col("after.total_amount").cast(DoubleType()).alias("total_amount"),
        F.col("after.updated_at").cast(TimestampType()).alias("updated_at"),
    )


def latest_change_per_key(changes: DataFrame) -> DataFrame:
    """One row per order_id: the latest change. Required before MERGE, which
    fails if several source rows match the same target row."""
    return dedup_latest(changes, "order_id", "ts_ms")


def build_scd2_versions(changes: DataFrame) -> DataFrame:
    """Turn a batch of changes into SCD Type 2 version rows.

    Each INSERT/UPDATE becomes a version valid from its event time until the
    next change for the same key. A DELETE only closes the previous version.
    """
    w = Window.partitionBy("order_id").orderBy("ts_ms")
    return (
        changes.withColumn("effective_end", F.lead("event_ts").over(w))
        .filter(F.col("op") != "DELETE")
        .withColumn("effective_start", F.col("event_ts"))
        .withColumn("is_current", F.col("effective_end").isNull())
        .select(
            "order_id", "customer_id", "status", "total_amount", "updated_at",
            "effective_start", "effective_end", "is_current",
            F.col("op").alias("_cdc_operation"),
        )
    )


def first_change_per_key(changes: DataFrame) -> DataFrame:
    """Earliest change time per key in the batch: used to close the version
    that is currently open in the target table."""
    return changes.groupBy("order_id").agg(F.min("event_ts").alias("first_change_ts"))
