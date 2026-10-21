"""Prevent stale sales records from crossing VALID/QUARANTINE tables."""

from delta.tables import DeltaTable
from pyspark.sql import functions as F


def filter_stale_cross_table_records(
    spark,
    incoming_df,
    opposite_target_path,
):
    """Reject incoming rows superseded by newer opposite-table rows."""

    if not isinstance(opposite_target_path, str) or not opposite_target_path.strip():
        raise ValueError("An opposite_target_path is required")

    required = {"transaction_key", "ingestion_timestamp"}
    missing = required - set(incoming_df.columns)

    if missing:
        raise ValueError(
            f"Missing freshness columns: {sorted(missing)}"
        )

    if not DeltaTable.isDeltaTable(spark, opposite_target_path):
        return incoming_df

    opposite = (
        spark.read.format("delta")
        .load(opposite_target_path)
        .groupBy("transaction_key")
        .agg(
            F.max("ingestion_timestamp").alias(
                "_opposite_latest_timestamp"
            )
        )
    )

    return (
        incoming_df.join(
            opposite,
            on="transaction_key",
            how="left",
        )
        .filter(
            F.col("_opposite_latest_timestamp").isNull()
            | (
                F.col("ingestion_timestamp")
                > F.col("_opposite_latest_timestamp")
            )
        )
        .drop("_opposite_latest_timestamp")
    )
