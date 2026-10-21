"""Reconcile sales records across valid and quarantine Delta tables."""

from delta.tables import DeltaTable
from pyspark.sql import functions as F


def remove_stale_sales_records(
    spark,
    incoming_df,
    opposite_target_path,
):
    """
    Remove opposite-table records when incoming records are at least
    as recent as the stored records.

    Only existing Delta tables are modified.
    """
    if not isinstance(opposite_target_path, str) or not opposite_target_path.strip():
        raise ValueError("An opposite_target_path is required")

    required = {"transaction_key", "ingestion_timestamp"}
    missing = required - set(incoming_df.columns)

    if missing:
        raise ValueError(
            f"Missing reconciliation columns: {sorted(missing)}"
        )

    if not DeltaTable.isDeltaTable(spark, opposite_target_path):
        return {"status": "not_found", "deleted": False}

    target = DeltaTable.forPath(spark, opposite_target_path)

    keys = incoming_df.select(
        "transaction_key",
        "ingestion_timestamp",
    ).dropDuplicates(["transaction_key"])

    (
        target.alias("target")
        .merge(
            keys.alias("source"),
            "target.transaction_key = source.transaction_key",
        )
        .whenMatchedDelete(
            condition=(
                "source.ingestion_timestamp >= "
                "target.ingestion_timestamp"
            )
        )
        .execute()
    )

    return {"status": "reconciled", "deleted": True}
