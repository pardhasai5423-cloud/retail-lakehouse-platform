"""Idempotent Delta MERGE writer for quarantined sales."""

from delta.tables import DeltaTable
from pyspark.sql import functions as F


def validate_quarantine_batch(df):
    """Validate quarantine input before any Delta writes."""
    required = {
        "transaction_key",
        "batch_id",
        "ingestion_timestamp",
        "validation_status",
        "quality_issues",
    }

    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing quarantine columns: {sorted(missing)}"
        )

    if df.filter(
        F.col("transaction_key").isNull()
        | (F.trim(F.col("transaction_key")) == "")
    ).limit(1).count():
        raise ValueError("Quarantine contains null or blank transaction keys")

    if df.filter(
        F.col("batch_id").isNull()
        | (F.trim(F.col("batch_id")) == "")
    ).limit(1).count():
        raise ValueError("Quarantine contains null or blank batch IDs")

    if df.filter(
        F.col("ingestion_timestamp").isNull()
    ).limit(1).count():
        raise ValueError("Quarantine contains null ingestion timestamps")

    if df.filter(
        F.col("validation_status") != "QUARANTINE"
    ).limit(1).count():
        raise ValueError("Quarantine contains non-quarantined records")

    if df.filter(
        F.col("validation_status").isNull()
    ).limit(1).count():
        raise ValueError("Quarantine contains null validation statuses")

    if df.select("batch_id").distinct().limit(2).count() > 1:
        raise ValueError("Quarantine contains multiple batch IDs")

    if df.groupBy("transaction_key").count().filter(
        F.col("count") > 1
    ).limit(1).count():
        raise ValueError("Quarantine contains duplicate transaction keys")

    return True


def merge_sales_quarantine(spark, df, target_path):
    """Insert/update quarantined transactions without duplicate keys."""
    if not isinstance(target_path, str) or not target_path.strip():
        raise ValueError("A quarantine target_path is required")

    validate_quarantine_batch(df)

    if not DeltaTable.isDeltaTable(spark, target_path):
        (
            df.write.format("delta")
            .mode("errorifexists")
            .save(target_path)
        )
        return {"status": "created", "target": target_path}

    target = DeltaTable.forPath(spark, target_path)

    (
        target.alias("target")
        .merge(
            df.alias("source"),
            "target.transaction_key = source.transaction_key",
        )
        .whenMatchedUpdateAll(
            condition=(
                "source.ingestion_timestamp > "
                "target.ingestion_timestamp"
            )
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    return {"status": "merged", "target": target_path}
