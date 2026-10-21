"""Durable Delta processing ledger.

Concurrency contract:
    Only one coordinator may mutate a ledger path at a time.
    This module does not implement a distributed lock.
"""

import json

from delta.tables import DeltaTable
from pyspark.sql import types as T

from src.glue.processing_ledger import (
    create_batch_record,
    transition_batch,
)

LEDGER_SCHEMA = T.StructType([
    T.StructField("batch_id", T.StringType(), False),
    T.StructField("datasets_json", T.StringType(), False),
    T.StructField("source_versions_json", T.StringType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("attempt", T.IntegerType(), False),
    T.StructField("created_at", T.StringType(), False),
    T.StructField("updated_at", T.StringType(), False),
    T.StructField("error", T.StringType(), True),
])


def _validate_path(path):
    if not isinstance(path, str) or not path.strip():
        raise ValueError("A ledger path is required")
    return path


def _encode(record):
    return (
        record["batch_id"],
        json.dumps(record["datasets"]),
        json.dumps(record["source_versions"], sort_keys=True),
        record["status"],
        record["attempt"],
        record["created_at"],
        record["updated_at"],
        record["error"],
    )


def _decode(row):
    return {
        "batch_id": row["batch_id"],
        "datasets": tuple(json.loads(row["datasets_json"])),
        "source_versions": json.loads(row["source_versions_json"]),
        "status": row["status"],
        "attempt": row["attempt"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "error": row["error"],
    }


def read_batch(spark, ledger_path, batch_id):
    _validate_path(ledger_path)

    if not DeltaTable.isDeltaTable(spark, ledger_path):
        return None

    from pyspark.sql import functions as F

    rows = (
        spark.read.format("delta")
        .load(ledger_path)
        .where(F.col("batch_id") == batch_id)
        .limit(2)
        .collect()
    )

    if len(rows) > 1:
        raise ValueError(f"Duplicate ledger batch_id: {batch_id}")

    return _decode(rows[0]) if rows else None


def register_batch(
    spark,
    ledger_path,
    batch_id,
    datasets,
    source_versions=None,
    *,
    allow_writes=False,
):
    """Register a batch, or return its existing immutable identity."""
    _validate_path(ledger_path)

    proposed = create_batch_record(
        batch_id, datasets, source_versions
    )

    existing = read_batch(spark, ledger_path, proposed["batch_id"])

    if existing is not None:
        if (
            existing["datasets"] != proposed["datasets"]
            or existing["source_versions"] != proposed["source_versions"]
        ):
            raise ValueError(
                "Batch ID already exists with different source identity"
            )
        return existing

    if not allow_writes:
        return {**proposed, "dry_run": True}

    df = spark.createDataFrame([_encode(proposed)], LEDGER_SCHEMA)

    if not DeltaTable.isDeltaTable(spark, ledger_path):
        df.write.format("delta").mode("errorifexists").save(
            ledger_path
        )
    else:
        (
            DeltaTable.forPath(spark, ledger_path)
            .alias("target")
            .merge(
                df.alias("source"),
                "target.batch_id = source.batch_id",
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    return read_batch(spark, ledger_path, proposed["batch_id"])


def update_batch_status(
    spark,
    ledger_path,
    batch_id,
    new_status,
    *,
    error=None,
    allow_writes=False,
):
    """Apply a validated state transition to an existing batch."""
    _validate_path(ledger_path)

    existing = read_batch(spark, ledger_path, batch_id)

    if existing is None:
        raise ValueError(f"Unknown batch_id: {batch_id}")

    updated = transition_batch(existing, new_status, error=error)

    if not allow_writes:
        return {**updated, "dry_run": True}

    df = spark.createDataFrame([_encode(updated)], LEDGER_SCHEMA)

    (
        DeltaTable.forPath(spark, ledger_path)
        .alias("target")
        .merge(
            df.alias("source"),
            "target.batch_id = source.batch_id",
        )
        .whenMatchedUpdateAll(
            condition=(
                "target.status = '"
                + existing["status"]
                + "' AND target.attempt = "
                + str(existing["attempt"])
            )
        )
        .execute()
    )

    saved = read_batch(spark, ledger_path, batch_id)

    if (
        saved["status"] != updated["status"]
        or saved["attempt"] != updated["attempt"]
    ):
        raise RuntimeError(
            "Ledger transition conflict; another writer may have updated "
            "this batch"
        )

    return saved
