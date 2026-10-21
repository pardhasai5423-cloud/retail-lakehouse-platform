"""Retry-safe Bronze Delta append operations."""

import hashlib
import re

from src.glue.batch_planner import plan_batch
from src.glue.config import get_glue_s3_path


BATCH_ID_PATTERN = re.compile(r"^batch-[0-9a-f]{24}$")


def get_bronze_target(source_key):
    """Return the approved Bronze Delta destination."""

    plan = plan_batch(source_key)

    return get_glue_s3_path(
        "bronze",
        plan.company,
        plan.dataset,
    )


def get_transaction_identity(source_key, batch_id):
    """
    Build a stable Delta transaction identity.

    Each source object identity maps to one transaction.
    """

    plan = plan_batch(source_key)

    if (
        not isinstance(batch_id, str)
        or BATCH_ID_PATTERN.fullmatch(batch_id) is None
    ):
        raise ValueError(
            "Batch ID must be a validated S3 object batch ID"
        )

    app_id = (
        f"retail-lakehouse-bronze-"
        f"{plan.company}-{plan.dataset}-{batch_id}"
    )

    return app_id, 1


def append_bronze_batch(df, source_key, batch_id):
    """
    Append one batch to its Bronze Delta table.

    Delta transaction identifiers support retry deduplication.
    This function performs a write and must only be called
    after AWS access and destination safety are approved.
    """

    target = get_bronze_target(source_key)

    app_id, txn_version = get_transaction_identity(
        source_key, batch_id
    )

    (
        df.write
        .format("delta")
        .mode("append")
        .option("txnAppId", app_id)
        .option("txnVersion", txn_version)
        .save(target)
    )

    return target
