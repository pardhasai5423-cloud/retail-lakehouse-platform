"""Ledger-controlled Gold pipeline coordinator.

A single coordinator must own the ledger and Gold target paths.
This is not a distributed locking implementation.
"""

from src.glue.delta_processing_ledger import (
    register_batch,
    update_batch_status,
)
from src.glue.jobs.gold_orchestrator import execute_gold_pipeline


def execute_incremental_batch(
    spark,
    *,
    batch_id,
    changed_datasets,
    source_versions,
    ledger_path,
    silver_base,
    gold_base,
    allow_writes=False,
):
    """Execute one registered batch with durable lifecycle tracking."""
    datasets = tuple(sorted(set(changed_datasets)))

    record = register_batch(
        spark,
        ledger_path,
        batch_id,
        datasets,
        source_versions,
        allow_writes=allow_writes,
    )

    if record["status"] == "COMPLETED":
        return {
            "status": "skipped",
            "reason": "already_completed",
            "batch_id": batch_id,
        }

    if record["status"] == "RUNNING":
        raise RuntimeError(
            f"Batch {batch_id} is already RUNNING; "
            "manual recovery is required"
        )

    if not allow_writes:
        plan = execute_gold_pipeline(
            spark,
            datasets,
            silver_base,
            gold_base,
            allow_writes=False,
        )
        return {
            "status": "dry_run",
            "batch_id": batch_id,
            "pipeline": plan,
        }

    update_batch_status(
        spark,
        ledger_path,
        batch_id,
        "RUNNING",
        allow_writes=True,
    )

    try:
        result = execute_gold_pipeline(
            spark,
            datasets,
            silver_base,
            gold_base,
            allow_writes=True,
        )

        if result["status"] != "completed":
            raise RuntimeError(
                "Gold pipeline did not report completion"
            )

        update_batch_status(
            spark,
            ledger_path,
            batch_id,
            "COMPLETED",
            allow_writes=True,
        )

        return {
            "status": "completed",
            "batch_id": batch_id,
            "pipeline": result,
        }

    except Exception as exc:
        update_batch_status(
            spark,
            ledger_path,
            batch_id,
            "FAILED",
            error=str(exc) or type(exc).__name__,
            allow_writes=True,
        )
        raise
