"""Controlled finalization of confirmed Bronze transactions."""

from src.pipeline.bronze_reconciliation import inspect_bronze_recovery
from src.pipeline.ledger import transition_source_batch
from src.pipeline.safety import require_write_approval


def finalize_confirmed_bronze(
    spark,
    batch,
    *,
    expected_sha256,
    allow_writes=False,
):
    """
    Finalize a RUNNING batch whose Bronze transaction is confirmed.

    Requires explicit approval and a single active coordinator.
    Does not append or modify Bronze data.
    """
    require_write_approval(allow_writes)

    inspection = inspect_bronze_recovery(
        spark,
        batch,
        expected_sha256=expected_sha256,
    )

    if inspection["decision"] != "ELIGIBLE_TO_FINALIZE":
        raise RuntimeError(
            "Bronze batch is not eligible for finalization: "
            f"{inspection['decision']}"
        )

    if inspection["ledger_status"] != "RUNNING":
        raise RuntimeError(
            "Only RUNNING batches can be finalized"
        )

    if inspection["transaction_status"] != "COMMITTED":
        raise RuntimeError(
            "Bronze transaction is not confirmed"
        )

    updated = transition_source_batch(
        spark,
        batch["batch_id"],
        "COMPLETED",
        allow_writes=True,
    )

    return {
        "status": "completed",
        "batch_id": batch["batch_id"],
        "ledger_status": updated["status"],
        "recovery": inspection,
    }
