"""Read-only reconciliation of Bronze ledger and Delta transaction state."""

from src.pipeline.bronze_recovery import inspect_bronze_transaction
from src.pipeline.ledger import get_batch_record


def inspect_bronze_recovery(spark, batch, *, expected_sha256):
    """
    Compare ledger status with the retained Delta transaction marker.

    This function performs no writes and never changes ledger state.
    """
    batch_id = batch["batch_id"]

    record = get_batch_record(spark, batch_id)

    if record is None:
        return {
            "batch_id": batch_id,
            "ledger_status": "NOT_REGISTERED",
            "decision": "MANUAL_REVIEW",
            "reason": "No ledger record exists",
        }

    expected_identity = {
        batch["dataset"]: {
            "company": batch["company"],
            "source_key": batch["source_key"],
            "etag": batch["etag"],
            "sha256": expected_sha256,
        }
    }

    if (
        record.get("datasets") != (batch["dataset"],)
        or record.get("source_versions") != expected_identity
    ):
        return {
            "batch_id": batch_id,
            "ledger_status": record["status"],
            "decision": "MANUAL_REVIEW",
            "reason": "Ledger source identity mismatch",
        }

    inspection = inspect_bronze_transaction(
        spark,
        batch["source_key"],
        batch_id,
    )

    ledger_status = record["status"]
    transaction_status = inspection["status"]

    if ledger_status == "RUNNING" and transaction_status == "COMMITTED":
        decision = "ELIGIBLE_TO_FINALIZE"
    elif ledger_status == "COMPLETED" and transaction_status == "COMMITTED":
        decision = "ALREADY_COMPLETED"
    else:
        decision = "MANUAL_REVIEW"

    return {
        "batch_id": batch_id,
        "ledger_status": ledger_status,
        "transaction_status": transaction_status,
        "decision": decision,
        "inspection": inspection,
    }
