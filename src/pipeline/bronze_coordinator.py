"""Single-coordinator, ledger-tracked Bronze batch execution."""

from src.glue.processing_ledger import should_process
from src.pipeline.bronze_runner import prepare_verified_bronze
from src.pipeline.bronze_writer import write_bronze_batch
from src.pipeline.ledger import (
    get_batch_record,
    register_source_batch,
    transition_source_batch,
)
from src.pipeline.safety import require_write_approval


def execute_bronze_source_batch(
    spark,
    s3_client,
    batch,
    *,
    expected_sha256,
    allow_writes=False,
):
    """
    Execute one verified source batch into Bronze Delta.

    Requires a single active coordinator. A RUNNING batch is not
    automatically recovered, and a COMPLETED batch is skipped.

    The caller must ensure that the source identity and checksum
    match the original registered batch.
    """
    require_write_approval(allow_writes)

    batch_id = batch["batch_id"]

    record = get_batch_record(spark, batch_id)

    if record is not None:
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
            raise ValueError(
                f"Ledger source identity mismatch for {batch_id}"
            )

        if record["status"] == "COMPLETED":
            return {
                "status": "skipped",
                "batch_id": batch_id,
                "reason": "already_completed",
            }

        if record["status"] == "RUNNING":
            raise RuntimeError(
                f"Batch {batch_id} is RUNNING; "
                "manual recovery verification is required"
            )

    record = register_source_batch(
        spark,
        batch,
        expected_sha256=expected_sha256,
        allow_writes=True,
    )

    if not should_process(record):
        raise RuntimeError(
            f"Batch {batch_id} cannot be processed "
            f"from status {record['status']}"
        )

    transition_source_batch(
        spark,
        batch_id,
        "RUNNING",
        allow_writes=True,
    )

    try:
        with prepare_verified_bronze(
            spark,
            s3_client,
            batch,
            expected_sha256=expected_sha256,
        ) as prepared:
            result = write_bronze_batch(
                prepared["dataframe"],
                batch["source_key"],
                batch_id,
                allow_writes=True,
            )

        transition_source_batch(
            spark,
            batch_id,
            "COMPLETED",
            allow_writes=True,
        )

        return {
            "status": "completed",
            "batch_id": batch_id,
            "bronze": result,
        }

    except Exception:
        # A Delta commit may have succeeded before an error was raised.
        # Preserve RUNNING until the target and ledger are reconciled.
        raise
