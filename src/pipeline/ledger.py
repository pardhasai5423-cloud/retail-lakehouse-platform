"""Safety-checked processing ledger for the local lakehouse."""

from src.glue.delta_processing_ledger import (
    read_batch,
    register_batch,
    update_batch_status,
)
from src.pipeline.safety import (
    pipeline_path,
    validate_pipeline_path,
    require_write_approval,
)


def get_ledger_path():
    """Return the isolated Delta processing ledger location."""
    return pipeline_path("ledger", "processing_batches")


def validate_ledger_path(path):
    """Require the exact approved ledger destination."""
    validate_pipeline_path(path)

    if path != get_ledger_path():
        raise ValueError("Unapproved processing ledger path")

    return path


def get_batch_record(spark, batch_id):
    """Read a batch record without modifying S3."""
    path = validate_ledger_path(get_ledger_path())
    return read_batch(spark, path, batch_id)


def register_source_batch(
    spark,
    batch,
    *,
    expected_sha256,
    allow_writes=False,
):
    """Register one source object with its immutable identity."""
    require_write_approval(allow_writes)

    path = validate_ledger_path(get_ledger_path())

    source_identity = {
        batch["dataset"]: {
            "company": batch["company"],
            "source_key": batch["source_key"],
            "etag": batch["etag"],
            "sha256": expected_sha256,
        }
    }

    return register_batch(
        spark,
        path,
        batch["batch_id"],
        [batch["dataset"]],
        source_identity,
        allow_writes=True,
    )


def transition_source_batch(
    spark,
    batch_id,
    new_status,
    *,
    error=None,
    allow_writes=False,
):
    """Apply a validated ledger status transition."""
    require_write_approval(allow_writes)

    path = validate_ledger_path(get_ledger_path())

    return update_batch_status(
        spark,
        path,
        batch_id,
        new_status,
        error=error,
        allow_writes=True,
    )
