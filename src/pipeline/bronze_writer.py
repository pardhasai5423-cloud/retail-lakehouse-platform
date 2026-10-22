"""Safety-checked Delta Bronze writer for the local pipeline."""

from src.glue.batch_planner import plan_batch
from src.glue.bronze_writer import get_transaction_identity
from src.pipeline.safety import (
    pipeline_path,
    validate_pipeline_path,
    require_write_approval,
)


def get_bronze_target(source_key):
    """Build the approved Bronze destination from a source key."""
    plan = plan_batch(source_key)

    target = pipeline_path(
        "bronze",
        plan.company,
        plan.dataset,
    )

    return validate_pipeline_path(target)


def write_bronze_batch(
    df,
    source_key,
    batch_id,
    *,
    allow_writes=False,
):
    """Append one source batch to its isolated Bronze Delta table."""
    require_write_approval(allow_writes)

    target = get_bronze_target(source_key)

    expected_prefix = (
        "s3a://retail-lakehouse-parsam-2026/"
        "test-runs/glue-incremental-v1/bronze/"
    )

    if not target.startswith(expected_prefix):
        raise ValueError("Bronze target outside approved prefix")

    app_id, txn_version = get_transaction_identity(
        source_key,
        batch_id,
    )

    (
        df.write
        .format("delta")
        .mode("append")
        .option("txnAppId", app_id)
        .option("txnVersion", txn_version)
        .save(target)
    )

    return {
        "target": target,
        "batch_id": batch_id,
        "transaction_app_id": app_id,
        "transaction_version": txn_version,
    }


def get_run_bronze_target(source_key, run_config):
    """Resolve a Bronze destination for a validated new run."""
    from src.glue.batch_planner import plan_run_batch
    from src.pipeline.run_config import RunConfig

    if not isinstance(run_config, RunConfig):
        raise TypeError("A validated RunConfig is required")

    plan = plan_run_batch(source_key, run_config)

    target = run_config.path(
        "bronze",
        plan.company,
        plan.dataset,
    )

    expected = (
        f"s3a://{run_config.bucket}/"
        f"{run_config.prefix}/bronze/"
    )

    if not target.startswith(expected):
        raise ValueError("Bronze target outside approved run")

    return target


def write_run_bronze_batch(
    df,
    source_key,
    batch_id,
    run_config,
    *,
    allow_writes=False,
):
    """Append a batch to a new run's Bronze Delta table.

    No writes occur unless explicitly enabled.
    """
    target = get_run_bronze_target(source_key, run_config)

    if allow_writes is not True:
        raise PermissionError(
            "Bronze writes require explicit authorization"
        )

    if not isinstance(batch_id, str) or not batch_id.strip():
        raise ValueError("A nonempty batch ID is required")

    app_id, txn_version = get_run_transaction_identity(
        source_key,
        batch_id,
        run_config,
    )

    (
        df.write
        .format("delta")
        .mode("append")
        .option("txnAppId", app_id)
        .option("txnVersion", txn_version)
        .save(target)
    )

    return {
        "target": target,
        "batch_id": batch_id,
        "transaction_app_id": app_id,
        "transaction_version": txn_version,
    }


def get_run_transaction_identity(source_key, batch_id, run_config):
    """Build a deterministic transaction identity for an isolated run."""
    import re
    from src.glue.batch_planner import plan_run_batch

    plan = plan_run_batch(source_key, run_config)

    if (
        not isinstance(batch_id, str)
        or re.fullmatch(r"batch-[0-9a-f]{16}", batch_id) is None
        or batch_id != plan.batch_id
    ):
        raise ValueError(
            "Batch ID must match the validated source key"
        )

    app_id = (
        f"retail-lakehouse-bronze-"
        f"{plan.company}-{plan.dataset}-{batch_id}"
    )

    return app_id, 1
