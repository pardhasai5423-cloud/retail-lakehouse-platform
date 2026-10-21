from src.glue.bronze_writer import get_bronze_target
from src.glue.jobs.bronze_coordinator import plan_bronze_execution
"""Single-coordinator Bronze -> Silver -> Gold batch orchestration.

Requires one coordinator per ledger and target namespace.
No distributed lock or cross-table atomicity is provided.
"""

from src.glue.delta_processing_ledger import (
    register_batch,
    update_batch_status,
)
from src.glue.jobs.bronze_execution import execute_bronze_batch
from src.glue.jobs.silver_execution import execute_silver_batch
from src.glue.jobs.gold_orchestrator import execute_gold_pipeline


def execute_end_to_end_batch(
    spark,
    s3_client,
    *,
    batch_id,
    company,
    dataset,
    source_key,
    version_id,
    ledger_path,
    bronze_path,
    silver_base,
    gold_base,
    expected_sha256=None,
    allow_writes=False,
):
    """Run one version-pinned incoming dataset through all three layers."""

    if not isinstance(version_id, str) or not version_id.strip():
        raise ValueError("A nonempty S3 VersionId is required")

    if not isinstance(batch_id, str) or not batch_id.strip():
        raise ValueError("A nonempty batch_id is required")

    source_identity = {
        dataset: {
            "company": company,
            "source_key": source_key,
            "version_id": version_id,
            "expected_sha256": expected_sha256,
        }
    }

    if allow_writes:
        bronze_plan = plan_bronze_execution(
            s3_client, source_key, version_id
        )

        if bronze_plan["batch_id"] != batch_id:
            raise ValueError(
                "Coordinator batch ID does not match "
                "the source object's generated batch ID"
            )

        expected_bronze_path = get_bronze_target(source_key)

        if expected_bronze_path != bronze_path:
            raise ValueError(
                "Coordinator Bronze path does not match "
                "the approved Bronze destination"
            )

    record = register_batch(
        spark,
        ledger_path,
        batch_id,
        [dataset],
        source_identity,
        allow_writes=allow_writes,
    )

    if record["status"] == "COMPLETED":
        return {
            "status": "skipped",
            "batch_id": batch_id,
            "reason": "already_completed",
        }

    if record["status"] == "RUNNING":
        raise RuntimeError(
            f"Batch {batch_id} is already RUNNING; "
            "manual recovery is required"
        )

    if not allow_writes:
        return {
            "status": "dry_run",
            "batch_id": batch_id,
            "dataset": dataset,
            "source_identity": source_identity,
            "gold_plan": execute_gold_pipeline(
                spark,
                [dataset],
                silver_base,
                gold_base,
                allow_writes=False,
            ),
        }

    update_batch_status(
        spark,
        ledger_path,
        batch_id,
        "RUNNING",
        allow_writes=True,
    )

    try:
        bronze_result = execute_bronze_batch(
            spark,
            s3_client,
            source_key,
            version_id,
            allow_writes=True,
            expected_sha256=expected_sha256,
        )

        if bronze_result.get("execution_ready") is not True:
            raise RuntimeError(
                f"Bronze did not confirm successful execution: {bronze_result}"
            )

        if bronze_result.get("batch_id") != batch_id:
            raise RuntimeError(
                "Bronze batch ID does not match the ledger batch ID"
            )

        if bronze_result.get("bronze_target") != bronze_path:
            raise RuntimeError(
                "Bronze target does not match the Silver input path"
            )

        silver_result = execute_silver_batch(
            spark,
            company,
            dataset,
            batch_id,
            bronze_path,
            silver_base,
            allow_writes=True,
        )

        if silver_result.get("status") not in (
            "completed",
            "success",
            "merged",
        ):
            raise RuntimeError(
                f"Silver did not confirm success: {silver_result}"
            )

        gold_result = execute_gold_pipeline(
            spark,
            [dataset],
            silver_base,
            gold_base,
            allow_writes=True,
        )

        if gold_result.get("status") != "completed":
            raise RuntimeError(
                f"Gold did not confirm success: {gold_result}"
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
            "bronze": bronze_result,
            "silver": silver_result,
            "gold": gold_result,
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
