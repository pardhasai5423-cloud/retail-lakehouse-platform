"""Path-restricted coordinator for incremental Silver processing."""

from src.glue.jobs.silver_execution import execute_silver_batch
from src.glue.jobs.silver_incremental import DATASETS
from src.pipeline.bronze_writer import get_bronze_target
from src.pipeline.safety import pipeline_path, validate_pipeline_path
from src.pipeline.silver_safety import (
    get_silver_target,
    require_silver_write_approval,
)

COMPANIES = frozenset({"retailmax", "freshmart"})
SILVER_BASE = pipeline_path("silver")


def execute_isolated_silver_batch(
    spark,
    batch,
    *,
    allow_writes=False,
):
    """Process one verified Bronze batch within isolated Silver paths."""
    if not isinstance(batch, dict):
        raise ValueError("A discovered source batch dictionary is required")

    company = batch.get("company")
    dataset = batch.get("dataset")
    batch_id = batch.get("batch_id")
    source_key = batch.get("source_key")

    if company not in COMPANIES:
        raise ValueError(f"Unsupported company: {company!r}")

    if dataset not in DATASETS:
        raise ValueError(f"Unsupported dataset: {dataset!r}")

    if not isinstance(batch_id, str) or not batch_id.strip():
        raise ValueError("A nonempty batch_id is required")

    if not isinstance(source_key, str) or not source_key.strip():
        raise ValueError("A nonempty source_key is required")

    expected_source = (
        f"test-runs/glue-incremental-v1/raw/"
        f"{company}/{dataset}/"
    )

    if not source_key.startswith(expected_source):
        raise ValueError("Source key does not match company/dataset")

    if "/" in source_key[len(expected_source):] or (
        not source_key.endswith(".csv")
    ):
        raise ValueError("Unexpected raw source object structure")

    bronze_path = get_bronze_target(source_key)
    validate_pipeline_path(bronze_path)

    silver_target = get_silver_target(dataset)

    destinations = [silver_target]

    if dataset == "sales":
        destinations.append(get_silver_target("sales_quarantine"))

    # Check every possible destination before any Spark operation.
    if allow_writes:
        for target in destinations:
            require_silver_write_approval(
                target,
                allow_writes=allow_writes,
            )

    result = execute_silver_batch(
        spark,
        company,
        dataset,
        batch_id,
        bronze_path,
        SILVER_BASE,
        allow_writes=allow_writes,
    )

    return {
        "company": company,
        "dataset": dataset,
        "batch_id": batch_id,
        "bronze_path": bronze_path,
        "silver_destinations": destinations,
        "execution": result,
    }
