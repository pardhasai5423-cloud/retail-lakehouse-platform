"""Incremental Bronze-to-Silver execution coordinator."""

from delta.tables import DeltaTable

from src.glue.jobs.silver_incremental import (
    DATASETS,
    standardize_silver_batch,
    clean_silver_batch,
    clean_incremental_sales_batch,
)
from src.glue.silver_merge import merge_silver_batch
from src.glue.sales_quarantine import merge_sales_quarantine
from src.glue.sales_reconciliation import remove_stale_sales_records
from src.glue.sales_freshness import filter_stale_cross_table_records


COMPANIES = ("retailmax", "freshmart")
SALES_REFERENCES = ("customers", "products", "stores")


def execute_silver_batch(
    spark,
    company,
    dataset,
    batch_id,
    bronze_path,
    silver_base,
    *,
    allow_writes=False,
):
    """Process one company's Bronze batch into canonical Silver."""

    if company not in COMPANIES:
        raise ValueError(f"Unsupported company: {company}")

    if dataset not in DATASETS:
        raise ValueError(f"Unsupported dataset: {dataset}")

    if not isinstance(batch_id, str) or not batch_id.strip():
        raise ValueError("A non-empty batch_id is required")

    if not isinstance(bronze_path, str) or not bronze_path.strip():
        raise ValueError("A bronze_path is required")

    if not isinstance(silver_base, str) or not silver_base.strip():
        raise ValueError("A silver_base is required")

    if not isinstance(allow_writes, bool):
        raise ValueError("allow_writes must be a boolean")

    # Prevent accidental Spark reads/writes during dry-run.
    if not allow_writes:
        return {
            "status": "dry_run",
            "company": company,
            "dataset": dataset,
            "batch_id": batch_id,
            "bronze_path": bronze_path,
            "silver_target": f"{silver_base.rstrip('/')}/{dataset}",
            "requires_references": (
                list(SALES_REFERENCES) if dataset == "sales" else []
            ),
        }

    bronze_df = spark.read.format("delta").load(bronze_path)

    standardized = standardize_silver_batch(
        bronze_df,
        company,
        dataset,
        batch_id,
    )

    target = f"{silver_base.rstrip('/')}/{dataset}"

    if dataset == "sales":
        references = {}

        for reference in SALES_REFERENCES:
            reference_path = (
                f"{silver_base.rstrip('/')}/{reference}"
            )

            if not DeltaTable.isDeltaTable(spark, reference_path):
                raise ValueError(
                    f"Missing Silver reference table: {reference}"
                )

            references[reference] = (
                spark.read.format("delta").load(reference_path)
            )

        valid, quarantine = clean_incremental_sales_batch(
            standardized,
            references["customers"],
            references["products"],
            references["stores"],
        )

        quarantine_target = (
            f"{silver_base.rstrip('/')}/sales_quarantine"
        )

        # Reject stale incoming records when the opposite table
        # already holds an equally recent or newer version.
        valid = filter_stale_cross_table_records(
            spark,
            valid,
            quarantine_target,
        )
        quarantine = filter_stale_cross_table_records(
            spark,
            quarantine,
            target,
        )

        # Persist quarantine before merging valid sales.
        merge_sales_quarantine(
            spark,
            quarantine,
            quarantine_target,
        )

        cleaned = valid

    else:
        cleaned = clean_silver_batch(
            standardized,
            dataset,
        )

    result = merge_silver_batch(
        spark,
        cleaned,
        dataset,
        target,
    )

    if dataset == "sales":
        # Both destination writes must succeed before reconciliation.
        remove_stale_sales_records(
            spark,
            valid,
            quarantine_target,
        )
        remove_stale_sales_records(
            spark,
            quarantine,
            target,
        )

    return {
        "status": "completed",
        "company": company,
        "dataset": dataset,
        "batch_id": batch_id,
        "merge": result,
    }
