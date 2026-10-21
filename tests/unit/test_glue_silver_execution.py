"""Unit tests for incremental Silver execution."""

from unittest.mock import MagicMock, patch

import pytest

from src.glue.jobs.silver_execution import execute_silver_batch


@pytest.mark.parametrize(
    "dataset",
    ["customers", "products", "stores", "sales", "inventory", "suppliers"],
)
def test_dry_run_does_not_touch_spark(dataset):
    spark = MagicMock()

    result = execute_silver_batch(
        spark,
        "retailmax",
        dataset,
        "batch-001",
        "/tmp/bronze",
        "/tmp/silver",
    )

    assert result["status"] == "dry_run"
    assert result["silver_target"] == f"/tmp/silver/{dataset}"
    spark.assert_not_called()
    spark.read.format.assert_not_called()


@pytest.mark.parametrize(
    "company,dataset",
    [
        ("unknown", "customers"),
        ("retailmax", "unknown"),
    ],
)
def test_invalid_company_or_dataset(company, dataset):
    with pytest.raises(ValueError):
        execute_silver_batch(
            MagicMock(),
            company,
            dataset,
            "batch-001",
            "/tmp/bronze",
            "/tmp/silver",
        )


def test_customer_batch_calls_merge():
    spark = MagicMock()

    with (
        patch(
            "src.glue.jobs.silver_execution.standardize_silver_batch"
        ) as standardize,
        patch(
            "src.glue.jobs.silver_execution.clean_silver_batch"
        ) as clean,
        patch(
            "src.glue.jobs.silver_execution.merge_silver_batch"
        ) as merge,
    ):
        result = execute_silver_batch(
            spark,
            "retailmax",
            "customers",
            "batch-001",
            "/tmp/bronze",
            "/tmp/silver",
            allow_writes=True,
        )

    standardize.assert_called_once()
    clean.assert_called_once()
    merge.assert_called_once()

    assert result["status"] == "completed"


def test_sales_requires_reference_tables():
    spark = MagicMock()

    with (
        patch(
            "src.glue.jobs.silver_execution.standardize_silver_batch"
        ),
        patch(
            "src.glue.jobs.silver_execution.DeltaTable.isDeltaTable",
            return_value=False,
        ),
    ):
        with pytest.raises(
            ValueError, match="Missing Silver reference table"
        ):
            execute_silver_batch(
                spark,
                "retailmax",
                "sales",
                "batch-001",
                "/tmp/bronze",
                "/tmp/silver",
                allow_writes=True,
            )


def test_sales_quarantine_is_persisted():
    """Sales quarantine must be written, not silently discarded."""
    spark = MagicMock()
    valid = MagicMock()
    quarantine = MagicMock()

    with (
        patch(
            "src.glue.jobs.silver_execution.standardize_silver_batch"
        ),
        patch(
            "src.glue.jobs.silver_execution.DeltaTable.isDeltaTable",
            return_value=True,
        ),
        patch(
            "src.glue.jobs.silver_execution.clean_incremental_sales_batch",
            return_value=(valid, quarantine),
        ),
        patch(
            "src.glue.jobs.silver_execution.filter_stale_cross_table_records",
            side_effect=lambda spark, df, path: df,
        ),
        patch(
            "src.glue.jobs.silver_execution.merge_sales_quarantine"
        ) as quarantine_merge,
        patch(
            "src.glue.jobs.silver_execution.merge_silver_batch"
        ),
        patch(
            "src.glue.jobs.silver_execution.remove_stale_sales_records"
        ),
    ):
        result = execute_silver_batch(
            spark,
            "retailmax",
            "sales",
            "batch-001",
            "/tmp/bronze",
            "/tmp/silver",
            allow_writes=True,
        )

    quarantine_merge.assert_called_once_with(
        spark,
        quarantine,
        "/tmp/silver/sales_quarantine",
    )

    assert result["status"] == "completed"
