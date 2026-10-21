"""Verify sales write and reconciliation ordering."""

from unittest.mock import MagicMock, patch, call

import pytest

from src.glue.jobs.silver_execution import execute_silver_batch


def execute_sales(spark):
    return execute_silver_batch(
        spark,
        "retailmax",
        "sales",
        "batch-001",
        "/tmp/bronze",
        "/tmp/silver",
        allow_writes=True,
    )


def test_reconciliation_runs_after_both_writes():
    spark = MagicMock()
    valid = MagicMock()
    quarantine = MagicMock()

    operations = []

    with (
        patch(
            "src.glue.jobs.silver_execution.standardize_silver_batch"
        ),
        patch(
            "src.glue.jobs.silver_execution.filter_stale_cross_table_records",
            side_effect=lambda spark, df, path: df,
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
            "src.glue.jobs.silver_execution.merge_sales_quarantine",
            side_effect=lambda *args: operations.append("write_quarantine"),
        ),
        patch(
            "src.glue.jobs.silver_execution.merge_silver_batch",
            side_effect=lambda *args: operations.append("write_valid"),
        ),
        patch(
            "src.glue.jobs.silver_execution.remove_stale_sales_records",
            side_effect=lambda *args: operations.append(
                f"reconcile:{args[2]}"
            ),
        ),
    ):
        result = execute_sales(spark)

    assert operations == [
        "write_quarantine",
        "write_valid",
        "reconcile:/tmp/silver/sales_quarantine",
        "reconcile:/tmp/silver/sales",
    ]

    assert result["status"] == "completed"


def test_failed_valid_merge_skips_reconciliation():
    spark = MagicMock()

    with (
        patch(
            "src.glue.jobs.silver_execution.standardize_silver_batch"
        ),
        patch(
            "src.glue.jobs.silver_execution.filter_stale_cross_table_records",
            side_effect=lambda spark, df, path: df,
        ),
        patch(
            "src.glue.jobs.silver_execution.DeltaTable.isDeltaTable",
            return_value=True,
        ),
        patch(
            "src.glue.jobs.silver_execution.clean_incremental_sales_batch",
            return_value=(MagicMock(), MagicMock()),
        ),
        patch(
            "src.glue.jobs.silver_execution.merge_sales_quarantine"
        ),
        patch(
            "src.glue.jobs.silver_execution.merge_silver_batch",
            side_effect=RuntimeError("Silver MERGE failed"),
        ),
        patch(
            "src.glue.jobs.silver_execution.remove_stale_sales_records"
        ) as reconcile,
    ):
        with pytest.raises(RuntimeError, match="Silver MERGE failed"):
            execute_sales(spark)

    reconcile.assert_not_called()
