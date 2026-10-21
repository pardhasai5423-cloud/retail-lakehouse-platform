"""Verify sales quarantine and valid Silver MERGE wiring."""

from unittest.mock import MagicMock, patch

from src.glue.jobs.silver_execution import execute_silver_batch


def test_sales_persists_quarantine_before_valid_merge():
    spark = MagicMock()
    valid = MagicMock()
    quarantine = MagicMock()

    calls = []

    def quarantine_writer(*args):
        calls.append(("quarantine", args[2]))
        return {"status": "merged"}

    def silver_writer(*args):
        calls.append(("silver", args[3]))
        return {"status": "merged"}

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
            side_effect=quarantine_writer,
        ),
        patch(
            "src.glue.jobs.silver_execution.merge_silver_batch",
            side_effect=silver_writer,
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

    assert calls == [
        ("quarantine", "/tmp/silver/sales_quarantine"),
        ("silver", "/tmp/silver/sales"),
    ]

    assert result["status"] == "completed"


def test_quarantine_failure_prevents_valid_sales_merge():
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
            "src.glue.jobs.silver_execution.merge_sales_quarantine",
            side_effect=RuntimeError("Quarantine write failed"),
        ),
        patch(
            "src.glue.jobs.silver_execution.merge_silver_batch"
        ) as silver_merge,
    ):
        try:
            execute_silver_batch(
                spark,
                "retailmax",
                "sales",
                "batch-001",
                "/tmp/bronze",
                "/tmp/silver",
                allow_writes=True,
            )
        except RuntimeError as error:
            assert "Quarantine write failed" in str(error)
        else:
            raise AssertionError("Expected quarantine failure")

    silver_merge.assert_not_called()
