"""Full-snapshot reconciliation safety tests."""

from unittest.mock import MagicMock, patch

import pytest

from src.glue.gold_writer import merge_gold_table
from src.glue.jobs.gold_execution import execute_gold_table


def test_sales_requires_explicit_full_snapshot():
    with pytest.raises(ValueError, match="full snapshot"):
        merge_gold_table(
            MagicMock(),
            MagicMock(),
            "fact_sales",
            "/tmp/gold/fact_sales",
        )


def test_empty_sales_snapshot_rejected():
    df = MagicMock()
    df.limit.return_value.count.return_value = 0

    with pytest.raises(ValueError, match="Empty sales"):
        merge_gold_table(
            MagicMock(),
            df,
            "fact_sales",
            "/tmp/gold/fact_sales",
            full_snapshot=True,
        )


def test_nonempty_sales_snapshot_reconciles():
    df = MagicMock()
    df.limit.return_value.count.return_value = 1

    with (
        patch(
            "src.glue.gold_writer.validate_gold_source",
            return_value="transaction_key",
        ),
        patch("src.glue.gold_writer.DeltaTable") as delta,
    ):
        delta.isDeltaTable.return_value = True

        merge = (
            delta.forPath.return_value
            .alias.return_value
            .merge.return_value
        )
        merge.whenMatchedUpdateAll.return_value = merge
        merge.whenNotMatchedInsertAll.return_value = merge
        merge.whenNotMatchedBySourceDelete.return_value = merge

        result = merge_gold_table(
            MagicMock(),
            df,
            "fact_sales",
            "/tmp/gold/fact_sales",
            full_snapshot=True,
        )

        assert result["status"] == "merged"
        merge.whenNotMatchedBySourceDelete.assert_called_once()


def test_sales_executor_declares_full_snapshot():
    with (
        patch(
            "src.glue.jobs.gold_execution.current_delta_version",
            return_value=7,
        ),
        patch(
            "src.glue.jobs.gold_execution.read_pinned_snapshot",
        ),
        patch(
            "src.glue.jobs.gold_execution.assert_snapshot_unchanged",
            return_value=7,
        ),
    ):
        spark = MagicMock()

        with (
            patch(
                "src.glue.jobs.gold_execution.build_gold_table"
            ) as builder,
            patch(
                "src.glue.jobs.gold_execution.merge_gold_table"
            ) as writer,
        ):
            writer.return_value = {"status": "merged"}

            execute_gold_table(
                spark,
                "fact_sales",
                "/tmp/silver",
                "/tmp/gold",
                allow_writes=True,
            )

            assert writer.call_args.kwargs["full_snapshot"] is True


def test_customer_executor_does_not_declare_sales_snapshot():
    spark = MagicMock()

    with (
        patch(
            "src.glue.jobs.gold_execution.build_gold_table"
        ),
        patch(
            "src.glue.jobs.gold_execution.merge_gold_table"
        ) as writer,
    ):
        writer.return_value = {"status": "merged"}

        execute_gold_table(
            spark,
            "dim_customers",
            "/tmp/silver",
            "/tmp/gold",
            allow_writes=True,
        )

        assert writer.call_args.kwargs["full_snapshot"] is False
