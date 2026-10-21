"""Unit tests for Gold execution planning and write gating."""

from unittest.mock import MagicMock, patch

import pytest

from src.glue.jobs.gold_execution import (
    plan_gold_execution,
    execute_gold_table,
)


def test_plan_sales_gold_execution():
    plan = plan_gold_execution(
        ["sales"], "/tmp/lakehouse/gold"
    )

    assert plan["direct_tables"] == ("fact_sales",)
    assert set(plan["deferred_tables"]) == {
        "dim_date",
        "agg_daily_sales",
        "agg_store_performance",
        "agg_category_sales",
    }


def test_plan_all_gold_outputs():
    plan = plan_gold_execution(
        [
            "customers", "products", "suppliers",
            "stores", "inventory", "sales",
        ],
        "/tmp/lakehouse/gold",
    )

    assert len(plan["tables"]) == 11
    assert len(plan["direct_tables"]) == 6
    assert len(plan["deferred_tables"]) == 5


@pytest.mark.parametrize(
    "invalid",
    ["", " ", None],
)
def test_invalid_gold_base_rejected(invalid):
    with pytest.raises(ValueError):
        plan_gold_execution(["sales"], invalid)


def test_dry_run_does_not_read_or_write():
    spark = MagicMock()

    with patch(
        "src.glue.jobs.gold_execution.merge_gold_table"
    ) as merge:
        result = execute_gold_table(
            spark,
            "fact_sales",
            "/tmp/silver",
            "/tmp/gold",
        )

    assert result["status"] == "dry_run"
    spark.read.format.assert_not_called()
    merge.assert_not_called()


def test_write_mode_calls_transform_and_merge():
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
            ) as build,
            patch(
                "src.glue.jobs.gold_execution.merge_gold_table"
            ) as merge,
        ):
            merge.return_value = {
                "status": "merged",
                "table": "fact_sales",
            }

            result = execute_gold_table(
                spark,
                "fact_sales",
                "/tmp/silver",
                "/tmp/gold",
                allow_writes=True,
            )

        assert result["status"] == "merged"
        build.assert_called_once()
        merge.assert_called_once()
        # Sales must use the pinned Delta snapshot reader.
        from src.glue.jobs import gold_execution
        gold_execution.read_pinned_snapshot.assert_called_once_with(
            spark, "/tmp/silver/sales", 7
        )


def test_aggregate_cannot_use_direct_executor():
    with pytest.raises(ValueError):
        execute_gold_table(
            MagicMock(),
            "agg_daily_sales",
            "/tmp/silver",
            "/tmp/gold",
        )


@pytest.mark.parametrize(
    "silver_base,gold_base",
    [
        ("", "/tmp/gold"),
        ("/tmp/silver", ""),
        (None, "/tmp/gold"),
        ("/tmp/silver", None),
    ],
)
def test_invalid_execution_paths(silver_base, gold_base):
    with pytest.raises(ValueError):
        execute_gold_table(
            MagicMock(),
            "fact_sales",
            silver_base,
            gold_base,
        )
