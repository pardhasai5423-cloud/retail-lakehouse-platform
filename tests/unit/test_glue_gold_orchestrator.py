"""Gold orchestration ordering and safety tests."""

from unittest.mock import MagicMock, patch

import pytest

from src.glue.gold_planner import GOLD_ORDER
from src.glue.jobs.gold_orchestrator import (
    plan_gold_pipeline,
    execute_gold_pipeline,
    AGGREGATE_TABLES,
    DIRECT_TABLES,
)


def test_all_eleven_tables_covered():
    assert len(GOLD_ORDER) == 11
    assert set(GOLD_ORDER) == (
        AGGREGATE_TABLES
        | DIRECT_TABLES
        | {"dim_date"}
    )


def test_all_datasets_plan_in_dependency_order():
    datasets = [
        "customers", "products", "suppliers",
        "stores", "sales", "inventory",
    ]

    plan = plan_gold_pipeline(
        datasets, "/tmp/silver", "/tmp/gold"
    )

    assert tuple(plan["tables"]) == tuple(GOLD_ORDER)


def test_dry_run_performs_no_spark_reads():
    spark = MagicMock()

    result = execute_gold_pipeline(
        spark,
        ["sales"],
        "/tmp/silver",
        "/tmp/gold",
    )

    assert result["status"] == "dry_run"
    spark.read.format.assert_not_called()


def test_protected_original_dataset_rejected():
    with pytest.raises(ValueError, match="Protected"):
        plan_gold_pipeline(
            ["sales"],
            "/tmp/silver",
            "s3://bucket/test-runs/run-001/gold",
        )


def test_empty_changes_rejected():
    with pytest.raises(
        ValueError,
        match="At least one changed dataset",
    ):
        plan_gold_pipeline(
            [], "/tmp/silver", "/tmp/gold"
        )


def test_all_execution_types_dispatched():
    with (
        patch(
            "src.glue.jobs.gold_orchestrator.execute_gold_table"
        ) as direct,
        patch(
            "src.glue.jobs.gold_orchestrator.execute_date_refresh"
        ) as dates,
        patch(
            "src.glue.jobs.gold_orchestrator.execute_aggregate_refresh"
        ) as aggregate,
    ):
        direct.return_value = {"status": "merged"}
        dates.return_value = {"status": "merged"}
        aggregate.return_value = {"status": "refreshed"}

        result = execute_gold_pipeline(
            MagicMock(),
            [
                "customers", "products", "suppliers",
                "stores", "sales", "inventory",
            ],
            "/tmp/silver",
            "/tmp/gold",
            allow_writes=True,
        )

        assert result["status"] == "completed"
        assert direct.call_count == 6
        assert dates.call_count == 1
        assert aggregate.call_count == 4


def test_sales_aggregations_follow_fact_sales():
    plan = plan_gold_pipeline(
        ["sales"], "/tmp/silver", "/tmp/gold"
    )

    tables = plan["tables"]

    assert tables.index("fact_sales") < tables.index(
        "agg_daily_sales"
    )
    assert tables.index("fact_sales") < tables.index(
        "agg_store_performance"
    )
    assert tables.index("fact_sales") < tables.index(
        "agg_category_sales"
    )
