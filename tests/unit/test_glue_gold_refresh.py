"""Gold date and aggregation refresh safety tests."""

from unittest.mock import MagicMock, patch

import pytest

from src.glue.gold_refresh import (
    AGGREGATION_INPUTS,
    validate_gold_target,
    execute_date_refresh,
    execute_aggregate_refresh,
)


@pytest.mark.parametrize(
    "path",
    [
        "s3://bucket/test-runs/run-001/gold/dim_date",
        "s3a://bucket/test-runs/run-001/gold/agg_daily_sales",
        "/tmp/test-runs/run-001/gold/dim_date",
        "s3://bucket/test-runs/run-001/../run-002/gold/dim_date",
    ],
)
def test_protected_or_unsafe_target_rejected(path):
    with pytest.raises(ValueError):
        validate_gold_target(path)


@pytest.mark.parametrize(
    "path",
    [
        "",
        None,
        "s3://bucket/random/path",
        "https://example.com/gold/dim_date",
        "s3:///gold/dim_date",
    ],
)
def test_invalid_target_rejected(path):
    with pytest.raises(ValueError):
        validate_gold_target(path)


def test_valid_incremental_target():
    path = (
        "s3://retail-lakehouse-parsam-2026/"
        "test-runs/glue-incremental-v1/gold/dim_date"
    )
    assert validate_gold_target(path) == path


def test_all_aggregate_inputs_registered():
    assert set(AGGREGATION_INPUTS) == {
        "agg_daily_sales",
        "agg_store_performance",
        "agg_category_sales",
        "agg_inventory_status",
    }


def test_date_dry_run_performs_no_reads():
    spark = MagicMock()

    result = execute_date_refresh(
        spark,
        "/tmp/silver",
        "/tmp/gold",
    )

    assert result["status"] == "dry_run"
    spark.read.format.assert_not_called()


def test_aggregate_dry_run_performs_no_reads():
    spark = MagicMock()

    result = execute_aggregate_refresh(
        spark,
        "agg_category_sales",
        "/tmp/gold",
    )

    assert result["status"] == "dry_run"
    spark.read.format.assert_not_called()


def test_aggregate_refresh_uses_current_gold_inputs():
    spark = MagicMock()

    with patch.dict(
        "src.glue.gold_refresh.AGGREGATION_BUILDERS",
        {"agg_daily_sales": MagicMock()},
    ) as builders:
        result = execute_aggregate_refresh(
            spark,
            "agg_daily_sales",
            "/tmp/gold",
            allow_writes=True,
        )

    assert result["status"] == "refreshed"
    spark.read.format.assert_called_once_with("delta")
    spark.read.format.return_value.load.assert_called_once_with(
        "/tmp/gold/fact_sales"
    )
    assert "agg_daily_sales" in builders


def test_unsupported_aggregate_rejected():
    with pytest.raises(ValueError):
        execute_aggregate_refresh(
            MagicMock(),
            "fact_sales",
            "/tmp/gold",
        )
