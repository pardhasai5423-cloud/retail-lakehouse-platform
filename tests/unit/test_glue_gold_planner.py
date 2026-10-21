"""Tests for Gold dependency planning and builder dispatch."""

from unittest.mock import MagicMock, patch

import pytest

from src.glue.gold_planner import (
    GOLD_ORDER,
    BUILDERS,
    plan_gold_refresh,
    build_gold_table,
    get_gold_source,
)


@pytest.mark.parametrize(
    "dataset,expected",
    [
        ("customers", {"dim_customers", "dim_date"}),
        ("products", {
            "dim_products",
            "agg_category_sales",
            "agg_inventory_status",
        }),
        ("suppliers", {"dim_suppliers"}),
        ("stores", {
            "dim_stores",
            "agg_store_performance",
        }),
        ("inventory", {
            "dim_date",
            "fact_inventory",
            "agg_inventory_status",
        }),
        ("sales", {
            "dim_date",
            "fact_sales",
            "agg_daily_sales",
            "agg_store_performance",
            "agg_category_sales",
        }),
    ],
)
def test_dataset_impacts(dataset, expected):
    plan = plan_gold_refresh([dataset])
    assert set(plan) == expected
    assert plan == tuple(t for t in GOLD_ORDER if t in expected)


def test_all_datasets_cover_all_gold_tables():
    plan = plan_gold_refresh([
        "customers", "products", "suppliers",
        "stores", "inventory", "sales",
    ])
    assert plan == GOLD_ORDER
    assert len(plan) == 11


def test_combined_changes_do_not_duplicate_outputs():
    plan = plan_gold_refresh(["sales", "sales", "products"])
    assert len(plan) == len(set(plan))
    assert "agg_category_sales" in plan


@pytest.mark.parametrize(
    "invalid",
    [[], ["unknown"], "sales", None],
)
def test_invalid_planning_input(invalid):
    with pytest.raises(ValueError):
        plan_gold_refresh(invalid)


@pytest.mark.parametrize("table", sorted(BUILDERS))
def test_existing_builder_dispatch(table):
    df = MagicMock()
    expected = MagicMock()

    with patch(
        "src.glue.gold_planner.import_module"
    ) as importer:
        module = importer.return_value
        function_name = BUILDERS[table][1]
        getattr(module, function_name).return_value = expected

        result = build_gold_table(table, df)

    assert result is expected
    getattr(module, function_name).assert_called_once_with(df)


@pytest.mark.parametrize("table", sorted(BUILDERS))
def test_builder_has_silver_source(table):
    assert get_gold_source(table) in {
        "customers", "products", "suppliers",
        "stores", "sales", "inventory",
    }


def test_aggregation_requires_separate_builder():
    with pytest.raises(ValueError):
        build_gold_table("agg_daily_sales", MagicMock())


def test_aggregation_has_no_direct_silver_source():
    with pytest.raises(ValueError):
        get_gold_source("agg_category_sales")
