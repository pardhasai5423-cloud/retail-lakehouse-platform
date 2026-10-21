"""Fast tests for Gold aggregation builder registration."""

import pytest

from src.glue.gold_aggregations import (
    AGGREGATION_BUILDERS,
    build_daily_sales,
    build_store_performance,
    build_category_sales,
    build_inventory_status,
)


def test_all_four_aggregations_registered():
    assert set(AGGREGATION_BUILDERS) == {
        "agg_daily_sales",
        "agg_store_performance",
        "agg_category_sales",
        "agg_inventory_status",
    }


@pytest.mark.parametrize(
    "table,builder",
    [
        ("agg_daily_sales", build_daily_sales),
        ("agg_store_performance", build_store_performance),
        ("agg_category_sales", build_category_sales),
        ("agg_inventory_status", build_inventory_status),
    ],
)
def test_builder_registration(table, builder):
    assert AGGREGATION_BUILDERS[table] is builder


def test_aggregations_do_not_use_direct_silver_merge():
    from src.glue.gold_writer import GOLD_KEYS

    assert not set(AGGREGATION_BUILDERS).intersection(GOLD_KEYS)
