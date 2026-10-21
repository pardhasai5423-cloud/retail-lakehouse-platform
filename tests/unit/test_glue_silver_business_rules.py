from unittest.mock import MagicMock, patch

import pytest

from src.glue.jobs.silver_incremental import (
    clean_silver_batch,
    clean_incremental_sales_batch,
)


def test_inventory_filters_before_deduplication():
    module = MagicMock()
    df = MagicMock()

    with (
        patch("importlib.import_module", return_value=module),
        patch("src.glue.jobs.silver_incremental.F.col"),
    ):
        result = clean_silver_batch(df, "inventory")

    module.deduplicate_inventory.assert_called_once()

    assert result is module.deduplicate_inventory.return_value

    filtered = module.add_inventory_freshness_status.return_value.filter
    filtered.assert_called_once()


def test_sales_requires_references():
    with pytest.raises(ValueError, match="requires customer"):
        clean_silver_batch(MagicMock(), "sales")


def test_sales_reference_checks():
    module = MagicMock()

    with (
        patch("importlib.import_module", return_value=module),
        patch("src.glue.jobs.silver_incremental.F.col"),
    ):
        valid, quarantine = clean_incremental_sales_batch(
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
        )

    assert module.add_reference_status.call_count == 3

    keys = [
        call.args[2]
        for call in module.add_reference_status.call_args_list
    ]

    assert keys == [
        "customer_key",
        "product_key",
        "store_key",
    ]

    module.add_quality_flags.assert_called_once()

    classified = module.add_quality_flags.return_value
    assert classified.filter.call_count == 2
    assert valid is not None
    assert quarantine is not None
