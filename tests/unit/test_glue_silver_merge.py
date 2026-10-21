from unittest.mock import MagicMock, patch

import pytest

from src.glue.silver_merge import (
    SILVER_KEYS,
    get_silver_key,
    merge_silver_batch,
)


@pytest.mark.parametrize("dataset,key", SILVER_KEYS.items())
def test_business_keys(dataset, key):
    assert get_silver_key(dataset) == key


def test_unknown_dataset():
    with pytest.raises(ValueError, match="Unsupported"):
        get_silver_key("payments")


def test_empty_target_rejected():
    with pytest.raises(ValueError, match="target_path"):
        merge_silver_batch(
            MagicMock(), MagicMock(), "customers", ""
        )


def test_merge_creates_table_when_missing():
    spark = MagicMock()
    df = MagicMock()
    target = "file:///tmp/test_silver_customers"

    with (
        patch(
            "src.glue.silver_merge.validate_merge_source",
            return_value="customer_key",
        ),
        patch(
            "src.glue.silver_merge.DeltaTable.isDeltaTable",
            return_value=False,
        ),
    ):
        result = merge_silver_batch(
            spark, df, "customers", target
        )

    assert result["status"] == "created"
    df.write.format.assert_called_with("delta")


def test_merge_existing_table():
    spark = MagicMock()
    df = MagicMock()
    target = "file:///tmp/test_silver_customers"

    with (
        patch(
            "src.glue.silver_merge.validate_merge_source",
            return_value="customer_key",
        ),
        patch(
            "src.glue.silver_merge.DeltaTable.isDeltaTable",
            return_value=True,
        ),
        patch(
            "src.glue.silver_merge.DeltaTable.forPath"
        ) as for_path,
    ):
        result = merge_silver_batch(
            spark, df, "customers", target
        )

    assert result["status"] == "merged"
    for_path.assert_called_once_with(spark, target)
