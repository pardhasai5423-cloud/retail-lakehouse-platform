"""Unit tests for validated Gold Delta MERGE."""

from unittest.mock import MagicMock, patch

import pytest

from src.glue.gold_writer import (
    GOLD_KEYS,
    validate_gold_source,
    merge_gold_table,
)


@pytest.mark.parametrize("table,key", GOLD_KEYS.items())
def test_gold_business_keys(table, key):
    assert GOLD_KEYS[table] == key


def test_unsupported_table_rejected():
    with pytest.raises(ValueError, match="Unsupported"):
        validate_gold_source(MagicMock(), "agg_daily_sales")


def test_missing_columns_rejected():
    df = MagicMock()
    df.columns = ["customer_key"]

    with pytest.raises(ValueError, match="Missing"):
        validate_gold_source(df, "dim_customers")


@pytest.mark.parametrize("path", ["", " ", None])
def test_invalid_target_rejected(path):
    with pytest.raises(ValueError, match="target path"):
        merge_gold_table(
            MagicMock(), MagicMock(), "dim_customers", path
        )


def test_duplicate_keys_rejected():
    df = MagicMock()
    df.columns = ["customer_key", "ingestion_timestamp"]

    # Both null checks pass.
    df.filter.return_value.limit.return_value.count.return_value = 0

    # Duplicate business key check fails.
    duplicate_rows = MagicMock()
    duplicate_rows.filter.return_value.limit.return_value.count.return_value = 1
    df.groupBy.return_value.count.return_value = duplicate_rows

    with patch("src.glue.gold_writer.F.col") as mock_col, patch(
        "src.glue.gold_writer.F.trim"
    ):
        mock_col.return_value.__gt__.return_value = MagicMock()

        with pytest.raises(ValueError, match="duplicate"):
            validate_gold_source(df, "dim_customers")



def test_null_keys_rejected():
    df = MagicMock()
    df.columns = ["customer_key", "ingestion_timestamp"]
    df.filter.return_value.limit.return_value.count.return_value = 1

    with patch("src.glue.gold_writer.F.col"), patch(
        "src.glue.gold_writer.F.trim"
    ):
        with pytest.raises(ValueError, match="null or blank"):
            validate_gold_source(df, "dim_customers")


def test_new_gold_table_created():
    spark = MagicMock()
    df = MagicMock()

    with (
        patch(
            "src.glue.gold_writer.validate_gold_source",
            return_value="customer_key",
        ),
        patch(
            "src.glue.gold_writer.DeltaTable.isDeltaTable",
            return_value=False,
        ),
    ):
        result = merge_gold_table(
            spark, df, "dim_customers", "/tmp/gold/dim_customers"
        )

    assert result["status"] == "created"
    df.write.format.assert_called_once_with("delta")


def test_existing_gold_table_merged():
    spark = MagicMock()
    df = MagicMock()

    with (
        patch(
            "src.glue.gold_writer.validate_gold_source",
            return_value="transaction_key",
        ),
        patch(
            "src.glue.gold_writer.DeltaTable.isDeltaTable",
            return_value=True,
        ),
        patch(
            "src.glue.gold_writer.DeltaTable.forPath"
        ) as for_path,
    ):
        result = merge_gold_table(
            spark, df, "fact_sales", "/tmp/gold/fact_sales",
                full_snapshot=True
        )

    assert result["status"] == "merged"

    merge_builder = (
        for_path.return_value.alias.return_value.merge.return_value
    )
    merge_builder.whenMatchedUpdateAll.assert_called_once_with(
        condition=(
            "source.ingestion_timestamp "
            "> target.ingestion_timestamp"
        )
    )
