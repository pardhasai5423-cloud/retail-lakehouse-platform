"""Unit tests for the sales quarantine Delta writer."""

from unittest.mock import MagicMock, patch

import pytest

from src.glue.sales_quarantine import merge_sales_quarantine


def test_quarantine_requires_target_path():
    with pytest.raises(ValueError, match="target_path"):
        merge_sales_quarantine(MagicMock(), MagicMock(), "")


def test_quarantine_initial_creation():
    spark = MagicMock()
    df = MagicMock()

    with (
        patch(
            "src.glue.sales_quarantine.validate_quarantine_batch"
        ),
        patch(
            "src.glue.sales_quarantine.DeltaTable.isDeltaTable",
            return_value=False,
        ),
    ):
        result = merge_sales_quarantine(
            spark, df, "/tmp/sales_quarantine"
        )

    assert result["status"] == "created"
    df.write.format.assert_called_once_with("delta")


def test_quarantine_existing_table_merge():
    spark = MagicMock()
    df = MagicMock()

    with (
        patch(
            "src.glue.sales_quarantine.validate_quarantine_batch"
        ),
        patch(
            "src.glue.sales_quarantine.DeltaTable.isDeltaTable",
            return_value=True,
        ),
        patch(
            "src.glue.sales_quarantine.DeltaTable.forPath"
        ) as for_path,
    ):
        result = merge_sales_quarantine(
            spark, df, "/tmp/sales_quarantine"
        )

    assert result["status"] == "merged"
    for_path.assert_called_once_with(
        spark, "/tmp/sales_quarantine"
    )

    (
        for_path.return_value.alias.return_value.merge
        .return_value.whenMatchedUpdateAll
        .assert_called_once()
    )


def test_quarantine_rejects_missing_columns():
    from src.glue.sales_quarantine import validate_quarantine_batch

    df = MagicMock()
    df.columns = ["transaction_key"]

    with pytest.raises(ValueError, match="Missing quarantine columns"):
        validate_quarantine_batch(df)
