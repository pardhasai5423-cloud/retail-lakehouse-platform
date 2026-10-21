"""Unit tests for valid/quarantine sales reconciliation."""

from unittest.mock import MagicMock, patch

import pytest

from src.glue.sales_reconciliation import remove_stale_sales_records


def test_missing_target_path_rejected():
    with pytest.raises(ValueError, match="opposite_target_path"):
        remove_stale_sales_records(
            MagicMock(),
            MagicMock(),
            "",
        )


def test_missing_columns_rejected():
    df = MagicMock()
    df.columns = ["transaction_key"]

    with pytest.raises(ValueError, match="Missing reconciliation columns"):
        remove_stale_sales_records(
            MagicMock(),
            df,
            "/tmp/silver/sales",
        )


def test_missing_opposite_table_is_safe():
    spark = MagicMock()
    df = MagicMock()
    df.columns = ["transaction_key", "ingestion_timestamp"]

    with patch(
        "src.glue.sales_reconciliation.DeltaTable.isDeltaTable",
        return_value=False,
    ):
        result = remove_stale_sales_records(
            spark,
            df,
            "/tmp/silver/sales",
        )

    assert result["status"] == "not_found"


def test_existing_opposite_table_uses_merge_delete():
    spark = MagicMock()
    df = MagicMock()
    df.columns = ["transaction_key", "ingestion_timestamp"]

    with (
        patch(
            "src.glue.sales_reconciliation.DeltaTable.isDeltaTable",
            return_value=True,
        ),
        patch(
            "src.glue.sales_reconciliation.DeltaTable.forPath"
        ) as for_path,
    ):
        result = remove_stale_sales_records(
            spark,
            df,
            "/tmp/silver/sales_quarantine",
        )

    assert result["status"] == "reconciled"

    merge = for_path.return_value.alias.return_value.merge
    delete = merge.return_value.whenMatchedDelete

    delete.assert_called_once()
    assert "ingestion_timestamp >=" in (
        delete.call_args.kwargs["condition"]
    )
