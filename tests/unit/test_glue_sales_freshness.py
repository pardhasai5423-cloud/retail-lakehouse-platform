"""Unit tests for cross-table sales freshness protection."""

from unittest.mock import MagicMock, patch

import pytest

from src.glue.sales_freshness import (
    filter_stale_cross_table_records,
)


def test_missing_path_rejected():
    with pytest.raises(ValueError, match="opposite_target_path"):
        filter_stale_cross_table_records(
            MagicMock(), MagicMock(), ""
        )


def test_missing_columns_rejected():
    df = MagicMock()
    df.columns = ["transaction_key"]

    with pytest.raises(ValueError, match="Missing freshness columns"):
        filter_stale_cross_table_records(
            MagicMock(), df, "/tmp/silver/sales"
        )


def test_missing_opposite_table_returns_original():
    spark = MagicMock()
    df = MagicMock()
    df.columns = ["transaction_key", "ingestion_timestamp"]

    with patch(
        "src.glue.sales_freshness.DeltaTable.isDeltaTable",
        return_value=False,
    ):
        result = filter_stale_cross_table_records(
            spark, df, "/tmp/silver/sales"
        )

    assert result is df


class FakeColumn:
    """Minimal Spark Column substitute for unit testing."""

    def alias(self, name):
        return self

    def isNull(self):
        return self

    def __gt__(self, other):
        return self

    def __or__(self, other):
        return self


def test_existing_opposite_table_filters_records():
    spark = MagicMock()
    df = MagicMock()
    df.columns = ["transaction_key", "ingestion_timestamp"]

    with (
        patch(
            "src.glue.sales_freshness.DeltaTable.isDeltaTable",
            return_value=True,
        ),
        patch("src.glue.sales_freshness.F.col", side_effect=lambda name: FakeColumn()),
        patch("src.glue.sales_freshness.F.max", side_effect=lambda name: FakeColumn()),
    ):
        result = filter_stale_cross_table_records(
            spark, df, "/tmp/silver/sales"
        )

    assert result is not None
    df.join.assert_called_once()
