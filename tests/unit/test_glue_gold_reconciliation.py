"""Gold MERGE reconciliation tests."""

from unittest.mock import MagicMock, patch

import pytest

from src.glue.gold_writer import merge_gold_table


@pytest.mark.parametrize(
    "table",
    ["dim_customers", "fact_sales", "dim_products"],
)
def test_gold_reconciliation_rules(table):
    spark = MagicMock()
    df = MagicMock()

    with (
        patch(
            "src.glue.gold_writer.validate_gold_source",
            return_value={
                "dim_customers": "customer_key",
                "fact_sales": "transaction_key",
                "dim_products": "product_key",
            }[table],
        ),
        patch(
            "src.glue.gold_writer.DeltaTable"
        ) as delta,
    ):
        delta.isDeltaTable.return_value = True

        merge = (
            delta.forPath.return_value
            .alias.return_value
            .merge.return_value
        )

        merge.whenMatchedUpdateAll.return_value = merge
        merge.whenNotMatchedInsertAll.return_value = merge
        merge.whenNotMatchedBySourceDelete.return_value = merge

        result = merge_gold_table(
            spark,
            df,
            table,
            f"/tmp/gold/{table}",
            full_snapshot=(table == "fact_sales"),
        )

        assert result["status"] == "merged"

        if table == "dim_customers":
            merge.whenMatchedUpdateAll.assert_called_once_with()
        else:
            merge.whenMatchedUpdateAll.assert_called_once()

        if table == "fact_sales":
            merge.whenNotMatchedBySourceDelete.assert_called_once_with()
        else:
            merge.whenNotMatchedBySourceDelete.assert_not_called()


def test_protected_sales_reconciliation_rejected():
    with pytest.raises(ValueError, match="Protected"):
        merge_gold_table(
            MagicMock(),
            MagicMock(),
            "fact_sales",
            "s3://bucket/test-runs/run-001/gold/fact_sales",
        )
