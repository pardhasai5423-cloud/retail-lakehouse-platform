"""Verify completeness-aware customer MERGE precedence."""

from unittest.mock import MagicMock, patch

from src.glue.silver_merge import merge_silver_batch


def test_customer_merge_uses_completeness_precedence():
    spark = MagicMock()
    df = MagicMock()

    completeness_columns = [
        "first_name", "last_name", "email", "phone",
        "city", "state", "registration_date",
    ]

    df.columns = [
        "customer_key", "ingestion_timestamp", "batch_id",
        *completeness_columns,
    ]

    spark.read.format.return_value.load.return_value.columns = (
        df.columns
    )

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
        merge_silver_batch(
            spark, df, "customers", "/tmp/silver_customers"
        )

    merge = for_path.return_value.alias.return_value.merge
    update = merge.return_value.whenMatchedUpdateAll

    condition = update.call_args.kwargs["condition"]

    assert "source.`first_name` IS NOT NULL" in condition
    assert "target.`first_name` IS NOT NULL" in condition
    assert "source.ingestion_timestamp >" in condition
    assert "target.ingestion_timestamp" in condition


def test_customer_merge_without_completeness_columns():
    spark = MagicMock()
    df = MagicMock()
    df.columns = [
        "customer_key", "ingestion_timestamp", "batch_id",
    ]

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
        merge_silver_batch(
            spark, df, "customers", "/tmp/silver_customers"
        )

    update = (
        for_path.return_value.alias.return_value
        .merge.return_value.whenMatchedUpdateAll
    )

    assert update.call_args.kwargs["condition"] == (
        "source.ingestion_timestamp > target.ingestion_timestamp"
    )
