from src.glue.gold_refresh import validate_gold_target
"""Validated, freshness-aware Delta MERGE for Gold dimensions and facts."""

from delta.tables import DeltaTable
from pyspark.sql import functions as F

GOLD_KEYS = {
    "dim_customers": "customer_key",
    "dim_products": "product_key",
    "dim_suppliers": "supplier_key",
    "dim_stores": "store_key",
    "fact_sales": "transaction_key",
    "fact_inventory": "inventory_key",
}


def validate_gold_source(df, table):
    if table not in GOLD_KEYS:
        raise ValueError(f"Unsupported Gold MERGE table: {table}")

    key = GOLD_KEYS[table]
    required = {key, "ingestion_timestamp"}
    missing = required - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing Gold MERGE columns: {sorted(missing)}"
        )

    if df.filter(
        F.col(key).isNull()
        | (F.trim(F.col(key).cast("string")) == "")
    ).limit(1).count():
        raise ValueError("Gold MERGE source has null or blank keys")

    if df.filter(
        F.col("ingestion_timestamp").isNull()
    ).limit(1).count():
        raise ValueError("Gold MERGE source has null timestamps")

    if (
        df.groupBy(key)
        .count()
        .filter(F.col("count") > 1)
        .limit(1)
        .count()
    ):
        raise ValueError("Gold MERGE source has duplicate business keys")

    return key


def merge_gold_table(
    spark, df, table, target_path, *, full_snapshot=False
):
    """Create or merge a Gold dimension/fact using freshness precedence."""
    if not isinstance(target_path, str) or not target_path.strip():
        raise ValueError("A Gold target path is required")

    validate_gold_target(target_path)
    if table == "fact_sales":
        if full_snapshot is not True:
            raise ValueError(
                "Sales reconciliation requires an explicit full snapshot"
            )
        if df.limit(1).count() == 0:
            raise ValueError(
                "Empty sales snapshot cannot reconcile Gold"
            )

    key = validate_gold_source(df, table)

    if not DeltaTable.isDeltaTable(spark, target_path):
        (
            df.write
            .format("delta")
            .mode("errorifexists")
            .save(target_path)
        )
        return {"status": "created", "table": table}

    target = DeltaTable.forPath(spark, target_path)

    merge = (
        target.alias("target")
        .merge(
            df.alias("source"),
            f"target.`{key}` = source.`{key}`",
        )
    )

    if table == "dim_customers":
        # Silver's current customer state is authoritative.
        # This preserves completeness-based updates accepted by Silver.
        merge = merge.whenMatchedUpdateAll()
    else:
        merge = merge.whenMatchedUpdateAll(
            condition=(
                "source.ingestion_timestamp "
                "> target.ingestion_timestamp"
            )
        )

    merge = merge.whenNotMatchedInsertAll()

    if table == "fact_sales":
        # Full valid-Silver snapshot reconciliation.
        # Transactions no longer present in Silver are removed from Gold.
        merge = merge.whenNotMatchedBySourceDelete()

    merge.execute()

    return {"status": "merged", "table": table}
