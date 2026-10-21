"""Incremental Silver Delta Lake MERGE operations."""

from delta.tables import DeltaTable
from pyspark.sql import functions as F

SILVER_KEYS = {
    "customers": "customer_key",
    "products": "product_key",
    "stores": "store_key",
    "sales": "transaction_key",
    "inventory": "inventory_key",
    "suppliers": "supplier_key",
}


def get_silver_key(dataset):
    if dataset not in SILVER_KEYS:
        raise ValueError(f"Unsupported Silver dataset: {dataset}")
    return SILVER_KEYS[dataset]


def validate_merge_source(df, dataset):
    """Reject ambiguous or unsafe merge inputs."""
    key = get_silver_key(dataset)

    required = {key, "ingestion_timestamp", "batch_id"}
    missing = required - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing Silver columns: {sorted(missing)}"
        )

    if df.filter(
        F.col(key).isNull() | (F.trim(F.col(key)) == "")
    ).limit(1).count():
        raise ValueError("Silver merge source contains null or blank business keys")

    if df.filter(
        F.col("ingestion_timestamp").isNull()
    ).limit(1).count():
        raise ValueError("Silver merge source contains null ingestion timestamps")

    if df.filter(
        F.col("batch_id").isNull() |
        (F.trim(F.col("batch_id")) == "")
    ).limit(1).count():
        raise ValueError("Silver merge source contains null or blank batch IDs")

    if df.select("batch_id").distinct().limit(2).count() > 1:
        raise ValueError("Silver merge source contains multiple batch IDs")

    if df.groupBy(key).count().filter("count > 1").limit(1).count():
        raise ValueError("Silver merge source contains duplicate business keys")

    return key


def merge_silver_batch(spark, df, dataset, target_path):
    """Insert new rows and update matching rows only when newer."""
    if not isinstance(target_path, str) or not target_path.strip():
        raise ValueError("A target_path is required")

    key = validate_merge_source(df, dataset)

    if not DeltaTable.isDeltaTable(spark, target_path):
        (
            df.write
            .format("delta")
            .mode("errorifexists")
            .save(target_path)
        )
        return {"status": "created", "target": target_path}

    target = DeltaTable.forPath(spark, target_path)

    update_condition = (
        "source.ingestion_timestamp > target.ingestion_timestamp"
    )

    if dataset == "customers":
        completeness_columns = (
            "first_name",
            "last_name",
            "email",
            "phone",
            "city",
            "state",
            "registration_date",
        )

        # Apply completeness precedence only when both sides
        # contain the existing customer business attributes.
        if set(completeness_columns).issubset(df.columns) and (
            set(completeness_columns).issubset(
                spark.read.format("delta").load(target_path).columns
            )
        ):
            def completeness(alias):
                return " + ".join(
                    f"CASE WHEN {alias}.`{column}` IS NOT NULL "
                    "THEN 1 ELSE 0 END"
                    for column in completeness_columns
                )

            source_score = completeness("source")
            target_score = completeness("target")

            update_condition = (
                f"(({source_score}) > ({target_score})) "
                f"OR (({source_score}) = ({target_score}) "
                "AND source.ingestion_timestamp > "
                "target.ingestion_timestamp)"
            )

    (
        target.alias("target")
        .merge(
            df.alias("source"),
            f"target.`{key}` = source.`{key}`"
        )
        .whenMatchedUpdateAll(condition=update_condition)
        .whenNotMatchedInsertAll()
        .execute()
    )

    return {"status": "merged", "target": target_path}
