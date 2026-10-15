
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.dirname(__file__))

from transform_customers import create_spark, BRONZE_BASE

from transform_inventory import (
    transform_retailmax_inventory,
    transform_freshmart_inventory,
)


def clean_identifiers(df):
    """
    Trim identifiers, convert blanks to null,
    and reconstruct source-qualified keys.
    """

    identifier_columns = [
        "source_inventory_id",
        "source_product_id",
        "source_store_id",
    ]

    for column_name in identifier_columns:
        df = df.withColumn(
            column_name,
            F.when(
                F.trim(F.col(column_name)) == "",
                F.lit(None)
            ).otherwise(
                F.trim(F.col(column_name))
            )
        )

    for source_column, key_column in [
        ("source_inventory_id", "inventory_key"),
        ("source_product_id", "product_key"),
        ("source_store_id", "store_key"),
    ]:
        df = df.withColumn(
            key_column,
            F.when(
                F.col(source_column).isNotNull(),
                F.concat(
                    F.col("source_system"),
                    F.lit(":"),
                    F.col(source_column)
                )
            )
        )

    return df


def add_validation_status(df):
    """
    Identify records that cannot be trusted
    because of missing IDs or invalid quantities.
    """

    return df.withColumn(
        "validation_status",
        F.when(
            F.col("source_inventory_id").isNull(),
            F.lit("MISSING_INVENTORY_ID")
        )
        .when(
            F.col("source_product_id").isNull(),
            F.lit("MISSING_PRODUCT_ID")
        )
        .when(
            F.col("source_store_id").isNull(),
            F.lit("MISSING_STORE_ID")
        )
        .when(
            F.col("stock_quantity").isNull(),
            F.lit("MISSING_STOCK_QUANTITY")
        )
        .when(
            F.col("stock_quantity") < 0,
            F.lit("NEGATIVE_STOCK")
        )
        .when(
            F.col("reorder_level").isNull(),
            F.lit("MISSING_REORDER_LEVEL")
        )
        .when(
            F.col("reorder_level") < 0,
            F.lit("NEGATIVE_REORDER_LEVEL")
        )
        .when(
            F.col("last_updated").isNull(),
            F.lit("MISSING_UPDATE_DATE")
        )
        .when(
            F.col("last_updated") > F.current_date(),
            F.lit("FUTURE_UPDATE_DATE")
        )
        .otherwise(F.lit("VALID"))
    )


def add_stock_status(df):
    """
    Classify inventory using stock quantity
    and reorder threshold.
    """

    return df.withColumn(
        "stock_status",
        F.when(
            F.col("validation_status") != "VALID",
            F.lit("UNKNOWN")
        )
        .when(
            F.col("stock_quantity") == 0,
            F.lit("OUT_OF_STOCK")
        )
        .when(
            F.col("stock_quantity")
            <= F.col("reorder_level"),
            F.lit("REORDER_REQUIRED")
        )
        .otherwise(
            F.lit("SUFFICIENT_STOCK")
        )
    )


def add_inventory_freshness_status(df):
    """
    Flag records older than 365 days.

    CURRENT means within the 365-day threshold,
    not necessarily real-time inventory.
    """

    return df.withColumn(
        "inventory_freshness_status",
        F.when(
            F.col("last_updated").isNull(),
            F.lit("UNKNOWN")
        )
        .when(
            F.col("last_updated") > F.current_date(),
            F.lit("FUTURE_DATE_REVIEW")
        )
        .when(
            F.col("last_updated")
            < F.date_sub(F.current_date(), 365),
            F.lit("STALE")
        )
        .otherwise(F.lit("CURRENT"))
    )


def deduplicate_inventory(df):
    """
    Keep one record per source inventory ID.

    Do not deduplicate solely by product/store,
    because multiple observations could be valid.
    """

    completeness_columns = [
        "source_product_id",
        "source_store_id",
        "stock_quantity",
        "reorder_level",
        "last_updated",
    ]

    completeness_score = F.lit(0)

    for column_name in completeness_columns:
        completeness_score = (
            completeness_score
            + F.when(
                F.col(column_name).isNotNull(),
                1
            ).otherwise(0)
        )

    df = df.withColumn(
        "_completeness_score",
        completeness_score
    )

    hash_columns = [
        "source_system",
        "source_inventory_id",
        "source_product_id",
        "source_store_id",
        "stock_quantity",
        "reorder_level",
        "last_updated",
    ]

    df = df.withColumn(
        "_record_hash",
        F.sha2(
            F.to_json(
                F.struct(
                    *[F.col(c) for c in hash_columns]
                ),
                options={"ignoreNullFields": "false"}
            ),
            256
        )
    )

    window_spec = (
        Window
        .partitionBy(
            "source_system",
            "source_inventory_id"
        )
        .orderBy(
            F.col("_completeness_score").desc(),
            F.col("last_updated").desc_nulls_last(),
            F.col(
                "ingestion_timestamp"
            ).desc_nulls_last(),
            F.col("_record_hash").asc(),
            F.col("source_file").asc_nulls_last()
        )
    )

    return (
        df.withColumn(
            "_row_number",
            F.row_number().over(window_spec)
        )
        .filter(F.col("_row_number") == 1)
        .drop(
            "_row_number",
            "_completeness_score",
            "_record_hash"
        )
    )


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== INVENTORY CLEANSING PIPELINE ===")

        # 1. Read Bronze inventory.
        retailmax_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/retailmax/inventory")
        )

        freshmart_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/freshmart/inventory")
        )

        # 2. Standardize source schemas.
        retailmax = transform_retailmax_inventory(
            retailmax_bronze
        )

        freshmart = transform_freshmart_inventory(
            freshmart_bronze
        )

        unified = retailmax.unionByName(freshmart)

        input_count = unified.count()
        print(f"Input inventory records: {input_count:,}")

        # 3. Clean identifiers.
        cleaned = clean_identifiers(unified)

        # 4. Add validation and business statuses.
        validated = add_validation_status(cleaned)

        classified = add_inventory_freshness_status(
            add_stock_status(validated)
        )

        print("\n=== VALIDATION STATUS ===")

        classified.groupBy(
            "validation_status"
        ).count().show()

        print("\n=== STOCK STATUS ===")

        classified.groupBy(
            "source_system",
            "stock_status"
        ).count().orderBy(
            "source_system",
            "stock_status"
        ).show(truncate=False)

        print("\n=== INVENTORY FRESHNESS STATUS ===")

        classified.groupBy(
            "source_system",
            "inventory_freshness_status"
        ).count().orderBy(
            "source_system",
            "inventory_freshness_status"
        ).show(truncate=False)

        # 5. Separate valid and invalid records.
        valid_inventory = classified.filter(
            F.col("validation_status") == "VALID"
        )

        quarantine_inventory = classified.filter(
            F.col("validation_status") != "VALID"
        )

        quarantine_count = quarantine_inventory.count()

        print(
            f"\nRecords requiring quarantine: "
            f"{quarantine_count}"
        )

        # 6. Deduplicate by source inventory ID.
        silver_inventory = deduplicate_inventory(
            valid_inventory
        )

        output_count = silver_inventory.count()

        print(
            f"Cleaned inventory records: "
            f"{output_count:,}"
        )

        # 7. Verify no duplicate IDs remain.
        duplicate_count = (
            silver_inventory
            .groupBy(
                "source_system",
                "source_inventory_id"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print(
            f"Remaining duplicate inventory IDs: "
            f"{duplicate_count}"
        )

        print("\n=== FINAL COUNTS BY SOURCE ===")

        silver_inventory.groupBy(
            "source_system"
        ).count().show()

        print("\n=== FINAL STOCK STATUS ===")

        silver_inventory.groupBy(
            "stock_status"
        ).count().orderBy(
            "stock_status"
        ).show()

        print("\n=== FINAL FRESHNESS STATUS ===")

        silver_inventory.groupBy(
            "inventory_freshness_status"
        ).count().orderBy(
            "inventory_freshness_status"
        ).show()

        # Development dataset assertions.
        assert input_count == 3600
        assert output_count == 3600
        assert quarantine_count == 0
        assert duplicate_count == 0

        print(
            "\nSUCCESS: INVENTORY CLEANSING "
            "AND VALIDATION COMPLETED!"
        )

        print(
            "\nNOTE: No Silver Inventory table "
            "has been written yet."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
