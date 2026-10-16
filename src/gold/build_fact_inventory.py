
import os
import sys

from pyspark.sql import functions as F
from delta.tables import DeltaTable


PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)

sys.path.insert(
    0,
    os.path.join(PROJECT_ROOT, "src", "silver")
)

from transform_customers import create_spark


BUCKET = "retail-lakehouse-parsam-2026"

SILVER_INVENTORY_PATH = (
    f"s3a://{BUCKET}/silver/inventory"
)

GOLD_BASE = f"s3a://{BUCKET}/gold"

GOLD_INVENTORY_PATH = (
    f"{GOLD_BASE}/fact_inventory"
)

EXPECTED_INVENTORY = 3600


def validate_dimension_references(
    facts,
    dimension,
    key,
    dimension_name
):
    """
    Validate that every fact foreign key
    references a unique dimension key.
    """

    dimension_keys = dimension.select(key)

    null_dimension_keys = dimension_keys.filter(
        F.col(key).isNull()
    ).count()

    duplicate_dimension_keys = (
        dimension_keys
        .groupBy(key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    assert null_dimension_keys == 0, (
        f"Null keys in {dimension_name}"
    )

    assert duplicate_dimension_keys == 0, (
        f"Duplicate keys in {dimension_name}"
    )

    missing = (
        facts.join(
            dimension_keys,
            on=key,
            how="left_anti"
        ).count()
    )

    assert missing == 0, (
        f"{missing} Inventory rows have invalid "
        f"{dimension_name} references"
    )

    print(
        f"PASS: All Inventory reference "
        f"valid {dimension_name}"
    )



def build_inventory_fact(silver_inventory):
    """
    Build one Gold row per trusted Silver
    inventory record.

    Rename Silver inventory_freshness_status
    to the Gold reporting column freshness_status.
    """

    return (
        silver_inventory
        .withColumnRenamed(
            "inventory_freshness_status",
            "freshness_status"
        )
        .withColumn(
            "last_updated_date_key",
            F.date_format(
                F.to_date("last_updated"),
                "yyyyMMdd"
            ).cast("int")
        )
        .select(
            "inventory_key",
            "source_system",
            "source_inventory_id",
            "product_key",
            "store_key",
            "last_updated_date_key",
            "stock_quantity",
            "reorder_level",
            "stock_status",
            "freshness_status",
            "last_updated",
            "ingestion_timestamp",
            "batch_id",
            "source_file"
        )
    )



def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(
            "\n=== GOLD INVENTORY FACT PIPELINE ==="
        )

        # 1. Read validated Silver Inventory

        silver_inventory = (
            spark.read.format("delta")
            .load(SILVER_INVENTORY_PATH)
        )

        silver_count = silver_inventory.count()

        assert silver_count == EXPECTED_INVENTORY

        print(
            f"Silver Inventory: {silver_count:,}"
        )

        # 2. Build Gold fact

        fact_inventory = build_inventory_fact(
            silver_inventory
        )

        fact_count = fact_inventory.count()

        assert fact_count == EXPECTED_INVENTORY

        print(
            f"Gold Inventory Facts: {fact_count:,}"
        )

        # 3. Validate inventory keys

        invalid_keys = fact_inventory.filter(
            F.col("inventory_key").isNull()
            | (F.trim("inventory_key") == "")
        ).count()

        duplicate_keys = (
            fact_inventory
            .groupBy("inventory_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        expected_key = F.concat(
            F.col("source_system"),
            F.lit(":"),
            F.col("source_inventory_id")
        )

        incorrect_keys = fact_inventory.filter(
            ~F.col("inventory_key").eqNullSafe(
                expected_key
            )
        ).count()

        assert invalid_keys == 0
        assert duplicate_keys == 0
        assert incorrect_keys == 0

        print(
            "PASS: Inventory keys unique, "
            "non-null and source-qualified"
        )

        # 4. Validate source distribution

        print("\n=== INVENTORY BY SOURCE ===")

        fact_inventory.groupBy(
            "source_system"
        ).count().show()

        source_counts = {
            row["source_system"]: row["count"]
            for row in (
                fact_inventory
                .groupBy("source_system")
                .count()
                .collect()
            )
        }

        assert source_counts == {
            "retailmax": 2000,
            "freshmart": 1600,
        }

        print("PASS: Source counts verified")

        # 5. Validate inventory quantities

        invalid_quantities = fact_inventory.filter(
            F.col("stock_quantity").isNull()
            | (F.col("stock_quantity") < 0)
            | F.col("reorder_level").isNull()
            | (F.col("reorder_level") < 0)
        ).count()

        assert invalid_quantities == 0

        print("PASS: Inventory quantities verified")

        # 6. Validate stock classifications

        expected_stock_status = (
            F.when(
                F.col("stock_quantity") == 0,
                "OUT_OF_STOCK"
            )
            .when(
                F.col("stock_quantity")
                <= F.col("reorder_level"),
                "REORDER_REQUIRED"
            )
            .otherwise("SUFFICIENT_STOCK")
        )

        incorrect_statuses = fact_inventory.filter(
            ~F.col("stock_status").eqNullSafe(
                expected_stock_status
            )
        ).count()

        assert incorrect_statuses == 0

        print(
            "PASS: Stock status classifications verified"
        )

        # 7. Validate date keys

        invalid_dates = fact_inventory.filter(
            F.col("last_updated").isNull()
            | F.col("last_updated_date_key").isNull()
        ).count()

        incorrect_date_keys = fact_inventory.filter(
            ~F.col("last_updated_date_key").eqNullSafe(
                F.date_format(
                    F.to_date("last_updated"),
                    "yyyyMMdd"
                ).cast("int")
            )
        ).count()

        assert invalid_dates == 0
        assert incorrect_date_keys == 0

        print(
            "PASS: Inventory update dates "
            "and date keys verified"
        )

        # 8. Read Gold dimensions

        dim_products = (
            spark.read.format("delta")
            .load(f"{GOLD_BASE}/dim_products")
        )

        dim_stores = (
            spark.read.format("delta")
            .load(f"{GOLD_BASE}/dim_stores")
        )

        dim_date = (
            spark.read.format("delta")
            .load(f"{GOLD_BASE}/dim_date")
        )

        # 9. Validate dimension references

        validate_dimension_references(
            fact_inventory,
            dim_products,
            "product_key",
            "Gold Products"
        )

        validate_dimension_references(
            fact_inventory,
            dim_stores,
            "store_key",
            "Gold Stores"
        )

        date_keys = dim_date.select(
            F.col("date_key").alias(
                "last_updated_date_key"
            )
        )

        validate_dimension_references(
            fact_inventory,
            date_keys,
            "last_updated_date_key",
            "Gold Date"
        )

        # 10. Validate source consistency

        for key, dimension, name in [
            ("product_key", dim_products, "Products"),
            ("store_key", dim_stores, "Stores"),
        ]:
            mismatches = (
                fact_inventory.alias("f")
                .join(
                    dimension.alias("d"),
                    on=key,
                    how="inner"
                )
                .filter(
                    ~F.col("f.source_system").eqNullSafe(
                        F.col("d.source_system")
                    )
                )
                .count()
            )

            assert mismatches == 0, (
                f"Source mismatch for {name}"
            )

        print(
            "PASS: Inventory dimension "
            "source systems match"
        )

        # 11. Validate lineage

        missing_lineage = fact_inventory.filter(
            F.col("ingestion_timestamp").isNull()
            | F.col("batch_id").isNull()
            | (F.trim("batch_id") == "")
            | F.col("source_file").isNull()
            | (F.trim("source_file") == "")
        ).count()

        assert missing_lineage == 0

        print("PASS: Inventory lineage preserved")

        # 12. Write Gold fact

        print("\n=== WRITING GOLD FACT_INVENTORY ===")

        (
            fact_inventory.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(GOLD_INVENTORY_PATH)
        )

        print("PASS: Gold Inventory fact written")

        # 13. Verify persisted table

        assert DeltaTable.isDeltaTable(
            spark,
            GOLD_INVENTORY_PATH
        )

        persisted = (
            spark.read.format("delta")
            .load(GOLD_INVENTORY_PATH)
        )

        assert persisted.count() == EXPECTED_INVENTORY

        print("PASS: Persisted count = 3,600")

        # 14. Display schema

        print("\n=== GOLD INVENTORY FACT SCHEMA ===")

        persisted.printSchema()

        # 15. Inventory status summary

        print("\n=== INVENTORY STOCK STATUS ===")

        persisted.groupBy(
            "stock_status"
        ).count().orderBy(
            "stock_status"
        ).show(truncate=False)

        # 16. Freshness summary

        print("\n=== INVENTORY FRESHNESS STATUS ===")

        persisted.groupBy(
            "freshness_status"
        ).count().orderBy(
            "freshness_status"
        ).show(truncate=False)

        # 17. Delta history

        print("\n=== DELTA HISTORY ===")

        DeltaTable.forPath(
            spark,
            GOLD_INVENTORY_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: GOLD FACT_INVENTORY CREATED!"
        )

        print(
            f"Gold path: {GOLD_INVENTORY_PATH}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
