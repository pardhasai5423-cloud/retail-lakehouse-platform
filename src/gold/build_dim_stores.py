from src.config.write_safety import require_new_s3_target

import os
import sys

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from src.config.settings import get_s3_path

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)

sys.path.insert(
    0,
    os.path.join(PROJECT_ROOT, "src", "silver")
)

from transform_customers import create_spark
from src.config.settings import require_safe_write_environment


SILVER_STORES_PATH = get_s3_path(
    "silver", "stores"
)

SILVER_SALES_PATH = get_s3_path(
    "silver", "sales"
)

SILVER_INVENTORY_PATH = get_s3_path(
    "silver", "inventory"
)

GOLD_STORES_PATH = get_s3_path(
    "gold", "dim_stores"
)


def build_store_dimension(silver_stores):
    """
    Build analytics-ready Gold Store dimension
    from validated Silver Stores.
    """

    stores = (
        silver_stores
        .withColumn(
            "opened_year",
            F.year("opened_date")
        )
        .withColumn(
            "opened_month",
            F.month("opened_date")
        )
    )

    columns = [
        "store_key",
        "source_system",
        "source_store_id",
        "store_name",
        "city",
        "state",
        "zip_code",
        "opened_date",
        "opened_year",
        "opened_month",
        "ingestion_timestamp",
    ]

    for optional_column in [
        "batch_id",
        "source_file",
    ]:
        if optional_column in stores.columns:
            columns.append(optional_column)

    return stores.select(*columns)


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(
            "\n=== GOLD STORE DIMENSION PIPELINE ==="
        )

        # ----------------------------------
        # 1. Read Silver Stores
        # ----------------------------------

        silver_stores = (
            spark.read.format("delta")
            .load(SILVER_STORES_PATH)
        )

        silver_count = silver_stores.count()

        assert silver_count == 90

        print(
            f"Silver Stores: {silver_count:,}"
        )

        # ----------------------------------
        # 2. Build Gold dimension
        # ----------------------------------

        gold_stores = build_store_dimension(
            silver_stores
        )

        gold_count = gold_stores.count()

        assert gold_count == silver_count

        print(
            f"Gold Stores: {gold_count:,}"
        )

        # ----------------------------------
        # 3. Validate store keys
        # ----------------------------------

        invalid_keys = gold_stores.filter(
            F.col("store_key").isNull()
            | (F.trim("store_key") == "")
        ).count()

        duplicate_keys = (
            gold_stores
            .groupBy("store_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert invalid_keys == 0
        assert duplicate_keys == 0

        expected_key = F.concat(
            F.col("source_system"),
            F.lit(":"),
            F.col("source_store_id")
        )

        incorrectly_qualified = gold_stores.filter(
            ~F.col("store_key").eqNullSafe(
                expected_key
            )
        ).count()

        assert incorrectly_qualified == 0

        print(
            "PASS: Store keys unique, non-null "
            "and source-qualified"
        )

        # ----------------------------------
        # 4. Validate source counts
        # ----------------------------------

        print("\n=== STORES BY SOURCE ===")

        gold_stores.groupBy(
            "source_system"
        ).count().show()

        source_counts = {
            row["source_system"]: row["count"]
            for row in gold_stores.groupBy(
                "source_system"
            ).count().collect()
        }

        assert source_counts == {
            "retailmax": 50,
            "freshmart": 40,
        }

        print("PASS: Source counts verified")

        # ----------------------------------
        # 5. Validate store dates
        # ----------------------------------

        incorrect_year = gold_stores.filter(
            ~F.col("opened_year").eqNullSafe(
                F.year("opened_date")
            )
        ).count()

        incorrect_month = gold_stores.filter(
            ~F.col("opened_month").eqNullSafe(
                F.month("opened_date")
            )
        ).count()

        assert incorrect_year == 0
        assert incorrect_month == 0

        print("PASS: Store opening dates verified")

        # ----------------------------------
        # 6. Validate Sales references
        # ----------------------------------

        silver_sales = (
            spark.read.format("delta")
            .load(SILVER_SALES_PATH)
        )

        store_keys = gold_stores.select(
            "store_key"
        )

        missing_sales_stores = (
            silver_sales
            .join(
                store_keys,
                on="store_key",
                how="left_anti"
            )
            .count()
        )

        assert missing_sales_stores == 0

        print(
            "PASS: All trusted Sales reference "
            "valid Gold stores"
        )

        # ----------------------------------
        # 7. Validate Inventory references
        # ----------------------------------

        silver_inventory = (
            spark.read.format("delta")
            .load(SILVER_INVENTORY_PATH)
        )

        missing_inventory_stores = (
            silver_inventory
            .join(
                store_keys,
                on="store_key",
                how="left_anti"
            )
            .count()
        )

        assert missing_inventory_stores == 0

        print(
            "PASS: All Inventory records reference "
            "valid Gold stores"
        )

        # ----------------------------------
        # 8. Validate lineage
        # ----------------------------------

        missing_lineage = gold_stores.filter(
            F.col("ingestion_timestamp").isNull()
        ).count()

        assert missing_lineage == 0

        print("PASS: Store lineage preserved")

        # ----------------------------------
        # 9. Write Gold dimension
        # ----------------------------------

        print(
            "\n=== WRITING GOLD DIM_STORES ==="
        )

        require_new_s3_target(spark, GOLD_STORES_PATH)
        (
            gold_stores.write
            .format("delta")
            .mode("errorifexists")
            .option("overwriteSchema", "true")
            .save(GOLD_STORES_PATH)
        )

        print("PASS: Gold store dimension written")

        # ----------------------------------
        # 10. Verify persisted table
        # ----------------------------------

        assert DeltaTable.isDeltaTable(
            spark,
            GOLD_STORES_PATH
        )

        persisted = (
            spark.read.format("delta")
            .load(GOLD_STORES_PATH)
        )

        assert persisted.count() == 90

        print("PASS: Persisted count = 90")

        # ----------------------------------
        # 11. Show schema
        # ----------------------------------

        print("\n=== GOLD STORE SCHEMA ===")

        persisted.printSchema()

        # ----------------------------------
        # 12. Show sample stores
        # ----------------------------------

        print("\n=== SAMPLE GOLD STORES ===")

        persisted.select(
            "store_key",
            "store_name",
            "city",
            "state",
            "zip_code",
            "opened_year"
        ).show(10, truncate=False)

        # ----------------------------------
        # 13. Delta history
        # ----------------------------------

        print("\n=== DELTA HISTORY ===")

        DeltaTable.forPath(
            spark,
            GOLD_STORES_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: GOLD DIM_STORES CREATED!"
        )

        print(
            f"Gold path: {GOLD_STORES_PATH}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    require_safe_write_environment()
    main()
