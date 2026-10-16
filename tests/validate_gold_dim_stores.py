
import os
import sys

from pyspark.sql import functions as F
from delta.tables import DeltaTable


PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)

sys.path.insert(
    0,
    os.path.join(PROJECT_ROOT, "src", "silver")
)

from transform_customers import create_spark


BUCKET = "retail-lakehouse-parsam-2026"

SILVER_STORES_PATH = (
    f"s3a://{BUCKET}/silver/stores"
)

SILVER_SALES_PATH = (
    f"s3a://{BUCKET}/silver/sales"
)

SILVER_INVENTORY_PATH = (
    f"s3a://{BUCKET}/silver/inventory"
)

GOLD_STORES_PATH = (
    f"s3a://{BUCKET}/gold/dim_stores"
)


def source_counts(df):
    return {
        row["source_system"]: row["count"]
        for row in (
            df.groupBy("source_system")
            .count()
            .collect()
        )
    }


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(
            "\n=== INDEPENDENT GOLD STORE "
            "DIMENSION VALIDATION ==="
        )

        # 1. Read persisted Delta tables

        silver = (
            spark.read.format("delta")
            .load(SILVER_STORES_PATH)
        )

        gold = (
            spark.read.format("delta")
            .load(GOLD_STORES_PATH)
        )

        sales = (
            spark.read.format("delta")
            .load(SILVER_SALES_PATH)
        )

        inventory = (
            spark.read.format("delta")
            .load(SILVER_INVENTORY_PATH)
        )

        # 2. Validate record counts

        assert silver.count() == 90
        assert gold.count() == 90

        print("PASS: Silver Stores = 90")
        print("PASS: Gold Stores = 90")

        # 3. Validate source distribution

        expected_sources = {
            "retailmax": 50,
            "freshmart": 40,
        }

        assert source_counts(silver) == expected_sources
        assert source_counts(gold) == expected_sources

        print("PASS: Source-level counts verified")

        # 4. Validate Gold schema

        expected_types = {
            "store_key": "string",
            "source_system": "string",
            "source_store_id": "string",
            "store_name": "string",
            "city": "string",
            "state": "string",
            "zip_code": "string",
            "opened_date": "date",
            "opened_year": "int",
            "opened_month": "int",
            "ingestion_timestamp": "timestamp",
            "batch_id": "string",
            "source_file": "string",
        }

        actual_types = dict(gold.dtypes)

        for column, expected_type in expected_types.items():
            assert actual_types.get(column) == expected_type, (
                f"{column}: expected {expected_type}, "
                f"found {actual_types.get(column)}"
            )

        print("PASS: Gold store schema verified")

        # 5. Validate store keys

        invalid_keys = gold.filter(
            F.col("store_key").isNull()
            | (F.trim("store_key") == "")
        ).count()

        duplicate_keys = (
            gold.groupBy("store_key")
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

        incorrect_keys = gold.filter(
            ~F.col("store_key").eqNullSafe(expected_key)
        ).count()

        assert incorrect_keys == 0

        print(
            "PASS: Store keys unique, non-null "
            "and source-qualified"
        )

        # 6. Validate Silver-to-Gold key coverage

        missing_gold = (
            silver.select("store_key")
            .join(
                gold.select("store_key"),
                on="store_key",
                how="left_anti"
            )
            .count()
        )

        unexpected_gold = (
            gold.select("store_key")
            .join(
                silver.select("store_key"),
                on="store_key",
                how="left_anti"
            )
            .count()
        )

        assert missing_gold == 0
        assert unexpected_gold == 0

        print("PASS: Silver-to-Gold store coverage")

        # 7. Validate exact attribute preservation

        shared_columns = [
            "store_key",
            "source_system",
            "source_store_id",
            "store_name",
            "city",
            "state",
            "zip_code",
            "opened_date",
            "ingestion_timestamp",
            "batch_id",
            "source_file",
        ]

        silver_shared = silver.select(*shared_columns)
        gold_shared = gold.select(*shared_columns)

        missing_or_changed = (
            silver_shared.exceptAll(gold_shared).count()
        )

        unexpected_or_changed = (
            gold_shared.exceptAll(silver_shared).count()
        )

        assert missing_or_changed == 0
        assert unexpected_or_changed == 0

        print(
            "PASS: All Silver store attributes "
            "preserved exactly"
        )

        # 8. Validate opening date attributes

        incorrect_year = gold.filter(
            ~F.col("opened_year").eqNullSafe(
                F.year("opened_date")
            )
        ).count()

        incorrect_month = gold.filter(
            ~F.col("opened_month").eqNullSafe(
                F.month("opened_date")
            )
        ).count()

        assert incorrect_year == 0
        assert incorrect_month == 0

        print(
            "PASS: Store opening year "
            "and month verified"
        )

        # 9. Validate Sales store references

        store_keys = gold.select("store_key")

        missing_sales_stores = (
            sales.join(
                store_keys,
                on="store_key",
                how="left_anti"
            )
            .count()
        )

        assert sales.count() == 88600
        assert missing_sales_stores == 0

        print(
            "PASS: All 88,600 trusted Sales "
            "reference valid Gold stores"
        )

        # 10. Validate Inventory store references

        missing_inventory_stores = (
            inventory.join(
                store_keys,
                on="store_key",
                how="left_anti"
            )
            .count()
        )

        assert inventory.count() == 3600
        assert missing_inventory_stores == 0

        print(
            "PASS: All 3,600 Inventory records "
            "reference valid Gold stores"
        )

        # 11. Validate source consistency

        sales_store_join = (
            sales.alias("s")
            .join(
                gold.alias("d"),
                on="store_key",
                how="inner"
            )
        )

        sales_source_mismatches = (
            sales_store_join.filter(
                ~F.col("s.source_system").eqNullSafe(
                    F.col("d.source_system")
                )
            ).count()
        )

        inventory_store_join = (
            inventory.alias("i")
            .join(
                gold.alias("d"),
                on="store_key",
                how="inner"
            )
        )

        inventory_source_mismatches = (
            inventory_store_join.filter(
                ~F.col("i.source_system").eqNullSafe(
                    F.col("d.source_system")
                )
            ).count()
        )

        assert sales_source_mismatches == 0
        assert inventory_source_mismatches == 0

        print(
            "PASS: Sales and Inventory source "
            "systems match their Gold stores"
        )

        # 12. Validate lineage

        missing_lineage = gold.filter(
            F.col("ingestion_timestamp").isNull()
            | F.col("batch_id").isNull()
            | (F.trim("batch_id") == "")
            | F.col("source_file").isNull()
            | (F.trim("source_file") == "")
        ).count()

        assert missing_lineage == 0

        print("PASS: Store lineage preserved")

        # 13. Validate Delta metadata

        assert DeltaTable.isDeltaTable(
            spark,
            GOLD_STORES_PATH
        )

        print("PASS: Gold Stores is a Delta table")

        print("\n=== GOLD STORE DELTA HISTORY ===")

        DeltaTable.forPath(
            spark,
            GOLD_STORES_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        # 14. Final summary

        print(
            "\nSUCCESS: ALL GOLD STORE "
            "DIMENSION VALIDATIONS PASSED!"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
