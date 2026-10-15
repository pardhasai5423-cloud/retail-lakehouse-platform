
import os
import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)

sys.path.insert(
    0,
    os.path.join(PROJECT_ROOT, "src", "silver")
)

from transform_customers import create_spark


BUCKET = "retail-lakehouse-parsam-2026"

BRONZE_PATH = f"s3a://{BUCKET}/bronze"
STORES_PATH = f"s3a://{BUCKET}/silver/stores"


def count_unmatched_references(
    source_df,
    stores_df,
    source_system,
    store_id_column,
    dataset_name
):
    """
    Identify source records that reference
    a missing Silver store.

    Null or blank references are treated as
    unmatched records.
    """

    source_records = source_df.select(
        F.lit(source_system).alias("source_system"),
        F.trim(
            F.col(store_id_column)
        ).alias("source_store_id")
    )

    reference_stores = stores_df.select(
        "source_system",
        "source_store_id"
    )

    unmatched = source_records.join(
        reference_stores,
        on=["source_system", "source_store_id"],
        how="left_anti"
    )

    unmatched_count = unmatched.count()

    print(
        f"{source_system} {dataset_name} "
        f"unmatched store references: {unmatched_count}"
    )

    if unmatched_count > 0:
        print("\nSample unmatched store IDs:")

        unmatched.groupBy(
            "source_store_id"
        ).count().orderBy(
            F.desc("count")
        ).show(20, truncate=False)

    return unmatched_count


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== SILVER STORES VALIDATION ===")

        stores = (
            spark.read.format("delta")
            .load(STORES_PATH)
        )

        # ----------------------------------
        # 1. Total record count
        # ----------------------------------

        total = stores.count()

        assert total == 90, (
            f"Expected 90 stores, found {total}"
        )

        print("PASS: Total Silver stores = 90")

        counts = {
            row["source_system"]: row["count"]
            for row in stores.groupBy(
                "source_system"
            ).count().collect()
        }

        assert counts == {
            "retailmax": 50,
            "freshmart": 40
        }

        print("PASS: RetailMax = 50")
        print("PASS: FreshMart = 40")

        # ----------------------------------
        # 2. Duplicate store IDs
        # ----------------------------------

        duplicates = (
            stores.groupBy(
                "source_system",
                "source_store_id"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert duplicates == 0

        print("PASS: No duplicate store IDs")

        # ----------------------------------
        # 3. Mandatory fields
        # ----------------------------------

        required_columns = [
            "source_system",
            "source_store_id",
            "store_key",
            "store_name",
            "city",
            "state",
        ]

        missing_condition = F.lit(False)

        for column_name in required_columns:
            missing_condition = (
                missing_condition
                | F.col(column_name).isNull()
                | (
                    F.trim(F.col(column_name)) == ""
                )
            )

        missing_count = stores.filter(
            missing_condition
        ).count()

        assert missing_count == 0

        print("PASS: Mandatory store fields present")

        # ----------------------------------
        # 4. Composite store key
        # ----------------------------------

        expected_key = F.concat(
            F.col("source_system"),
            F.lit(":"),
            F.col("source_store_id")
        )

        invalid_keys = stores.filter(
            ~F.col("store_key").eqNullSafe(
                expected_key
            )
        ).count()

        assert invalid_keys == 0

        print("PASS: Composite store keys valid")

        # ----------------------------------
        # 5. Quality status columns
        # ----------------------------------

        invalid_quality = stores.filter(
            (F.col("validation_status") != "VALID")
            | (F.col("zip_quality_status") != "VALID")
            | (F.col("opening_date_status") != "VALID")
            | F.col("validation_status").isNull()
            | F.col("zip_quality_status").isNull()
            | F.col("opening_date_status").isNull()
        ).count()

        assert invalid_quality == 0

        print("PASS: All store quality flags VALID")

        # ----------------------------------
        # 6. Lineage metadata
        # ----------------------------------

        missing_lineage = stores.filter(
            F.col("ingestion_timestamp").isNull()
            | F.col("batch_id").isNull()
            | F.col("source_file").isNull()
        ).count()

        assert missing_lineage == 0

        print("PASS: Store lineage metadata preserved")

        # ----------------------------------
        # 7. Delta table verification
        # ----------------------------------

        assert DeltaTable.isDeltaTable(
            spark, STORES_PATH
        )

        print("PASS: Valid Stores Delta table")

        # ----------------------------------
        # 8. Read Bronze Sales and Inventory
        # ----------------------------------

        print(
            "\n=== STORE REFERENTIAL INTEGRITY ==="
        )

        retailmax_sales = (
            spark.read.format("delta")
            .load(f"{BRONZE_PATH}/retailmax/sales")
        )

        freshmart_sales = (
            spark.read.format("delta")
            .load(f"{BRONZE_PATH}/freshmart/sales")
        )

        retailmax_inventory = (
            spark.read.format("delta")
            .load(f"{BRONZE_PATH}/retailmax/inventory")
        )

        freshmart_inventory = (
            spark.read.format("delta")
            .load(f"{BRONZE_PATH}/freshmart/inventory")
        )

        # Confirm expected Bronze record counts.
        assert retailmax_sales.count() == 50000
        assert freshmart_sales.count() == 40000
        assert retailmax_inventory.count() == 2000
        assert freshmart_inventory.count() == 1600

        print("PASS: Bronze Sales = 90,000")
        print("PASS: Bronze Inventory = 3,600")

        # ----------------------------------
        # 9. Sales -> Stores checks
        # ----------------------------------

        print("\n=== SALES TO STORES ===")

        rm_sales_unmatched = count_unmatched_references(
            retailmax_sales,
            stores,
            "retailmax",
            "store_id",
            "sales"
        )

        fm_sales_unmatched = count_unmatched_references(
            freshmart_sales,
            stores,
            "freshmart",
            "location_code",
            "sales"
        )

        # ----------------------------------
        # 10. Inventory -> Stores checks
        # ----------------------------------

        print("\n=== INVENTORY TO STORES ===")

        rm_inventory_unmatched = count_unmatched_references(
            retailmax_inventory,
            stores,
            "retailmax",
            "store_id",
            "inventory"
        )

        fm_inventory_unmatched = count_unmatched_references(
            freshmart_inventory,
            stores,
            "freshmart",
            "location_code",
            "inventory"
        )

        # ----------------------------------
        # 11. Final referential integrity
        # ----------------------------------

        total_unmatched = (
            rm_sales_unmatched
            + fm_sales_unmatched
            + rm_inventory_unmatched
            + fm_inventory_unmatched
        )

        print(
            f"\nTotal unmatched store references: "
            f"{total_unmatched}"
        )

        assert total_unmatched == 0, (
            f"Found {total_unmatched} source records "
            "referencing missing Silver stores"
        )

        print(
            "PASS: All Sales and Inventory records "
            "reference valid Silver stores"
        )

        # ----------------------------------
        # 12. Delta transaction history
        # ----------------------------------

        print("\n=== STORES DELTA HISTORY ===")

        DeltaTable.forPath(
            spark, STORES_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: ALL SILVER STORES "
            "VALIDATION CHECKS PASSED!"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
