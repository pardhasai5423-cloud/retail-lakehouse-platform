from src.config.write_safety import require_new_s3_target

import os
import sys

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from src.config.settings import get_s3_path

sys.path.insert(0, os.path.dirname(__file__))

from transform_customers import (
    create_spark,
    BRONZE_BASE,
)

from transform_sales import (
    transform_retailmax_sales,
    transform_freshmart_sales,
)

from clean_sales import (
    clean_identifiers,
    deduplicate_sales,
    add_reference_status,
    add_quality_flags,
)
from src.config.settings import require_safe_write_environment


SILVER_BASE = get_s3_path("silver")

SILVER_SALES_PATH = get_s3_path(
    "silver", "sales"
)

QUARANTINE_SALES_PATH = get_s3_path(
    "quarantine", "sales"
)


def get_source_counts(df):
    return {
        row["source_system"]: row["count"]
        for row in df.groupBy(
            "source_system"
        ).count().collect()
    }


def verify_delta_table(
    spark,
    path,
    expected_count,
    label,
):
    """
    Verify that the persisted S3 path
    contains a readable Delta table.
    """

    assert DeltaTable.isDeltaTable(
        spark,
        path
    ), f"{label} is not a Delta table"

    persisted = (
        spark.read.format("delta")
        .load(path)
    )

    actual_count = persisted.count()

    assert actual_count == expected_count, (
        f"{label}: expected {expected_count}, "
        f"found {actual_count}"
    )

    print(
        f"PASS: {label} persisted "
        f"{actual_count:,} records"
    )

    print(f"\n=== {label.upper()} DELTA HISTORY ===")

    DeltaTable.forPath(
        spark,
        path
    ).history(5).select(
        "version",
        "timestamp",
        "operation"
    ).show(truncate=False)

    return persisted


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    classified = None

    try:
        print("\n=== SILVER SALES WRITE PIPELINE ===")

        # -----------------------------------
        # 1. Read Bronze Sales
        # -----------------------------------

        retailmax_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/retailmax/sales")
        )

        freshmart_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/freshmart/sales")
        )

        # -----------------------------------
        # 2. Standardize schemas
        # -----------------------------------

        retailmax = transform_retailmax_sales(
            retailmax_bronze
        )

        freshmart = transform_freshmart_sales(
            freshmart_bronze
        )

        unified = retailmax.unionByName(freshmart)

        input_count = unified.count()

        assert input_count == 90000

        print(f"Input Sales records: {input_count:,}")

        # -----------------------------------
        # 3. Clean and deduplicate
        # -----------------------------------

        cleaned = clean_identifiers(unified)

        deduplicated = deduplicate_sales(
            cleaned
        )

        deduplicated_count = deduplicated.count()

        assert deduplicated_count == 90000

        print(
            "Records after deduplication:",
            f"{deduplicated_count:,}"
        )

        # -----------------------------------
        # 4. Load Silver master data
        # -----------------------------------

        customers = (
            spark.read.format("delta")
            .load(f"{SILVER_BASE}/customers")
        )

        products = (
            spark.read.format("delta")
            .load(f"{SILVER_BASE}/products")
        )

        stores = (
            spark.read.format("delta")
            .load(f"{SILVER_BASE}/stores")
        )

        # -----------------------------------
        # 5. Validate references
        # -----------------------------------

        referenced = add_reference_status(
            deduplicated,
            customers,
            "customer_key",
            "customer_reference_status"
        )

        referenced = add_reference_status(
            referenced,
            products,
            "product_key",
            "product_reference_status"
        )

        referenced = add_reference_status(
            referenced,
            stores,
            "store_key",
            "store_reference_status"
        )

        # -----------------------------------
        # 6. Classify quality
        # -----------------------------------

        classified = add_quality_flags(
            referenced
        ).cache()

        valid_sales = classified.filter(
            F.col("validation_status") == "VALID"
        )

        quarantine_sales = classified.filter(
            F.col("validation_status") == "QUARANTINE"
        )

        valid_count = valid_sales.count()
        quarantine_count = quarantine_sales.count()

        print("\n=== SALES CLASSIFICATION ===")

        print(f"Valid Sales: {valid_count:,}")
        print(
            f"Quarantined Sales: "
            f"{quarantine_count:,}"
        )

        assert valid_count == 88600
        assert quarantine_count == 1400

        assert (
            valid_count + quarantine_count
            == deduplicated_count
        )

        # -----------------------------------
        # 7. Validate source-level counts
        # -----------------------------------

        assert get_source_counts(valid_sales) == {
            "retailmax": 49000,
            "freshmart": 39600,
        }

        assert get_source_counts(quarantine_sales) == {
            "retailmax": 1000,
            "freshmart": 400,
        }

        print("PASS: Source-level counts verified")

        # -----------------------------------
        # 8. Validate quality classification
        # -----------------------------------

        invalid_valid = valid_sales.filter(
            F.size(F.col("quality_issues")) != 0
        ).count()

        invalid_quarantine = quarantine_sales.filter(
            F.size(F.col("quality_issues")) == 0
        ).count()

        assert invalid_valid == 0
        assert invalid_quarantine == 0

        print("PASS: Quality classification verified")

        # -----------------------------------
        # 9. Validate unique transaction keys
        # -----------------------------------

        duplicate_keys = (
            classified
            .groupBy("transaction_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert duplicate_keys == 0

        print("PASS: No duplicate transaction keys")

        # -----------------------------------
        # 10. Show quality issue distribution
        # -----------------------------------

        print("\n=== QUARANTINE ISSUE DISTRIBUTION ===")

        (
            quarantine_sales
            .select(
                "source_system",
                F.explode("quality_issues").alias(
                    "quality_issue"
                )
            )
            .groupBy(
                "source_system",
                "quality_issue"
            )
            .count()
            .orderBy(
                "source_system",
                "quality_issue"
            )
            .show(100, truncate=False)
        )

        # -----------------------------------
        # 11. Write trusted Silver Sales
        # -----------------------------------

        print("\n=== WRITING SILVER SALES TO S3 ===")

        require_new_s3_target(spark, SILVER_SALES_PATH)
        (
            valid_sales.write
            .format("delta")
            .mode("errorifexists")
            .option("overwriteSchema", "true")
            .save(SILVER_SALES_PATH)
        )

        print("PASS: Silver Sales write completed")

        # -----------------------------------
        # 12. Write Sales quarantine
        # -----------------------------------

        print("\n=== WRITING SALES QUARANTINE TO S3 ===")

        require_new_s3_target(spark, QUARANTINE_SALES_PATH)
        (
            quarantine_sales.write
            .format("delta")
            .mode("errorifexists")
            .option("overwriteSchema", "true")
            .save(QUARANTINE_SALES_PATH)
        )

        print("PASS: Sales quarantine write completed")

        # -----------------------------------
        # 13. Verify persisted Delta tables
        # -----------------------------------

        print("\n=== VERIFYING PERSISTED TABLES ===")

        persisted_sales = verify_delta_table(
            spark,
            SILVER_SALES_PATH,
            88600,
            "Silver Sales",
        )

        persisted_quarantine = verify_delta_table(
            spark,
            QUARANTINE_SALES_PATH,
            1400,
            "Sales Quarantine",
        )

        # -----------------------------------
        # 14. Verify persisted classifications
        # -----------------------------------

        assert (
            persisted_sales.filter(
                F.col("validation_status") != "VALID"
            ).count()
            == 0
        )

        assert (
            persisted_quarantine.filter(
                F.col("validation_status") != "QUARANTINE"
            ).count()
            == 0
        )

        print(
            "PASS: Persisted validation statuses "
            "verified"
        )

        # -----------------------------------
        # 15. Final reconciliation
        # -----------------------------------

        persisted_total = (
            persisted_sales.count()
            + persisted_quarantine.count()
        )

        assert persisted_total == 90000

        print(
            f"PASS: Persisted reconciliation = "
            f"{persisted_total:,} records"
        )

        print("\n=== PERSISTED SILVER SALES COUNTS ===")

        persisted_sales.groupBy(
            "source_system"
        ).count().show()

        print("\n=== PERSISTED QUARANTINE COUNTS ===")

        persisted_quarantine.groupBy(
            "source_system"
        ).count().show()

        print(
            "\nSUCCESS: SILVER SALES AND "
            "QUARANTINE DELTA TABLES CREATED!"
        )

        print(
            f"\nSilver Sales: {SILVER_SALES_PATH}"
        )

        print(
            "Sales Quarantine: "
            f"{QUARANTINE_SALES_PATH}"
        )

    finally:
        if classified is not None:
            classified.unpersist()

        spark.stop()


if __name__ == "__main__":
    require_safe_write_environment()
    main()
