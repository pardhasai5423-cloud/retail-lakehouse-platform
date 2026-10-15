
import os
import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)

sys.path.insert(
    0,
    os.path.join(PROJECT_ROOT, "src", "silver")
)

from transform_customers import create_spark


SILVER_PATH = (
    "s3a://retail-lakehouse-parsam-2026/silver/products"
)


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== VALIDATING S3 SILVER PRODUCTS ===")

        df = spark.read.format("delta").load(SILVER_PATH)

        # 1. Total record count
        total = df.count()

        assert total == 1800, (
            f"Expected 1800 products, found {total}"
        )

        print(f"PASS: Total products = {total:,}")

        # 2. Source-level counts
        counts = {
            row["source_system"]: row["count"]
            for row in df.groupBy(
                "source_system"
            ).count().collect()
        }

        assert counts == {
            "retailmax": 1000,
            "freshmart": 800,
        }, f"Unexpected source counts: {counts}"

        print("PASS: RetailMax = 1,000")
        print("PASS: FreshMart = 800")

        # 3. Check duplicate product IDs
        duplicate_count = (
            df.groupBy(
                "source_system",
                "source_product_id"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert duplicate_count == 0
        print("PASS: No duplicate source product IDs")

        # 4. Check mandatory fields
        invalid_required = df.filter(
            F.col("source_system").isNull()
            | F.col("source_product_id").isNull()
            | (F.trim(F.col("source_product_id")) == "")
            | F.col("product_key").isNull()
            | F.col("product_name").isNull()
            | (F.trim(F.col("product_name")) == "")
            | F.col("created_date").isNull()
        ).count()

        assert invalid_required == 0
        print("PASS: Mandatory product fields present")

        # 5. Verify product key construction
        incorrect_keys = df.filter(
            F.col("product_key") != F.concat(
                F.col("source_system"),
                F.lit(":"),
                F.col("source_product_id")
            )
        ).count()

        assert incorrect_keys == 0
        print("PASS: Composite product keys are correct")

        # 6. Verify price data type
        price_type = df.schema["unit_price"].dataType

        assert price_type == DecimalType(12, 2), (
            f"Unexpected price type: {price_type}"
        )

        print("PASS: unit_price uses decimal(12,2)")

        # 7. Check price quality classifications
        quality_counts = {
            row["price_quality_status"]: row["count"]
            for row in df.groupBy(
                "price_quality_status"
            ).count().collect()
        }

        assert quality_counts == {
            "VALID": 1782,
            "MISSING_PRICE": 18,
        }, f"Unexpected quality counts: {quality_counts}"

        print("PASS: 1,782 valid prices")
        print("PASS: 18 missing prices flagged")

        # 8. Verify null prices are classified correctly
        inconsistent_prices = df.filter(
            (
                F.col("unit_price").isNull()
                & (
                    F.col("price_quality_status")
                    != "MISSING_PRICE"
                )
            )
            | (
                F.col("unit_price").isNotNull()
                & (F.col("unit_price") > 0)
                & (
                    F.col("price_quality_status")
                    != "VALID"
                )
            )
            | (
                F.col("unit_price").isNotNull()
                & (F.col("unit_price") <= 0)
            )
        ).count()

        assert inconsistent_prices == 0
        print("PASS: Price quality flags match actual values")

        # 9. Validation status
        invalid_status = df.filter(
            F.col("validation_status").isNull()
            | (F.col("validation_status") != "VALID")
        ).count()

        assert invalid_status == 0
        print("PASS: All persisted products passed validation")

        # 10. Verify lineage metadata
        missing_lineage = df.filter(
            F.col("ingestion_timestamp").isNull()
            | F.col("batch_id").isNull()
            | F.col("source_file").isNull()
        ).count()

        assert missing_lineage == 0
        print("PASS: Source lineage metadata preserved")

        # 11. Verify Delta Lake transaction log
        assert DeltaTable.isDeltaTable(spark, SILVER_PATH)

        print("PASS: Valid Delta Lake table")

        print("\n=== DELTA TRANSACTION HISTORY ===")

        delta_table = DeltaTable.forPath(
            spark, SILVER_PATH
        )

        delta_table.history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: ALL SILVER PRODUCT "
            "VALIDATIONS PASSED!"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
