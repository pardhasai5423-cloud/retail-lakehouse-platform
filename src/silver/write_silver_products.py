from src.config.write_safety import require_new_s3_target

import os
import sys

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from src.config.settings import get_s3_path

sys.path.insert(0, os.path.dirname(__file__))

from transform_customers import create_spark, BRONZE_BASE

from transform_products import (
    transform_retailmax_products,
    transform_freshmart_products,
)

from clean_products import (
    clean_blank_values,
    add_price_quality_status,
    add_validation_status,
    deduplicate_products,
)
from src.config.settings import require_safe_write_environment


SILVER_PRODUCTS_PATH = get_s3_path(
    "silver", "products"
)


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== SILVER PRODUCTS PIPELINE ===")

        # 1. Read the source Bronze Delta tables.
        retailmax_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/retailmax/products")
        )

        freshmart_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/freshmart/products")
        )

        # 2. Standardize source schemas.
        retailmax = transform_retailmax_products(
            retailmax_bronze
        )

        freshmart = transform_freshmart_products(
            freshmart_bronze
        )

        unified = retailmax.unionByName(freshmart)

        input_count = unified.count()
        print(f"Input records: {input_count:,}")

        # 3. Clean and classify data quality.
        cleaned = clean_blank_values(unified)

        validated = add_validation_status(
            add_price_quality_status(cleaned)
        )

        valid_products = validated.filter(
            F.col("validation_status") == "VALID"
        )

        invalid_products = validated.filter(
            F.col("validation_status") != "VALID"
        )

        invalid_count = invalid_products.count()

        print(f"Invalid records: {invalid_count:,}")

        # Do not silently discard invalid records.
        # Stop before writing until quarantine is implemented.
        if invalid_count > 0:
            raise ValueError(
                f"{invalid_count} invalid product records found. "
                "Quarantine handling is required before writing."
            )

        # 4. Deduplicate source product IDs.
        silver_products = deduplicate_products(
            valid_products
        )

        output_count = silver_products.count()

        print(f"Cleaned records: {output_count:,}")

        # 5. Validate expected counts.
        assert input_count == 1800
        assert output_count == 1800

        duplicates = (
            silver_products
            .groupBy("source_system", "source_product_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert duplicates == 0
        print("PASS: No duplicate source product IDs")

        missing_prices = silver_products.filter(
            F.col("price_quality_status") == "MISSING_PRICE"
        ).count()

        assert missing_prices == 18
        print("PASS: 18 missing prices preserved and flagged")

        # 6. Persist the Silver Delta table to AWS S3.
        print("\n=== WRITING SILVER PRODUCTS TO S3 ===")

        require_new_s3_target(spark, SILVER_PRODUCTS_PATH)
        (
            silver_products.write
            .format("delta")
            .mode("errorifexists")
            .option("overwriteSchema", "true")
            .save(SILVER_PRODUCTS_PATH)
        )

        # 7. Read back the persisted Delta table.
        print("\n=== VALIDATING PERSISTED S3 TABLE ===")

        verified = (
            spark.read
            .format("delta")
            .load(SILVER_PRODUCTS_PATH)
        )

        verified_count = verified.count()
        assert verified_count == 1800

        print(
            f"PASS: S3 Silver products = "
            f"{verified_count:,}"
        )

        print("\n=== COUNTS BY COMPANY ===")

        verified.groupBy("source_system").count().show()

        print("\n=== PRICE QUALITY STATUS ===")

        verified.groupBy(
            "source_system",
            "price_quality_status"
        ).count().orderBy(
            "source_system",
            "price_quality_status"
        ).show()

        print("\n=== SILVER PRODUCTS SCHEMA ===")
        verified.printSchema()

        # 8. Confirm Delta Lake transaction history.
        assert DeltaTable.isDeltaTable(
            spark, SILVER_PRODUCTS_PATH
        )

        print("\n=== DELTA TRANSACTION HISTORY ===")

        DeltaTable.forPath(
            spark, SILVER_PRODUCTS_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: SILVER PRODUCTS "
            "DELTA TABLE CREATED!"
        )

        print(f"\nS3 Path: {SILVER_PRODUCTS_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    require_safe_write_environment()
    main()
