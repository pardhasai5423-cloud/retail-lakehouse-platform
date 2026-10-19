from src.config.write_safety import require_new_s3_target

import os
import sys

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from src.config.settings import get_s3_path

sys.path.insert(0, os.path.dirname(__file__))

from transform_customers import create_spark, BRONZE_BASE

from transform_suppliers import (
    transform_retailmax_suppliers,
    transform_freshmart_suppliers,
)

from clean_suppliers import (
    clean_blank_values,
    add_email_quality_status,
    add_validation_status,
    deduplicate_suppliers,
)
from src.config.settings import require_safe_write_environment


SILVER_SUPPLIERS_PATH = get_s3_path(
    "silver", "suppliers"
)


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== SILVER SUPPLIERS PIPELINE ===")

        # 1. Read Bronze Delta tables from S3.
        retailmax_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/retailmax/suppliers")
        )

        freshmart_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/freshmart/suppliers")
        )

        # 2. Standardize schemas.
        retailmax = transform_retailmax_suppliers(
            retailmax_bronze
        )

        freshmart = transform_freshmart_suppliers(
            freshmart_bronze
        )

        unified = retailmax.unionByName(freshmart)

        input_count = unified.count()
        print(f"Input records: {input_count:,}")

        # 3. Clean and validate.
        cleaned = clean_blank_values(unified)

        validated = add_validation_status(
            add_email_quality_status(cleaned)
        )

        valid_suppliers = validated.filter(
            F.col("validation_status") == "VALID"
        )

        invalid_suppliers = validated.filter(
            F.col("validation_status") != "VALID"
        )

        invalid_count = invalid_suppliers.count()

        print(f"Invalid records: {invalid_count:,}")

        # Stop rather than silently dropping rejected rows.
        # Persistent quarantine can be added in a later step.
        if invalid_count > 0:
            raise ValueError(
                f"{invalid_count} invalid suppliers found. "
                "Quarantine handling is required before writing."
            )

        # 4. Deduplicate within each source system.
        silver_suppliers = deduplicate_suppliers(
            valid_suppliers
        )

        output_count = silver_suppliers.count()

        print(f"Cleaned records: {output_count:,}")

        # 5. Validate before writing.
        assert input_count == 180
        assert output_count == 180

        duplicates = (
            silver_suppliers
            .groupBy(
                "source_system",
                "source_supplier_id"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert duplicates == 0
        print("PASS: No duplicate source supplier IDs")

        email_quality = {
            row["email_quality_status"]: row["count"]
            for row in silver_suppliers.groupBy(
                "email_quality_status"
            ).count().collect()
        }

        assert email_quality == {"VALID": 180}

        print("PASS: All 180 supplier emails valid")

        # 6. Write the unified Delta table to S3.
        print("\n=== WRITING SILVER SUPPLIERS TO S3 ===")

        require_new_s3_target(spark, SILVER_SUPPLIERS_PATH)
        (
            silver_suppliers.write
            .format("delta")
            .mode("errorifexists")
            .option("overwriteSchema", "true")
            .save(SILVER_SUPPLIERS_PATH)
        )

        # 7. Read back from S3.
        print("\n=== VALIDATING PERSISTED S3 TABLE ===")

        verified = (
            spark.read
            .format("delta")
            .load(SILVER_SUPPLIERS_PATH)
        )

        verified_count = verified.count()

        assert verified_count == 180

        print(
            f"PASS: S3 Silver suppliers = "
            f"{verified_count:,}"
        )

        print("\n=== COUNTS BY COMPANY ===")

        verified.groupBy(
            "source_system"
        ).count().show()

        print("\n=== EMAIL QUALITY STATUS ===")

        verified.groupBy(
            "source_system",
            "email_quality_status"
        ).count().orderBy(
            "source_system"
        ).show()

        print("\n=== SILVER SUPPLIERS SCHEMA ===")
        verified.printSchema()

        # 8. Confirm Delta transaction history.
        assert DeltaTable.isDeltaTable(
            spark, SILVER_SUPPLIERS_PATH
        )

        print("\n=== DELTA TRANSACTION HISTORY ===")

        DeltaTable.forPath(
            spark, SILVER_SUPPLIERS_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: SILVER SUPPLIERS "
            "DELTA TABLE CREATED!"
        )

        print(f"\nS3 Path: {SILVER_SUPPLIERS_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    require_safe_write_environment()
    main()
