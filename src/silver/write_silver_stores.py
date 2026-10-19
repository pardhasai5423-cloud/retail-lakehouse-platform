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

from transform_stores import (
    transform_retailmax_stores,
    transform_freshmart_stores,
)

from clean_stores import (
    clean_blank_values,
    add_zip_quality_status,
    add_opening_date_status,
    add_validation_status,
    deduplicate_stores,
)
from src.config.settings import require_safe_write_environment


SILVER_STORES_PATH = get_s3_path(
    "silver", "stores"
)


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== SILVER STORES PIPELINE ===")

        # 1. Read Bronze Delta tables.
        retailmax_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/retailmax/stores")
        )

        freshmart_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/freshmart/stores")
        )

        # 2. Standardize schemas.
        retailmax = transform_retailmax_stores(
            retailmax_bronze
        )

        freshmart = transform_freshmart_stores(
            freshmart_bronze
        )

        unified = retailmax.unionByName(freshmart)

        input_count = unified.count()

        print(f"Input records: {input_count:,}")

        # 3. Clean and validate.
        cleaned = clean_blank_values(unified)

        validated = add_validation_status(
            add_opening_date_status(
                add_zip_quality_status(cleaned)
            )
        )

        invalid_stores = validated.filter(
            F.col("validation_status") != "VALID"
        )

        invalid_count = invalid_stores.count()

        print(f"Invalid records: {invalid_count:,}")

        if invalid_count > 0:
            raise ValueError(
                f"{invalid_count} invalid stores found. "
                "Quarantine handling is required "
                "before writing."
            )

        # 4. Deduplicate valid records.
        valid_stores = validated.filter(
            F.col("validation_status") == "VALID"
        )

        silver_stores = deduplicate_stores(
            valid_stores
        )

        output_count = silver_stores.count()

        print(f"Cleaned records: {output_count:,}")

        # 5. Pre-write validations.
        assert input_count == 90
        assert output_count == 90

        duplicates = (
            silver_stores
            .groupBy(
                "source_system",
                "source_store_id"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert duplicates == 0

        print("PASS: No duplicate store IDs")

        invalid_zip_flags = silver_stores.filter(
            F.col("zip_quality_status") != "VALID"
        ).count()

        assert invalid_zip_flags == 0

        print("PASS: All store ZIP codes valid")

        invalid_date_flags = silver_stores.filter(
            F.col("opening_date_status") != "VALID"
        ).count()

        assert invalid_date_flags == 0

        print("PASS: All opening dates valid")

        # 6. Write Silver Delta table to S3.
        print("\n=== WRITING SILVER STORES TO S3 ===")

        require_new_s3_target(spark, SILVER_STORES_PATH)
        (
            silver_stores.write
            .format("delta")
            .mode("errorifexists")
            .option("overwriteSchema", "true")
            .save(SILVER_STORES_PATH)
        )

        # 7. Read the persisted Delta table.
        print("\n=== VALIDATING PERSISTED S3 TABLE ===")

        verified = (
            spark.read
            .format("delta")
            .load(SILVER_STORES_PATH)
        )

        verified_count = verified.count()

        assert verified_count == 90

        print(
            f"PASS: S3 Silver stores = "
            f"{verified_count:,}"
        )

        print("\n=== COUNTS BY COMPANY ===")

        verified.groupBy(
            "source_system"
        ).count().show()

        print("\n=== ZIP QUALITY STATUS ===")

        verified.groupBy(
            "source_system",
            "zip_quality_status"
        ).count().orderBy(
            "source_system"
        ).show()

        print("\n=== OPENING DATE STATUS ===")

        verified.groupBy(
            "source_system",
            "opening_date_status"
        ).count().orderBy(
            "source_system"
        ).show()

        print("\n=== SILVER STORES SCHEMA ===")

        verified.printSchema()

        # 8. Verify Delta transaction history.
        assert DeltaTable.isDeltaTable(
            spark,
            SILVER_STORES_PATH
        )

        print("\n=== DELTA TRANSACTION HISTORY ===")

        DeltaTable.forPath(
            spark,
            SILVER_STORES_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: SILVER STORES "
            "DELTA TABLE CREATED!"
        )

        print(f"\nS3 Path: {SILVER_STORES_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    require_safe_write_environment()
    main()
