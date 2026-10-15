
import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(__file__))

from transform_customers import (
    create_spark,
    transform_retailmax_customers,
    transform_freshmart_customers,
    BRONZE_BASE,
)

from clean_customers import (
    clean_blank_values,
    deduplicate_customers,
)


BUCKET = "retail-lakehouse-parsam-2026"

SILVER_CUSTOMERS_PATH = (
    f"s3a://{BUCKET}/silver/customers"
)


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== SILVER CUSTOMER PIPELINE ===")

        # Step 1: Read Bronze Delta tables.
        retailmax_bronze = (
            spark.read
            .format("delta")
            .load(f"{BRONZE_BASE}/retailmax/customers")
        )

        freshmart_bronze = (
            spark.read
            .format("delta")
            .load(f"{BRONZE_BASE}/freshmart/customers")
        )

        # Step 2: Standardize source schemas.
        retailmax = transform_retailmax_customers(
            retailmax_bronze
        )

        freshmart = transform_freshmart_customers(
            freshmart_bronze
        )

        # Step 3: Combine source systems.
        unified = retailmax.unionByName(freshmart)

        input_count = unified.count()

        print(f"Input records: {input_count:,}")

        # Step 4: Clean blank values.
        cleaned = clean_blank_values(unified)

        # Step 5: Validate required customer identifiers.
        invalid_customers = cleaned.filter(
            F.col("source_customer_id").isNull()
            | F.col("customer_key").isNull()
            | F.col("registration_date").isNull()
        )

        invalid_count = invalid_customers.count()

        print(f"Invalid records: {invalid_count:,}")

        if invalid_count > 0:
            raise ValueError(
                "Invalid customer records detected. "
                "Review and quarantine these records "
                "before writing Silver."
            )

        # Step 6: Deduplicate source customer IDs.
        silver_customers = deduplicate_customers(
            cleaned
        )

        # Step 7: Validate expected counts.
        output_count = silver_customers.count()

        print(f"Cleaned records: {output_count:,}")

        assert input_count == 9090
        assert output_count == 9000

        remaining_duplicates = (
            silver_customers
            .groupBy("source_system", "source_customer_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert remaining_duplicates == 0

        # Step 8: Write the Silver Delta table.
        print("\n=== WRITING SILVER CUSTOMERS TO S3 ===")

        (
            silver_customers.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(SILVER_CUSTOMERS_PATH)
        )

        # Step 9: Read back and validate.
        print("\n=== VALIDATING S3 SILVER TABLE ===")

        verified = (
            spark.read
            .format("delta")
            .load(SILVER_CUSTOMERS_PATH)
        )

        verified_count = verified.count()

        assert verified_count == 9000

        print(
            f"PASS: S3 Silver customers = "
            f"{verified_count:,}"
        )

        print("\n=== SILVER COUNTS BY COMPANY ===")

        verified.groupBy(
            "source_system"
        ).count().show()

        print("\n=== SILVER SCHEMA ===")

        verified.printSchema()

        print(
            "\nSUCCESS: SILVER CUSTOMER "
            "DELTA TABLE CREATED!"
        )

        print(f"\nS3 Path: {SILVER_CUSTOMERS_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
