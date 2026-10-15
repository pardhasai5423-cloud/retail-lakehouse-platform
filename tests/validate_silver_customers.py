
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


SILVER_PATH = (
    "s3a://retail-lakehouse-parsam-2026/silver/customers"
)


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== VALIDATING S3 SILVER CUSTOMERS ===")

        df = spark.read.format("delta").load(SILVER_PATH)

        # 1. Total record count
        total = df.count()
        assert total == 9000, f"Unexpected count: {total}"
        print(f"PASS: Total customers = {total:,}")

        # 2. Source-level counts
        counts = {
            row["source_system"]: row["count"]
            for row in df.groupBy("source_system").count().collect()
        }

        assert counts == {
            "retailmax": 5000,
            "freshmart": 4000,
        }, f"Unexpected source counts: {counts}"

        print("PASS: RetailMax = 5,000")
        print("PASS: FreshMart = 4,000")

        # 3. Composite key uniqueness
        duplicate_count = (
            df.groupBy("source_system", "source_customer_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert duplicate_count == 0
        print("PASS: No duplicate source customer IDs")

        # 4. Required identifiers and registration dates
        invalid_count = df.filter(
            F.col("source_system").isNull()
            | F.col("source_customer_id").isNull()
            | (F.trim(F.col("source_customer_id")) == "")
            | F.col("customer_key").isNull()
            | F.col("registration_date").isNull()
        ).count()

        assert invalid_count == 0
        print("PASS: Required identifiers and dates present")

        # 5. Verify customer key construction
        incorrect_keys = df.filter(
            F.col("customer_key") != F.concat(
                F.col("source_system"),
                F.lit(":"),
                F.col("source_customer_id")
            )
        ).count()

        assert incorrect_keys == 0
        print("PASS: Customer keys are correctly constructed")

        # 6. Missing emails are preserved
        missing_emails = df.filter(
            F.col("email").isNull()
            | (F.trim(F.col("email")) == "")
        ).count()

        assert missing_emails == 182, (
            f"Unexpected missing email count: {missing_emails}"
        )

        print(f"PASS: Missing emails preserved = {missing_emails}")

        # 7. Lineage metadata
        missing_lineage = df.filter(
            F.col("ingestion_timestamp").isNull()
            | F.col("batch_id").isNull()
            | F.col("source_file").isNull()
        ).count()

        assert missing_lineage == 0
        print("PASS: Ingestion lineage metadata present")

        # 8. Verify Delta table and transaction history
        assert DeltaTable.isDeltaTable(spark, SILVER_PATH)
        print("PASS: Valid Delta Lake table")

        print("\n=== DELTA TRANSACTION HISTORY ===")

        delta_table = DeltaTable.forPath(spark, SILVER_PATH)

        delta_table.history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: ALL SILVER CUSTOMER "
            "VALIDATIONS PASSED!"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
