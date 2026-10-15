
import os
import sys

from pyspark.sql import functions as F

# Allow imports from the src/silver directory.
sys.path.insert(0, os.path.dirname(__file__))

from transform_customers import (
    create_spark,
    transform_retailmax_customers,
    transform_freshmart_customers,
    BRONZE_BASE,
)


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== CUSTOMER DATA QUALITY PROFILING ===")

        # Read the two Bronze Delta tables from AWS S3.
        retailmax_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/retailmax/customers")
        )

        freshmart_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/freshmart/customers")
        )

        # Reuse the existing standardization functions.
        retailmax = transform_retailmax_customers(
            retailmax_bronze
        )

        freshmart = transform_freshmart_customers(
            freshmart_bronze
        )

        customers = retailmax.unionByName(freshmart)

        print("\n=== 1. RECORD COUNTS ===")
        customers.groupBy("source_system").count().show()

        print("\n=== 2. DUPLICATE CUSTOMER IDS ===")

        duplicate_ids = (
            customers
            .groupBy("source_system", "source_customer_id")
            .count()
            .filter(F.col("count") > 1)
        )

        duplicate_ids.groupBy("source_system").agg(
            F.count("*").alias("duplicate_id_groups"),
            F.sum(F.col("count") - 1).alias(
                "extra_duplicate_records"
            )
        ).show()

        print("\n=== 3. MISSING EMAIL ADDRESSES ===")

        customers.groupBy("source_system").agg(
            F.sum(
                F.when(
                    F.col("email").isNull()
                    | (F.trim(F.col("email")) == ""),
                    1
                ).otherwise(0)
            ).alias("missing_emails")
        ).show()

        print("\n=== 4. INVALID EMAIL FORMATS ===")

        email_pattern = (
            r"^[A-Za-z0-9._%+-]+@"
            r"[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
        )

        customers.groupBy("source_system").agg(
            F.sum(
                F.when(
                    F.col("email").isNotNull()
                    & (F.trim(F.col("email")) != "")
                    & (~F.col("email").rlike(email_pattern)),
                    1
                ).otherwise(0)
            ).alias("invalid_email_formats")
        ).show()

        print("\n=== 5. NULL REGISTRATION DATES ===")

        customers.groupBy("source_system").agg(
            F.sum(
                F.when(
                    F.col("registration_date").isNull(),
                    1
                ).otherwise(0)
            ).alias("null_registration_dates")
        ).show()

        print("\n=== 6. MISSING CUSTOMER IDS ===")

        customers.groupBy("source_system").agg(
            F.sum(
                F.when(
                    F.col("source_customer_id").isNull()
                    | (
                        F.trim(
                            F.col("source_customer_id")
                        ) == ""
                    ),
                    1
                ).otherwise(0)
            ).alias("missing_customer_ids")
        ).show()

        print("\n=== 7. CROSS-COMPANY SHARED EMAILS ===")

        shared_emails = (
            customers
            .filter(
                F.col("email").isNotNull()
                & (F.col("email") != "")
            )
            .groupBy("email")
            .agg(
                F.countDistinct("source_system").alias(
                    "company_count"
                )
            )
            .filter(F.col("company_count") > 1)
        )

        shared_count = shared_emails.count()

        print(
            "Distinct emails appearing in both companies:",
            shared_count
        )

        print("\n=== 8. EXAMPLE DUPLICATE CUSTOMER IDS ===")

        duplicate_ids.orderBy(
            "source_system", "source_customer_id"
        ).show(10, truncate=False)

        print("\n=== PROFILING COMPLETED ===")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
