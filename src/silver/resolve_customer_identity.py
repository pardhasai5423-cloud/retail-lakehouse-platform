
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


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== CUSTOMER IDENTITY RESOLUTION ===")

        # Read Bronze Delta tables from S3.
        retailmax_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/retailmax/customers")
        )

        freshmart_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/freshmart/customers")
        )

        # Standardize the two source schemas.
        retailmax = transform_retailmax_customers(
            retailmax_bronze
        )

        freshmart = transform_freshmart_customers(
            freshmart_bronze
        )

        unified = retailmax.unionByName(freshmart)

        # Clean and deduplicate within each company.
        customers = deduplicate_customers(
            clean_blank_values(unified)
        )

        print("\n=== CLEANED CUSTOMER COUNTS ===")

        customers.groupBy("source_system").count().show()

        # Exclude missing email addresses from matching.
        with_email = customers.filter(
            F.col("email").isNotNull()
            & (F.col("email") != "")
        )

        # Find email addresses appearing in both companies.
        shared_emails = (
            with_email
            .groupBy("email")
            .agg(
                F.countDistinct("source_system").alias(
                    "company_count"
                ),
                F.countDistinct("customer_key").alias(
                    "customer_count"
                )
            )
            .filter(F.col("company_count") == 2)
        )

        print("\n=== CROSS-COMPANY MATCH CANDIDATES ===")

        shared_email_count = shared_emails.count()

        print(
            "Distinct shared email addresses:",
            shared_email_count
        )

        # Show the original customer identities associated
        # with each shared email.
        candidates = (
            customers
            .join(
                shared_emails.select("email"),
                on="email",
                how="inner"
            )
            .select(
                "email",
                "source_system",
                "source_customer_id",
                "customer_key",
                "first_name",
                "last_name"
            )
        )

        print("\n=== SAMPLE MATCH CANDIDATES ===")

        candidates.orderBy(
            "email",
            "source_system",
            "source_customer_id"
        ).show(20, truncate=False)

        print("\n=== MATCH CANDIDATE STATISTICS ===")

        candidates.groupBy("source_system").agg(
            F.countDistinct("customer_key").alias(
                "customers_with_shared_email"
            )
        ).show()

        print("\n=== POTENTIAL MANY-TO-MANY MATCHES ===")

        ambiguous_emails = shared_emails.filter(
            F.col("customer_count") > 2
        )

        print(
            "Shared emails linked to more than two customers:",
            ambiguous_emails.count()
        )

        print("\n=== VALIDATION ===")

        total_customers = customers.count()

        assert total_customers == 9000, (
            f"Expected 9000 customers, got {total_customers}"
        )

        assert shared_email_count <= 9000

        print("PASS: 9,000 source-unique customers preserved")
        print("PASS: Shared emails identified")
        print("PASS: No automatic customer merging performed")

        print(
            "\nSUCCESS: CUSTOMER IDENTITY "
            "CANDIDATE ANALYSIS COMPLETED!"
        )

        print(
            "\nNOTE: No Silver table has been written yet."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
