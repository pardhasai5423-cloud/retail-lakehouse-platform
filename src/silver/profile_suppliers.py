
import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(__file__))

from transform_customers import (
    create_spark,
    BRONZE_BASE,
)

from transform_suppliers import (
    transform_retailmax_suppliers,
    transform_freshmart_suppliers,
)


def missing(column_name):
    """
    Return a condition for null or blank string values.
    """
    return (
        F.col(column_name).isNull()
        | (F.trim(F.col(column_name)) == "")
    )


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== SUPPLIER DATA QUALITY PROFILING ===")

        # Read supplier Bronze Delta tables from S3.
        retailmax_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/retailmax/suppliers")
        )

        freshmart_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/freshmart/suppliers")
        )

        # Standardize the source schemas.
        retailmax = transform_retailmax_suppliers(
            retailmax_bronze
        )

        freshmart = transform_freshmart_suppliers(
            freshmart_bronze
        )

        suppliers = retailmax.unionByName(freshmart)

        print("\n=== 1. SUPPLIER COUNTS ===")

        suppliers.groupBy("source_system").count().show()

        # -------------------------------------------
        # 2. Duplicate supplier IDs
        # -------------------------------------------

        print("\n=== 2. DUPLICATE SUPPLIER IDS ===")

        duplicates = (
            suppliers
            .groupBy(
                "source_system",
                "source_supplier_id"
            )
            .count()
            .filter(F.col("count") > 1)
        )

        duplicates.show(20, truncate=False)

        duplicate_count = duplicates.count()

        print(
            f"Duplicate supplier ID groups: {duplicate_count}"
        )

        # -------------------------------------------
        # 3. Missing mandatory fields
        # -------------------------------------------

        print("\n=== 3. MISSING SUPPLIER IDS AND NAMES ===")

        suppliers.groupBy("source_system").agg(
            F.sum(
                F.when(
                    missing("source_supplier_id"), 1
                ).otherwise(0)
            ).alias("missing_supplier_ids"),

            F.sum(
                F.when(
                    missing("supplier_name"), 1
                ).otherwise(0)
            ).alias("missing_supplier_names")
        ).show()

        # -------------------------------------------
        # 4. Contact email quality
        # -------------------------------------------

        print("\n=== 4. CONTACT EMAIL QUALITY ===")

        # Basic syntax check, not full email verification.
        email_pattern = (
            r"^[A-Za-z0-9._%+-]+@"
            r"[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
        )

        suppliers.groupBy("source_system").agg(
            F.sum(
                F.when(
                    missing("contact_email"), 1
                ).otherwise(0)
            ).alias("missing_emails"),

            F.sum(
                F.when(
                    ~missing("contact_email")
                    & ~F.col("contact_email").rlike(
                        email_pattern
                    ),
                    1
                ).otherwise(0)
            ).alias("invalid_email_formats")
        ).show()

        # -------------------------------------------
        # 5. Missing contact and location details
        # -------------------------------------------

        print("\n=== 5. MISSING CONTACT / LOCATION DATA ===")

        suppliers.groupBy("source_system").agg(
            F.sum(
                F.when(
                    missing("phone"), 1
                ).otherwise(0)
            ).alias("missing_phones"),

            F.sum(
                F.when(
                    missing("city"), 1
                ).otherwise(0)
            ).alias("missing_cities"),

            F.sum(
                F.when(
                    missing("state"), 1
                ).otherwise(0)
            ).alias("missing_states")
        ).show()

        # -------------------------------------------
        # 6. Shared contact emails across companies
        # -------------------------------------------

        print("\n=== 6. CROSS-COMPANY SHARED EMAILS ===")

        valid_emails = suppliers.filter(
            ~missing("contact_email")
        )

        shared_emails = (
            valid_emails
            .groupBy("contact_email")
            .agg(
                F.countDistinct(
                    "source_system"
                ).alias("source_count"),

                F.count("*").alias(
                    "supplier_records"
                )
            )
            .filter(F.col("source_count") > 1)
        )

        shared_email_count = shared_emails.count()

        print(
            "Distinct contact emails shared across "
            f"companies: {shared_email_count}"
        )

        shared_emails.orderBy(
            F.desc("supplier_records"),
            "contact_email"
        ).show(15, truncate=False)

        # -------------------------------------------
        # 7. Supplier name overlaps
        # -------------------------------------------

        print("\n=== 7. CROSS-COMPANY SHARED NAMES ===")

        normalized_names = suppliers.withColumn(
            "normalized_supplier_name",
            F.lower(
                F.regexp_replace(
                    F.trim(F.col("supplier_name")),
                    r"\s+",
                    " "
                )
            )
        ).filter(
            ~missing("normalized_supplier_name")
        )

        shared_names = (
            normalized_names
            .groupBy("normalized_supplier_name")
            .agg(
                F.countDistinct(
                    "source_system"
                ).alias("source_count"),

                F.count("*").alias(
                    "supplier_records"
                )
            )
            .filter(F.col("source_count") > 1)
        )

        print(
            "Distinct supplier names shared across "
            f"companies: {shared_names.count()}"
        )

        shared_names.orderBy(
            F.desc("supplier_records")
        ).show(15, truncate=False)

        print("\n=== PROFILING COMPLETED ===")

        print(
            "\nNOTE: No Silver supplier table "
            "has been written yet."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
