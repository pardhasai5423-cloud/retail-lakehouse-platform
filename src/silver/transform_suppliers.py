
import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(__file__))

from transform_customers import (
    create_spark,
    BRONZE_BASE,
)


def transform_retailmax_suppliers(df):
    """
    Standardize RetailMax supplier columns.
    """

    return df.select(
        F.lit("retailmax").alias("source_system"),

        F.trim(F.col("supplier_id")).alias(
            "source_supplier_id"
        ),

        F.concat(
            F.lit("retailmax:"),
            F.trim(F.col("supplier_id"))
        ).alias("supplier_key"),

        F.trim(F.col("supplier_name")).alias(
            "supplier_name"
        ),

        F.lower(
            F.trim(F.col("contact_email"))
        ).alias("contact_email"),

        F.trim(F.col("phone")).alias("phone"),

        F.trim(F.col("city")).alias("city"),

        F.upper(
            F.trim(F.col("state"))
        ).alias("state"),

        F.col("_ingestion_timestamp").alias(
            "ingestion_timestamp"
        ),

        F.col("_batch_id").alias("batch_id"),

        F.col("_source_file").alias("source_file")
    )


def transform_freshmart_suppliers(df):
    """
    Standardize FreshMart vendor columns.
    """

    return df.select(
        F.lit("freshmart").alias("source_system"),

        F.trim(F.col("vendor_code")).alias(
            "source_supplier_id"
        ),

        F.concat(
            F.lit("freshmart:"),
            F.trim(F.col("vendor_code"))
        ).alias("supplier_key"),

        F.trim(F.col("vendor_name")).alias(
            "supplier_name"
        ),

        F.lower(
            F.trim(F.col("email_address"))
        ).alias("contact_email"),

        F.trim(F.col("contact_number")).alias("phone"),

        F.trim(F.col("city_name")).alias("city"),

        F.upper(
            F.trim(F.col("state_code"))
        ).alias("state"),

        F.col("_ingestion_timestamp").alias(
            "ingestion_timestamp"
        ),

        F.col("_batch_id").alias("batch_id"),

        F.col("_source_file").alias("source_file")
    )


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== READING SUPPLIER BRONZE TABLES ===")

        retailmax_bronze = (
            spark.read
            .format("delta")
            .load(f"{BRONZE_BASE}/retailmax/suppliers")
        )

        freshmart_bronze = (
            spark.read
            .format("delta")
            .load(f"{BRONZE_BASE}/freshmart/suppliers")
        )

        print("\n=== STANDARDIZING SUPPLIER SCHEMAS ===")

        retailmax = transform_retailmax_suppliers(
            retailmax_bronze
        )

        freshmart = transform_freshmart_suppliers(
            freshmart_bronze
        )

        unified = retailmax.unionByName(freshmart)

        print("\n=== UNIFIED SUPPLIER SCHEMA ===")
        unified.printSchema()

        print("\n=== SUPPLIER COUNTS BY COMPANY ===")

        unified.groupBy("source_system").count().show()

        print("\n=== SAMPLE STANDARDIZED SUPPLIERS ===")

        unified.select(
            "source_system",
            "source_supplier_id",
            "supplier_key",
            "supplier_name",
            "contact_email",
            "city",
            "state"
        ).show(10, truncate=False)

        total = unified.count()

        print(f"\nTotal supplier records: {total:,}")

        assert total == 180, (
            f"Expected 180 suppliers, got {total}"
        )

        print(
            "\nSUCCESS: SUPPLIER SCHEMA "
            "STANDARDIZATION COMPLETED!"
        )

        print(
            "\nNOTE: No Silver supplier table "
            "has been written yet."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
