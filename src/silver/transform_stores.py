
import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(__file__))

from transform_customers import (
    create_spark,
    BRONZE_BASE,
)


def transform_retailmax_stores(df):
    """
    Standardize RetailMax store columns.
    """

    return df.select(
        F.lit("retailmax").alias("source_system"),

        F.trim(F.col("store_id")).alias(
            "source_store_id"
        ),

        F.concat(
            F.lit("retailmax:"),
            F.trim(F.col("store_id"))
        ).alias("store_key"),

        F.trim(F.col("store_name")).alias(
            "store_name"
        ),

        F.trim(F.col("city")).alias("city"),

        F.upper(
            F.trim(F.col("state"))
        ).alias("state"),

        F.trim(F.col("zip_code")).alias("zip_code"),

        F.to_date(
            F.col("opened_date"),
            "yyyy-MM-dd"
        ).alias("opened_date"),

        F.col("_ingestion_timestamp").alias(
            "ingestion_timestamp"
        ),

        F.col("_batch_id").alias("batch_id"),

        F.col("_source_file").alias("source_file")
    )


def transform_freshmart_stores(df):
    """
    Standardize FreshMart location columns.
    """

    return df.select(
        F.lit("freshmart").alias("source_system"),

        F.trim(F.col("location_code")).alias(
            "source_store_id"
        ),

        F.concat(
            F.lit("freshmart:"),
            F.trim(F.col("location_code"))
        ).alias("store_key"),

        F.trim(F.col("location_name")).alias(
            "store_name"
        ),

        F.trim(F.col("city_name")).alias("city"),

        F.upper(
            F.trim(F.col("state_code"))
        ).alias("state"),

        F.trim(F.col("postal_code")).alias("zip_code"),

        F.to_date(
            F.col("launch_date"),
            "MM/dd/yyyy"
        ).alias("opened_date"),

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
        print("\n=== READING STORE BRONZE TABLES ===")

        retailmax_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/retailmax/stores")
        )

        freshmart_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/freshmart/stores")
        )

        print("\n=== STANDARDIZING STORE SCHEMAS ===")

        retailmax = transform_retailmax_stores(
            retailmax_bronze
        )

        freshmart = transform_freshmart_stores(
            freshmart_bronze
        )

        unified = retailmax.unionByName(freshmart)

        print("\n=== UNIFIED STORE SCHEMA ===")
        unified.printSchema()

        print("\n=== STORE COUNTS BY COMPANY ===")

        unified.groupBy("source_system").count().show()

        print("\n=== SAMPLE STANDARDIZED STORES ===")

        unified.select(
            "source_system",
            "source_store_id",
            "store_key",
            "store_name",
            "city",
            "state",
            "zip_code",
            "opened_date"
        ).show(10, truncate=False)

        total = unified.count()

        print(f"\nTotal store records: {total:,}")

        assert total == 90, (
            f"Expected 90 stores, found {total}"
        )

        print(
            "\nSUCCESS: STORE SCHEMA "
            "STANDARDIZATION COMPLETED!"
        )

        print(
            "\nNOTE: No Silver store table "
            "has been written yet."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
