
import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(__file__))

from transform_customers import (
    create_spark,
    BRONZE_BASE,
)


def transform_retailmax_products(df):
    """
    Standardize RetailMax product columns.
    """

    return df.select(
        F.lit("retailmax").alias("source_system"),

        F.trim(F.col("product_id")).alias(
            "source_product_id"
        ),

        F.concat(
            F.lit("retailmax:"),
            F.trim(F.col("product_id"))
        ).alias("product_key"),

        F.trim(F.col("product_name")).alias(
            "product_name"
        ),

        F.trim(F.col("category")).alias("category"),

        F.trim(F.col("brand")).alias("brand"),

        F.col("unit_price").cast(
            "decimal(12,2)"
        ).alias("unit_price"),

        F.trim(F.col("supplier_id")).alias(
            "source_supplier_id"
        ),

        F.to_date(
            F.col("created_at"),
            "yyyy-MM-dd"
        ).alias("created_date"),

        F.col("_ingestion_timestamp").alias(
            "ingestion_timestamp"
        ),

        F.col("_batch_id").alias("batch_id"),
        F.col("_source_file").alias("source_file")
    )


def transform_freshmart_products(df):
    """
    Standardize FreshMart product columns.
    """

    return df.select(
        F.lit("freshmart").alias("source_system"),

        F.trim(F.col("sku_code")).alias(
            "source_product_id"
        ),

        F.concat(
            F.lit("freshmart:"),
            F.trim(F.col("sku_code"))
        ).alias("product_key"),

        F.trim(F.col("item_description")).alias(
            "product_name"
        ),

        F.trim(F.col("department")).alias("category"),

        F.trim(F.col("brand_name")).alias("brand"),

        F.col("selling_price").cast(
            "decimal(12,2)"
        ).alias("unit_price"),

        F.trim(F.col("vendor_code")).alias(
            "source_supplier_id"
        ),

        F.to_date(
            F.col("listed_date"),
            "MM/dd/yyyy"
        ).alias("created_date"),

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
        print("\n=== READING PRODUCT BRONZE TABLES ===")

        retailmax_bronze = (
            spark.read
            .format("delta")
            .load(f"{BRONZE_BASE}/retailmax/products")
        )

        freshmart_bronze = (
            spark.read
            .format("delta")
            .load(f"{BRONZE_BASE}/freshmart/products")
        )

        print("\n=== STANDARDIZING PRODUCT SCHEMAS ===")

        retailmax = transform_retailmax_products(
            retailmax_bronze
        )

        freshmart = transform_freshmart_products(
            freshmart_bronze
        )

        unified = retailmax.unionByName(freshmart)

        print("\n=== UNIFIED PRODUCT SCHEMA ===")
        unified.printSchema()

        print("\n=== PRODUCT COUNTS BY COMPANY ===")

        unified.groupBy("source_system").count().show()

        print("\n=== SAMPLE STANDARDIZED PRODUCTS ===")

        unified.select(
            "source_system",
            "source_product_id",
            "product_key",
            "product_name",
            "category",
            "unit_price",
            "source_supplier_id",
            "created_date"
        ).show(10, truncate=False)

        total = unified.count()

        print(f"\nTotal product records: {total:,}")

        assert total == 1800, (
            f"Expected 1800 products, got {total}"
        )

        print(
            "\nSUCCESS: PRODUCT SCHEMA "
            "STANDARDIZATION COMPLETED!"
        )

        print(
            "\nNOTE: No Silver product table "
            "has been written yet."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
