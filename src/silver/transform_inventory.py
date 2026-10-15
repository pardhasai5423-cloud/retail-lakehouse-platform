
import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(__file__))

from transform_customers import (
    create_spark,
    BRONZE_BASE,
)


def parse_inventory_date(column_name):
    """
    Parse either ISO or US-style dates.

    This handles the date-format differences
    between RetailMax and FreshMart.
    """
    return F.coalesce(
        F.to_date(F.col(column_name), "yyyy-MM-dd"),
        F.to_date(F.col(column_name), "MM/dd/yyyy"),
    )


def transform_retailmax_inventory(df):
    """
    Standardize RetailMax inventory records.
    """

    return df.select(
        F.lit("retailmax").alias("source_system"),

        F.trim(F.col("inventory_id")).alias(
            "source_inventory_id"
        ),

        F.concat(
            F.lit("retailmax:"),
            F.trim(F.col("inventory_id"))
        ).alias("inventory_key"),

        F.trim(F.col("product_id")).alias(
            "source_product_id"
        ),

        F.concat(
            F.lit("retailmax:"),
            F.trim(F.col("product_id"))
        ).alias("product_key"),

        F.trim(F.col("store_id")).alias(
            "source_store_id"
        ),

        F.concat(
            F.lit("retailmax:"),
            F.trim(F.col("store_id"))
        ).alias("store_key"),

        F.trim(F.col("stock_quantity")).cast(
            "int"
        ).alias("stock_quantity"),

        F.trim(F.col("reorder_level")).cast(
            "int"
        ).alias("reorder_level"),

        parse_inventory_date(
            "last_updated"
        ).alias("last_updated"),

        F.col("_ingestion_timestamp").alias(
            "ingestion_timestamp"
        ),

        F.col("_batch_id").alias("batch_id"),

        F.col("_source_file").alias("source_file")
    )


def transform_freshmart_inventory(df):
    """
    Standardize FreshMart inventory records.
    """

    return df.select(
        F.lit("freshmart").alias("source_system"),

        F.trim(F.col("stock_record_id")).alias(
            "source_inventory_id"
        ),

        F.concat(
            F.lit("freshmart:"),
            F.trim(F.col("stock_record_id"))
        ).alias("inventory_key"),

        F.trim(F.col("sku_code")).alias(
            "source_product_id"
        ),

        F.concat(
            F.lit("freshmart:"),
            F.trim(F.col("sku_code"))
        ).alias("product_key"),

        F.trim(F.col("location_code")).alias(
            "source_store_id"
        ),

        F.concat(
            F.lit("freshmart:"),
            F.trim(F.col("location_code"))
        ).alias("store_key"),

        F.trim(F.col("available_units")).cast(
            "int"
        ).alias("stock_quantity"),

        F.trim(F.col("minimum_stock")).cast(
            "int"
        ).alias("reorder_level"),

        parse_inventory_date(
            "stock_update_date"
        ).alias("last_updated"),

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
        print("\n=== READING INVENTORY BRONZE TABLES ===")

        retailmax_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/retailmax/inventory")
        )

        freshmart_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/freshmart/inventory")
        )

        print("\n=== STANDARDIZING INVENTORY SCHEMAS ===")

        retailmax = transform_retailmax_inventory(
            retailmax_bronze
        )

        freshmart = transform_freshmart_inventory(
            freshmart_bronze
        )

        unified = retailmax.unionByName(freshmart)

        print("\n=== UNIFIED INVENTORY SCHEMA ===")

        unified.printSchema()

        print("\n=== INVENTORY COUNTS BY COMPANY ===")

        unified.groupBy(
            "source_system"
        ).count().show()

        print("\n=== SAMPLE STANDARDIZED INVENTORY ===")

        unified.select(
            "source_system",
            "source_inventory_id",
            "inventory_key",
            "product_key",
            "store_key",
            "stock_quantity",
            "reorder_level",
            "last_updated"
        ).show(10, truncate=False)

        total = unified.count()

        print(f"\nTotal inventory records: {total:,}")

        assert total == 3600, (
            f"Expected 3600 inventory records, "
            f"found {total}"
        )

        print(
            "\nSUCCESS: INVENTORY SCHEMA "
            "STANDARDIZATION COMPLETED!"
        )

        print(
            "\nNOTE: No Silver Inventory table "
            "has been written yet."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
