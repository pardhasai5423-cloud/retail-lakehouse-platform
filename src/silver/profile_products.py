
import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(__file__))

from transform_customers import (
    create_spark,
    BRONZE_BASE,
)

from transform_products import (
    transform_retailmax_products,
    transform_freshmart_products,
)


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== PRODUCT DATA QUALITY PROFILING ===")

        # Read Bronze Delta tables.
        retailmax_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/retailmax/products")
        )

        freshmart_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/freshmart/products")
        )

        # Standardize both sources.
        retailmax = transform_retailmax_products(
            retailmax_bronze
        )

        freshmart = transform_freshmart_products(
            freshmart_bronze
        )

        products = retailmax.unionByName(freshmart)

        print("\n=== 1. TOTAL RECORD COUNTS ===")

        products.groupBy("source_system").count().show()

        print("\n=== 2. DUPLICATE PRODUCT IDS ===")

        duplicates = (
            products
            .groupBy("source_system", "source_product_id")
            .count()
            .filter(F.col("count") > 1)
        )

        duplicates.groupBy("source_system").agg(
            F.count("*").alias("duplicate_id_groups"),
            F.sum(F.col("count") - 1).alias(
                "extra_duplicate_records"
            )
        ).show()

        print("\n=== 3. MISSING PRODUCT IDS ===")

        products.groupBy("source_system").agg(
            F.sum(
                F.when(
                    F.col("source_product_id").isNull()
                    | (
                        F.trim(F.col("source_product_id"))
                        == ""
                    ),
                    1
                ).otherwise(0)
            ).alias("missing_product_ids")
        ).show()

        print("\n=== 4. MISSING PRODUCT NAMES ===")

        products.groupBy("source_system").agg(
            F.sum(
                F.when(
                    F.col("product_name").isNull()
                    | (F.trim(F.col("product_name")) == ""),
                    1
                ).otherwise(0)
            ).alias("missing_product_names")
        ).show()

        print("\n=== 5. MISSING OR INVALID PRICES ===")

        products.groupBy("source_system").agg(
            F.sum(
                F.when(
                    F.col("unit_price").isNull(),
                    1
                ).otherwise(0)
            ).alias("null_prices"),

            F.sum(
                F.when(
                    F.col("unit_price") < 0,
                    1
                ).otherwise(0)
            ).alias("negative_prices"),

            F.sum(
                F.when(
                    F.col("unit_price") == 0,
                    1
                ).otherwise(0)
            ).alias("zero_prices")
        ).show()

        print("\n=== 6. MISSING SUPPLIER IDS ===")

        products.groupBy("source_system").agg(
            F.sum(
                F.when(
                    F.col("source_supplier_id").isNull()
                    | (
                        F.trim(F.col("source_supplier_id"))
                        == ""
                    ),
                    1
                ).otherwise(0)
            ).alias("missing_supplier_ids")
        ).show()

        print("\n=== 7. NULL PRODUCT DATES ===")

        products.groupBy("source_system").agg(
            F.sum(
                F.when(
                    F.col("created_date").isNull(),
                    1
                ).otherwise(0)
            ).alias("null_created_dates")
        ).show()

        print("\n=== 8. SAMPLE INVALID PRICE RECORDS ===")

        products.filter(
            F.col("unit_price").isNull()
            | (F.col("unit_price") <= 0)
        ).select(
            "source_system",
            "source_product_id",
            "product_name",
            "unit_price"
        ).show(15, truncate=False)

        print("\n=== 9. SAMPLE DUPLICATE PRODUCT IDS ===")

        duplicates.orderBy(
            "source_system",
            "source_product_id"
        ).show(10, truncate=False)

        print("\n=== PROFILING COMPLETED ===")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
