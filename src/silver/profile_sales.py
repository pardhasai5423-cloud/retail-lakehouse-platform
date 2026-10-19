
import os
import sys

from pyspark.sql import functions as F
from src.config.settings import get_s3_path

sys.path.insert(0, os.path.dirname(__file__))

from transform_customers import create_spark, BRONZE_BASE

from transform_sales import (
    transform_retailmax_sales,
    transform_freshmart_sales,
)


SILVER_BASE = get_s3_path("silver")


def missing_string(column_name):
    return (
        F.col(column_name).isNull()
        | (F.trim(F.col(column_name)) == "")
    )


def show_reference_issues(
    sales,
    reference,
    key_column,
    reference_name,
):
    """
    Find Sales records with keys that do not
    match an existing Silver master record.

    A null join key is also unmatched.
    """

    reference_keys = (
        reference
        .select(key_column)
        .distinct()
    )

    unmatched = sales.join(
        reference_keys,
        on=key_column,
        how="left_anti"
    )

    print(
        f"\n=== {reference_name.upper()} "
        "REFERENTIAL INTEGRITY ==="
    )

    unmatched.groupBy(
        "source_system"
    ).count().show()

    print(
        f"Total unmatched {reference_name} "
        f"references: {unmatched.count()}"
    )

    unmatched.select(
        "source_system",
        "source_transaction_id",
        key_column,
    ).show(10, truncate=False)


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== SALES DATA QUALITY PROFILING ===")

        # 1. Read and standardize Bronze Sales.
        retailmax_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/retailmax/sales")
        )

        freshmart_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/freshmart/sales")
        )

        retailmax = transform_retailmax_sales(
            retailmax_bronze
        )

        freshmart = transform_freshmart_sales(
            freshmart_bronze
        )

        sales = (
            retailmax.unionByName(freshmart)
            .cache()
        )

        print("\n=== 1. SALES RECORD COUNTS ===")

        sales.groupBy(
            "source_system"
        ).count().show()

        # 2. Duplicate transaction IDs.
        print("\n=== 2. DUPLICATE TRANSACTION IDS ===")

        duplicates = (
            sales.groupBy(
                "source_system",
                "source_transaction_id"
            )
            .count()
            .filter(F.col("count") > 1)
        )

        print(
            "Duplicate transaction ID groups:",
            duplicates.count()
        )

        print(
            "Extra records beyond one per ID:",
            duplicates.agg(
                F.coalesce(
                    F.sum(F.col("count") - 1),
                    F.lit(0)
                ).alias("extra")
            ).first()["extra"]
        )

        duplicates.orderBy(
            F.desc("count")
        ).show(15, truncate=False)

        # 3. Missing identifiers.
        print("\n=== 3. MISSING IDENTIFIERS ===")

        sales.groupBy("source_system").agg(
            F.sum(
                F.when(
                    missing_string("source_transaction_id"),
                    1
                ).otherwise(0)
            ).alias("missing_transaction_ids"),

            F.sum(
                F.when(
                    missing_string("source_customer_id"),
                    1
                ).otherwise(0)
            ).alias("missing_customer_ids"),

            F.sum(
                F.when(
                    missing_string("source_product_id"),
                    1
                ).otherwise(0)
            ).alias("missing_product_ids"),

            F.sum(
                F.when(
                    missing_string("source_store_id"),
                    1
                ).otherwise(0)
            ).alias("missing_store_ids"),
        ).show(truncate=False)

        # 4. Quantity quality.
        print("\n=== 4. QUANTITY QUALITY ===")

        sales.groupBy("source_system").agg(
            F.sum(
                F.when(
                    F.col("quantity").isNull(),
                    1
                ).otherwise(0)
            ).alias("null_quantities"),

            F.sum(
                F.when(
                    F.col("quantity") == 0,
                    1
                ).otherwise(0)
            ).alias("zero_quantities"),

            F.sum(
                F.when(
                    F.col("quantity") < 0,
                    1
                ).otherwise(0)
            ).alias("negative_quantities"),
        ).show(truncate=False)

        # 5. Price, discount, total quality.
        print("\n=== 5. MONETARY VALUE QUALITY ===")

        sales.groupBy("source_system").agg(
            F.sum(
                F.when(
                    F.col("unit_price").isNull(),
                    1
                ).otherwise(0)
            ).alias("null_unit_prices"),

            F.sum(
                F.when(
                    F.col("unit_price") < 0,
                    1
                ).otherwise(0)
            ).alias("negative_unit_prices"),

            F.sum(
                F.when(
                    F.col("unit_price") == 0,
                    1
                ).otherwise(0)
            ).alias("zero_unit_prices"),

            F.sum(
                F.when(
                    F.col("discount_amount").isNull(),
                    1
                ).otherwise(0)
            ).alias("null_discounts"),

            F.sum(
                F.when(
                    F.col("discount_amount") < 0,
                    1
                ).otherwise(0)
            ).alias("negative_discounts"),

            F.sum(
                F.when(
                    F.col("total_amount").isNull(),
                    1
                ).otherwise(0)
            ).alias("null_totals"),

            F.sum(
                F.when(
                    F.col("total_amount") < 0,
                    1
                ).otherwise(0)
            ).alias("negative_totals"),
        ).show(truncate=False)

        # 6. Validate discount vs gross.
        print("\n=== 6. DISCOUNT VALIDATION ===")

        gross_amount = (
            F.col("quantity") * F.col("unit_price")
        )

        sales.groupBy("source_system").agg(
            F.sum(
                F.when(
                    (F.col("quantity") > 0)
                    & (F.col("unit_price") >= 0)
                    & (F.col("discount_amount") > gross_amount),
                    1
                ).otherwise(0)
            ).alias("discount_exceeds_gross")
        ).show(truncate=False)

        # 7. Transaction total consistency.
        print("\n=== 7. TRANSACTION TOTAL CONSISTENCY ===")

        expected_total = (
            gross_amount - F.col("discount_amount")
        )

        total_difference = F.abs(
            F.col("total_amount") - expected_total
        )

        # Allow a 1-cent rounding tolerance.
        mismatch_condition = (
            F.col("quantity").isNotNull()
            & F.col("unit_price").isNotNull()
            & F.col("discount_amount").isNotNull()
            & F.col("total_amount").isNotNull()
            & (total_difference > F.lit(0.01))
        )

        sales.groupBy("source_system").agg(
            F.sum(
                F.when(
                    mismatch_condition,
                    1
                ).otherwise(0)
            ).alias("total_mismatches")
        ).show(truncate=False)

        print("\nSample total mismatches:")

        sales.filter(
            mismatch_condition
        ).select(
            "source_system",
            "source_transaction_id",
            "quantity",
            "unit_price",
            "discount_amount",
            "total_amount",
            expected_total.alias("expected_total"),
            total_difference.alias("difference")
        ).show(15, truncate=False)

        # 8. Transaction dates.
        print("\n=== 8. TRANSACTION DATE QUALITY ===")

        sales.groupBy("source_system").agg(
            F.sum(
                F.when(
                    F.col("transaction_date").isNull(),
                    1
                ).otherwise(0)
            ).alias("null_dates"),

            F.sum(
                F.when(
                    F.col("transaction_date")
                    > F.current_date(),
                    1
                ).otherwise(0)
            ).alias("future_dates"),

            F.min("transaction_date").alias(
                "earliest_date"
            ),

            F.max("transaction_date").alias(
                "latest_date"
            ),
        ).show(truncate=False)

        # 9. Read Silver master tables.
        print("\n=== 9. READING SILVER MASTER TABLES ===")

        customers = (
            spark.read.format("delta")
            .load(f"{SILVER_BASE}/customers")
        )

        products = (
            spark.read.format("delta")
            .load(f"{SILVER_BASE}/products")
        )

        stores = (
            spark.read.format("delta")
            .load(f"{SILVER_BASE}/stores")
        )

        # 10. Referential integrity.
        show_reference_issues(
            sales,
            customers,
            "customer_key",
            "Customer"
        )

        show_reference_issues(
            sales,
            products,
            "product_key",
            "Product"
        )

        show_reference_issues(
            sales,
            stores,
            "store_key",
            "Store"
        )

        # 11. Source lineage quality.
        print("\n=== 11. SOURCE LINEAGE QUALITY ===")

        sales.groupBy("source_system").agg(
            F.sum(
                F.when(
                    F.col("ingestion_timestamp").isNull(),
                    1
                ).otherwise(0)
            ).alias("missing_ingestion_timestamps"),

            F.sum(
                F.when(
                    F.col("batch_id").isNull(),
                    1
                ).otherwise(0)
            ).alias("missing_batch_ids"),

            F.sum(
                F.when(
                    F.col("source_file").isNull(),
                    1
                ).otherwise(0)
            ).alias("missing_source_files"),
        ).show(truncate=False)

        print("\n=== PROFILING COMPLETED ===")

        print(
            "\nNOTE: No Silver Sales table "
            "has been written yet."
        )

        sales.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
