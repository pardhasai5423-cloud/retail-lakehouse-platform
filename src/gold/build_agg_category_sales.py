from src.config.write_safety import require_new_s3_target

import os
import sys
from decimal import Decimal

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from src.config.settings import get_s3_path


PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)

sys.path.insert(
    0,
    os.path.join(PROJECT_ROOT, "src", "silver")
)

from transform_customers import create_spark
from src.config.settings import require_safe_write_environment


GOLD_BASE = get_s3_path("gold")

FACT_PATH = get_s3_path(
    "gold", "fact_sales"
)

PRODUCTS_PATH = get_s3_path(
    "gold", "dim_products"
)

OUTPUT_PATH = get_s3_path(
    "gold", "agg_category_sales"
)


EXPECTED_SALES = 88600
EXPECTED_PRODUCTS = 1800

EXPECTED_SOURCE_COUNTS = {
    "retailmax": 49000,
    "freshmart": 39600,
}

EXPECTED_REVENUE = {
    "retailmax": Decimal("21606073.80"),
    "freshmart": Decimal("16896648.70"),
}


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(
            "\n=== GOLD CATEGORY SALES "
            "AGGREGATION PIPELINE ==="
        )

        # 1. Read Gold Sales and Products

        sales = (
            spark.read.format("delta")
            .load(FACT_PATH)
        )

        products = (
            spark.read.format("delta")
            .load(PRODUCTS_PATH)
        )

        assert sales.count() == EXPECTED_SALES
        assert products.count() == EXPECTED_PRODUCTS

        print(
            f"Gold Sales transactions: "
            f"{EXPECTED_SALES:,}"
        )

        print(
            f"Gold Products: "
            f"{EXPECTED_PRODUCTS:,}"
        )

        # 2. Validate Product dimension keys

        invalid_product_keys = products.filter(
            F.col("product_key").isNull()
            | (F.trim("product_key") == "")
        ).count()

        duplicate_product_keys = (
            products.groupBy("product_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert invalid_product_keys == 0
        assert duplicate_product_keys == 0

        print(
            "PASS: Product dimension keys "
            "unique and valid"
        )

        # 3. Validate Sales-to-Products coverage

        missing_products = (
            sales.select("product_key")
            .join(
                products.select("product_key"),
                on="product_key",
                how="left_anti"
            )
            .count()
        )

        assert missing_products == 0

        print(
            "PASS: All Sales reference "
            "valid Gold Products"
        )

        # 4. Check that Sales and Product
        # source systems agree.

        product_lookup = products.select(
            F.col("product_key"),
            F.col("source_system").alias(
                "product_source_system"
            ),
            F.col("category")
        )

        sales_with_category = (
            sales.alias("s")
            .join(
                product_lookup.alias("p"),
                on="product_key",
                how="inner"
            )
        )

        assert (
            sales_with_category.count()
            == EXPECTED_SALES
        )

        source_mismatches = (
            sales_with_category.filter(
                ~F.col("source_system").eqNullSafe(
                    F.col("product_source_system")
                )
            )
            .count()
        )

        assert source_mismatches == 0

        print(
            "PASS: Product and Sales "
            "source systems match"
        )

        # 5. Check category completeness

        missing_categories = (
            sales_with_category.filter(
                F.col("category").isNull()
                | (F.trim("category") == "")
            )
            .count()
        )

        assert missing_categories == 0, (
            "Sales contain products "
            "without categories"
        )

        print(
            "PASS: All sold products "
            "have categories"
        )

        # 6. Aggregate at source/category grain

        category_sales = (
            sales_with_category.groupBy(
                "source_system",
                "category"
            )
            .agg(
                F.count("*").alias(
                    "transaction_count"
                ),
                F.sum("quantity").alias(
                    "units_sold"
                ),
                F.sum(
                    F.col("quantity")
                    * F.col("unit_price")
                ).cast("decimal(18,2)").alias(
                    "gross_sales_amount"
                ),
                F.sum(
                    "discount_amount"
                ).cast("decimal(18,2)").alias(
                    "discount_amount"
                ),
                F.sum(
                    "total_amount"
                ).cast("decimal(18,2)").alias(
                    "net_revenue"
                ),
                F.countDistinct(
                    "product_key"
                ).alias(
                    "distinct_products_sold"
                ),
                F.countDistinct(
                    "customer_key"
                ).alias(
                    "distinct_customers"
                ),
                F.countDistinct(
                    "store_key"
                ).alias(
                    "distinct_stores"
                )
            )
            .withColumn(
                "average_transaction_value",
                (
                    F.col("net_revenue")
                    / F.col("transaction_count")
                ).cast("decimal(18,2)")
            )
        )

        # 7. Validate aggregation grain

        duplicate_groups = (
            category_sales.groupBy(
                "source_system",
                "category"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert duplicate_groups == 0

        invalid_groups = category_sales.filter(
            F.col("source_system").isNull()
            | F.col("category").isNull()
            | (F.col("transaction_count") <= 0)
        ).count()

        assert invalid_groups == 0

        print(
            "PASS: Category aggregation "
            "grain unique and valid"
        )

        # 8. Reconcile source totals

        source_totals = (
            category_sales.groupBy(
                "source_system"
            )
            .agg(
                F.sum(
                    "transaction_count"
                ).alias("transactions"),
                F.sum(
                    "units_sold"
                ).alias("units"),
                F.sum(
                    "net_revenue"
                ).alias("revenue")
            )
            .collect()
        )

        assert len(source_totals) == 2

        fact_units = {
            row["source_system"]: row["units"]
            for row in (
                sales.groupBy("source_system")
                .agg(
                    F.sum("quantity").alias(
                        "units"
                    )
                )
                .collect()
            )
        }

        for row in source_totals:
            source = row["source_system"]

            assert source in EXPECTED_SOURCE_COUNTS

            assert (
                row["transactions"]
                == EXPECTED_SOURCE_COUNTS[source]
            )

            assert (
                row["units"]
                == fact_units[source]
            )

            assert (
                row["revenue"]
                == EXPECTED_REVENUE[source]
            )

        print(
            "PASS: Category transactions, units "
            "and revenue reconcile to Sales"
        )

        # 9. Validate financial calculations

        incorrect_financials = (
            category_sales.filter(
                F.abs(
                    F.col("gross_sales_amount")
                    - F.col("discount_amount")
                    - F.col("net_revenue")
                ) > F.lit(0.01)
            )
            .count()
        )

        assert incorrect_financials == 0

        print(
            "PASS: Category financial "
            "calculations verified"
        )

        # 10. Validate averages

        incorrect_averages = (
            category_sales.filter(
                ~F.col(
                    "average_transaction_value"
                ).eqNullSafe(
                    (
                        F.col("net_revenue")
                        / F.col("transaction_count")
                    ).cast("decimal(18,2)")
                )
            )
            .count()
        )

        assert incorrect_averages == 0

        print(
            "PASS: Average transaction "
            "values verified"
        )

        # 11. Write Delta aggregation

        print(
            "\n=== WRITING AGG_CATEGORY_SALES ==="
        )

        require_new_s3_target(spark, OUTPUT_PATH)
        (
            category_sales.write
            .format("delta")
            .mode("errorifexists")
            .option(
                "overwriteSchema", "true"
            )
            .save(OUTPUT_PATH)
        )

        assert DeltaTable.isDeltaTable(
            spark, OUTPUT_PATH
        )

        persisted = (
            spark.read.format("delta")
            .load(OUTPUT_PATH)
        )

        row_count = persisted.count()

        print(
            f"PASS: Persisted {row_count:,} "
            "category aggregation rows"
        )

        # 12. Display schema

        print(
            "\n=== CATEGORY SALES SCHEMA ==="
        )

        persisted.printSchema()

        # 13. Top categories by revenue

        print(
            "\n=== TOP CATEGORIES BY REVENUE ==="
        )

        persisted.select(
            "source_system",
            "category",
            "transaction_count",
            "units_sold",
            "net_revenue",
            "distinct_products_sold",
            "distinct_customers",
            "distinct_stores"
        ).orderBy(
            F.col("net_revenue").desc()
        ).show(20, truncate=False)

        # 14. Source-level summary

        print(
            "\n=== CATEGORY SALES BY SOURCE ==="
        )

        persisted.groupBy(
            "source_system"
        ).agg(
            F.count("*").alias(
                "categories"
            ),
            F.sum(
                "transaction_count"
            ).alias(
                "transactions"
            ),
            F.sum(
                "units_sold"
            ).alias(
                "units_sold"
            ),
            F.sum(
                "net_revenue"
            ).alias(
                "net_revenue"
            )
        ).orderBy(
            "source_system"
        ).show(truncate=False)

        # 15. Delta history

        print("\n=== DELTA HISTORY ===")

        DeltaTable.forPath(
            spark, OUTPUT_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: GOLD "
            "AGG_CATEGORY_SALES CREATED!"
        )

        print(
            f"Gold path: {OUTPUT_PATH}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    require_safe_write_environment()
    main()
