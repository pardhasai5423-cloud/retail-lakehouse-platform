
import os
import sys
from decimal import Decimal

from pyspark.sql import functions as F
from delta.tables import DeltaTable


PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)

sys.path.insert(
    0,
    os.path.join(PROJECT_ROOT, "src", "silver")
)

from transform_customers import create_spark


BUCKET = "retail-lakehouse-parsam-2026"
GOLD_BASE = f"s3a://{BUCKET}/gold"

FACT_PATH = f"{GOLD_BASE}/fact_sales"
PRODUCTS_PATH = f"{GOLD_BASE}/dim_products"
AGG_PATH = f"{GOLD_BASE}/agg_category_sales"

EXPECTED_FACT_COUNT = 88600
EXPECTED_PRODUCT_COUNT = 1800
EXPECTED_AGG_COUNT = 14

EXPECTED_SOURCES = {
    "retailmax": {
        "categories": 6,
        "transactions": 49000,
        "units": 146797,
        "revenue": Decimal("21606073.80"),
    },
    "freshmart": {
        "categories": 8,
        "transactions": 39600,
        "units": 118736,
        "revenue": Decimal("16896648.70"),
    },
}


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(
            "\n=== INDEPENDENT GOLD CATEGORY "
            "SALES VALIDATION ==="
        )

        # 1. Read persisted Gold Delta tables

        fact = (
            spark.read.format("delta")
            .load(FACT_PATH)
        )

        products = (
            spark.read.format("delta")
            .load(PRODUCTS_PATH)
        )

        actual = (
            spark.read.format("delta")
            .load(AGG_PATH)
        )

        assert DeltaTable.isDeltaTable(
            spark, AGG_PATH
        )

        print(
            "PASS: Category Sales is a Delta table"
        )

        # 2. Verify record counts

        assert fact.count() == EXPECTED_FACT_COUNT
        assert products.count() == EXPECTED_PRODUCT_COUNT
        assert actual.count() == EXPECTED_AGG_COUNT

        print(
            "PASS: Fact, Product dimension "
            "and aggregation counts verified"
        )

        # 3. Validate exact schema

        expected_types = {
            "source_system": "string",
            "category": "string",
            "transaction_count": "bigint",
            "units_sold": "bigint",
            "gross_sales_amount": "decimal(18,2)",
            "discount_amount": "decimal(18,2)",
            "net_revenue": "decimal(18,2)",
            "distinct_products_sold": "bigint",
            "distinct_customers": "bigint",
            "distinct_stores": "bigint",
            "average_transaction_value": "decimal(18,2)",
        }

        assert dict(actual.dtypes) == expected_types

        print("PASS: Category Sales schema verified")

        # 4. Validate category grain

        duplicate_groups = (
            actual.groupBy(
                "source_system",
                "category"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        invalid_groups = actual.filter(
            F.col("source_system").isNull()
            | F.col("category").isNull()
            | (F.trim("category") == "")
            | (F.col("transaction_count") <= 0)
        ).count()

        assert duplicate_groups == 0
        assert invalid_groups == 0

        print(
            "PASS: One row per company "
            "and category"
        )

        # 5. Validate Product keys

        duplicate_products = (
            products.groupBy("product_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert duplicate_products == 0

        missing_products = (
            fact.select("product_key")
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
            "unique Gold Products"
        )

        # 6. Independently join Sales to
        # Product categories.

        product_lookup = products.select(
            "product_key",
            F.col("source_system").alias(
                "product_source_system"
            ),
            "category"
        )

        joined = fact.join(
            product_lookup,
            on="product_key",
            how="inner"
        )

        assert joined.count() == EXPECTED_FACT_COUNT

        source_mismatches = joined.filter(
            ~F.col("source_system").eqNullSafe(
                F.col("product_source_system")
            )
        ).count()

        assert source_mismatches == 0

        missing_categories = joined.filter(
            F.col("category").isNull()
            | (F.trim("category") == "")
        ).count()

        assert missing_categories == 0

        print(
            "PASS: Category assignments "
            "and source systems verified"
        )

        # 7. Independently reconstruct
        # category-level metrics.

        expected = (
            joined.groupBy(
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

        # 8. Compare every persisted row
        # with the reconstructed results.

        columns = list(expected.columns)

        missing_or_changed = (
            expected.select(*columns)
            .exceptAll(
                actual.select(*columns)
            )
            .count()
        )

        unexpected_or_changed = (
            actual.select(*columns)
            .exceptAll(
                expected.select(*columns)
            )
            .count()
        )

        assert missing_or_changed == 0, (
            f"{missing_or_changed} expected "
            "category rows missing or changed"
        )

        assert unexpected_or_changed == 0, (
            f"{unexpected_or_changed} unexpected "
            "category rows"
        )

        print(
            "PASS: Every category metric "
            "matches independent calculations"
        )

        # 9. Validate source-level totals

        source_totals = (
            actual.groupBy("source_system")
            .agg(
                F.count("*").alias("categories"),
                F.sum("transaction_count").alias(
                    "transactions"
                ),
                F.sum("units_sold").alias("units"),
                F.sum("net_revenue").alias("revenue")
            )
            .collect()
        )

        assert len(source_totals) == 2

        for row in source_totals:
            source = row["source_system"]

            assert source in EXPECTED_SOURCES

            expected_values = EXPECTED_SOURCES[source]

            assert (
                row["categories"]
                == expected_values["categories"]
            )

            assert (
                row["transactions"]
                == expected_values["transactions"]
            )

            assert (
                row["units"]
                == expected_values["units"]
            )

            assert (
                row["revenue"]
                == expected_values["revenue"]
            )

        print(
            "PASS: RetailMax and FreshMart "
            "category totals verified"
        )

        # 10. Independently reconcile totals
        # against the Sales fact.

        fact_totals = (
            fact.groupBy("source_system")
            .agg(
                F.count("*").alias("transactions"),
                F.sum("quantity").alias("units"),
                F.sum("total_amount").alias("revenue")
            )
        )

        category_totals = (
            actual.groupBy("source_system")
            .agg(
                F.sum("transaction_count").alias(
                    "transactions"
                ),
                F.sum("units_sold").alias("units"),
                F.sum("net_revenue").alias("revenue")
            )
        )

        assert (
            fact_totals.exceptAll(
                category_totals
            ).count()
        ) == 0

        assert (
            category_totals.exceptAll(
                fact_totals
            ).count()
        ) == 0

        print(
            "PASS: Category totals reconcile "
            "with fact_sales"
        )

        # 11. Validate financial equation

        invalid_financials = actual.filter(
            F.abs(
                F.col("gross_sales_amount")
                - F.col("discount_amount")
                - F.col("net_revenue")
            ) > F.lit(0.01)
        ).count()

        assert invalid_financials == 0

        print(
            "PASS: Gross sales minus discounts "
            "equals net revenue"
        )

        # 12. Validate averages

        invalid_averages = actual.filter(
            ~F.col(
                "average_transaction_value"
            ).eqNullSafe(
                (
                    F.col("net_revenue")
                    / F.col("transaction_count")
                ).cast("decimal(18,2)")
            )
        ).count()

        assert invalid_averages == 0

        print(
            "PASS: Average transaction "
            "values verified"
        )

        # 13. Validate distinct-count bounds

        invalid_distinct_counts = actual.filter(
            (F.col("distinct_products_sold") <= 0)
            | (F.col("distinct_customers") <= 0)
            | (F.col("distinct_stores") <= 0)
            | (
                F.col("distinct_products_sold")
                > F.col("transaction_count")
            )
            | (
                F.col("distinct_customers")
                > F.col("transaction_count")
            )
            | (
                F.col("distinct_stores")
                > F.col("transaction_count")
            )
        ).count()

        assert invalid_distinct_counts == 0

        print(
            "PASS: Distinct Product, Customer "
            "and Store counts valid"
        )

        # 14. Display Delta history

        print(
            "\n=== GOLD CATEGORY SALES "
            "DELTA HISTORY ==="
        )

        DeltaTable.forPath(
            spark, AGG_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: ALL GOLD CATEGORY "
            "SALES VALIDATIONS PASSED!"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
