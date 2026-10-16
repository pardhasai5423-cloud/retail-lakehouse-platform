
import os
import sys

from pyspark.sql import functions as F
from delta.tables import DeltaTable


PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)

sys.path.insert(
    0,
    os.path.join(PROJECT_ROOT, "src", "silver")
)

from transform_customers import create_spark


BUCKET = "retail-lakehouse-parsam-2026"
GOLD_BASE = f"s3a://{BUCKET}/gold"

FACT_PATH = f"{GOLD_BASE}/fact_sales"
STORES_PATH = f"{GOLD_BASE}/dim_stores"
OUTPUT_PATH = f"{GOLD_BASE}/agg_store_performance"

EXPECTED_SALES = 88600
EXPECTED_STORES = 90

EXPECTED_SOURCE_COUNTS = {
    "retailmax": 49000,
    "freshmart": 39600,
}

EXPECTED_REVENUE = {
    "retailmax": "21606073.80",
    "freshmart": "16896648.70",
}


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(
            "\n=== GOLD STORE PERFORMANCE "
            "AGGREGATION PIPELINE ==="
        )

        # 1. Read validated Gold tables

        sales = (
            spark.read.format("delta")
            .load(FACT_PATH)
        )

        stores = (
            spark.read.format("delta")
            .load(STORES_PATH)
        )

        assert sales.count() == EXPECTED_SALES
        assert stores.count() == EXPECTED_STORES

        print(
            f"Gold Sales transactions: "
            f"{EXPECTED_SALES:,}"
        )

        print(
            f"Gold Stores: {EXPECTED_STORES}"
        )

        # 2. Verify dimension store keys

        invalid_store_keys = stores.filter(
            F.col("store_key").isNull()
            | (F.trim("store_key") == "")
        ).count()

        duplicate_store_keys = (
            stores.groupBy("store_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert invalid_store_keys == 0
        assert duplicate_store_keys == 0

        print("PASS: Store dimension keys unique")

        # 3. Verify every sale references a store

        missing_store_references = (
            sales.select("store_key")
            .join(
                stores.select("store_key"),
                on="store_key",
                how="left_anti"
            )
            .count()
        )

        assert missing_store_references == 0

        print(
            "PASS: Every Sale references "
            "a valid Gold Store"
        )

        # 4. Aggregate Sales at store grain

        store_sales = (
            sales.groupBy("store_key")
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
                    "customer_key"
                ).alias(
                    "distinct_customers"
                ),
                F.countDistinct(
                    "product_key"
                ).alias(
                    "distinct_products_sold"
                )
            )
        )

        # 5. Join to dimension to retain all stores

        performance = (
            stores.select(
                "store_key",
                "source_system",
                "source_store_id",
                "store_name",
                "city",
                "state"
            )
            .join(
                store_sales,
                on="store_key",
                how="left"
            )
            .fillna(
                0,
                subset=[
                    "transaction_count",
                    "units_sold",
                    "distinct_customers",
                    "distinct_products_sold"
                ]
            )
            .withColumn(
                "gross_sales_amount",
                F.coalesce(
                    F.col("gross_sales_amount"),
                    F.lit(0).cast("decimal(18,2)")
                )
            )
            .withColumn(
                "discount_amount",
                F.coalesce(
                    F.col("discount_amount"),
                    F.lit(0).cast("decimal(18,2)")
                )
            )
            .withColumn(
                "net_revenue",
                F.coalesce(
                    F.col("net_revenue"),
                    F.lit(0).cast("decimal(18,2)")
                )
            )
            .withColumn(
                "average_transaction_value",
                F.when(
                    F.col("transaction_count") > 0,
                    (
                        F.col("net_revenue")
                        / F.col("transaction_count")
                    ).cast("decimal(18,2)")
                ).otherwise(
                    F.lit(0).cast("decimal(18,2)")
                )
            )
        )

        # 6. Validate output grain and coverage

        assert performance.count() == EXPECTED_STORES

        duplicate_groups = (
            performance.groupBy("store_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert duplicate_groups == 0

        print(
            "PASS: One row per store, "
            "including zero-sales stores"
        )

        # 7. Reconcile source-level metrics

        source_totals = (
            performance.groupBy("source_system")
            .agg(
                F.sum("transaction_count").alias(
                    "transactions"
                ),
                F.sum("units_sold").alias(
                    "units"
                ),
                F.sum("net_revenue").alias(
                    "revenue"
                )
            )
            .collect()
        )

        assert len(source_totals) == 2

        fact_units = {
            row["source_system"]: row["units"]
            for row in (
                sales.groupBy("source_system")
                .agg(
                    F.sum("quantity").alias("units")
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
                str(row["revenue"])
                == EXPECTED_REVENUE[source]
            )

        print(
            "PASS: Store transaction, unit and "
            "revenue totals reconcile"
        )

        # 8. Validate financial equation

        incorrect_financials = performance.filter(
            F.abs(
                F.col("gross_sales_amount")
                - F.col("discount_amount")
                - F.col("net_revenue")
            ) > F.lit(0.01)
        ).count()

        assert incorrect_financials == 0

        print(
            "PASS: Store financial "
            "calculations verified"
        )

        # 9. Validate average transaction value

        incorrect_averages = performance.filter(
            ~F.col(
                "average_transaction_value"
            ).eqNullSafe(
                F.when(
                    F.col("transaction_count") > 0,
                    (
                        F.col("net_revenue")
                        / F.col("transaction_count")
                    ).cast("decimal(18,2)")
                ).otherwise(
                    F.lit(0).cast("decimal(18,2)")
                )
            )
        ).count()

        assert incorrect_averages == 0

        print(
            "PASS: Average transaction "
            "values verified"
        )

        # 10. Write Gold aggregation

        print(
            "\n=== WRITING AGG_STORE_PERFORMANCE ==="
        )

        (
            performance.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(OUTPUT_PATH)
        )

        assert DeltaTable.isDeltaTable(
            spark, OUTPUT_PATH
        )

        persisted = (
            spark.read.format("delta")
            .load(OUTPUT_PATH)
        )

        assert persisted.count() == EXPECTED_STORES

        print(
            "PASS: Persisted 90 store "
            "performance rows"
        )

        # 11. Display schema

        print(
            "\n=== STORE PERFORMANCE SCHEMA ==="
        )

        persisted.printSchema()

        # 12. Show top 10 stores

        print(
            "\n=== TOP 10 STORES BY REVENUE ==="
        )

        persisted.select(
            "source_system",
            "store_name",
            "city",
            "state",
            "transaction_count",
            "units_sold",
            "net_revenue",
            "average_transaction_value"
        ).orderBy(
            F.col("net_revenue").desc()
        ).show(10, truncate=False)

        # 13. Show source-level totals

        print(
            "\n=== STORE PERFORMANCE BY SOURCE ==="
        )

        persisted.groupBy(
            "source_system"
        ).agg(
            F.count("*").alias("stores"),
            F.sum("transaction_count").alias(
                "transactions"
            ),
            F.sum("units_sold").alias(
                "units_sold"
            ),
            F.sum("net_revenue").alias(
                "net_revenue"
            )
        ).orderBy(
            "source_system"
        ).show(truncate=False)

        # 14. Delta history

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
            "AGG_STORE_PERFORMANCE CREATED!"
        )

        print(
            f"Gold path: {OUTPUT_PATH}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
