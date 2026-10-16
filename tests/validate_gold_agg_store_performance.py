
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
STORES_PATH = f"{GOLD_BASE}/dim_stores"
AGG_PATH = f"{GOLD_BASE}/agg_store_performance"

EXPECTED_FACT_COUNT = 88600
EXPECTED_STORE_COUNT = 90

EXPECTED_SOURCES = {
    "retailmax": {
        "stores": 50,
        "transactions": 49000,
        "units": 146797,
        "revenue": Decimal("21606073.80"),
    },
    "freshmart": {
        "stores": 40,
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
            "\n=== INDEPENDENT GOLD STORE "
            "PERFORMANCE VALIDATION ==="
        )

        # 1. Read persisted Gold Delta tables

        fact = (
            spark.read.format("delta")
            .load(FACT_PATH)
        )

        stores = (
            spark.read.format("delta")
            .load(STORES_PATH)
        )

        actual = (
            spark.read.format("delta")
            .load(AGG_PATH)
        )

        assert DeltaTable.isDeltaTable(
            spark, AGG_PATH
        )

        print(
            "PASS: Store Performance is a Delta table"
        )

        # 2. Verify record counts

        assert fact.count() == EXPECTED_FACT_COUNT
        assert stores.count() == EXPECTED_STORE_COUNT
        assert actual.count() == EXPECTED_STORE_COUNT

        print(
            "PASS: Fact, Store dimension and "
            "aggregation counts verified"
        )

        # 3. Validate exact schema

        expected_types = {
            "store_key": "string",
            "source_system": "string",
            "source_store_id": "string",
            "store_name": "string",
            "city": "string",
            "state": "string",
            "transaction_count": "bigint",
            "units_sold": "bigint",
            "gross_sales_amount": "decimal(18,2)",
            "discount_amount": "decimal(18,2)",
            "net_revenue": "decimal(18,2)",
            "distinct_customers": "bigint",
            "distinct_products_sold": "bigint",
            "average_transaction_value": "decimal(18,2)",
        }

        assert dict(actual.dtypes) == expected_types

        print("PASS: Store Performance schema verified")

        # 4. Validate one row per store

        null_keys = actual.filter(
            F.col("store_key").isNull()
            | (F.trim("store_key") == "")
        ).count()

        duplicate_keys = (
            actual.groupBy("store_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert null_keys == 0
        assert duplicate_keys == 0

        print("PASS: Store keys unique and non-null")

        # 5. Validate complete Store coverage

        missing_stores = (
            stores.select("store_key")
            .join(
                actual.select("store_key"),
                on="store_key",
                how="left_anti"
            )
            .count()
        )

        unexpected_stores = (
            actual.select("store_key")
            .join(
                stores.select("store_key"),
                on="store_key",
                how="left_anti"
            )
            .count()
        )

        assert missing_stores == 0
        assert unexpected_stores == 0

        print(
            "PASS: All 90 Gold Stores represented"
        )

        # 6. Verify Store attributes

        store_attributes = [
            "store_key",
            "source_system",
            "source_store_id",
            "store_name",
            "city",
            "state",
        ]

        expected_attributes = stores.select(
            *store_attributes
        )

        actual_attributes = actual.select(
            *store_attributes
        )

        assert (
            expected_attributes.exceptAll(
                actual_attributes
            ).count()
        ) == 0

        assert (
            actual_attributes.exceptAll(
                expected_attributes
            ).count()
        ) == 0

        print(
            "PASS: Store names and locations "
            "preserved exactly"
        )

        # 7. Independently calculate all
        # store-level Sales metrics.

        calculated = (
            fact.groupBy("store_key")
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

        # 8. Retain all stores, including
        # stores without transactions.

        expected = (
            stores.select(*store_attributes)
            .join(
                calculated,
                on="store_key",
                how="left"
            )
            .fillna(
                0,
                subset=[
                    "transaction_count",
                    "units_sold",
                    "distinct_customers",
                    "distinct_products_sold",
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

        # 9. Compare every reconstructed
        # store row against persisted Gold.

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
            "store rows missing or changed"
        )

        assert unexpected_or_changed == 0, (
            f"{unexpected_or_changed} unexpected "
            "store rows"
        )

        print(
            "PASS: Every store's metrics "
            "match independently calculated values"
        )

        # 10. Reconcile source-level totals

        source_totals = (
            actual.groupBy("source_system")
            .agg(
                F.count("*").alias("stores"),
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
                row["stores"]
                == expected_values["stores"]
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
            "store totals verified"
        )

        # 11. Validate financial equations

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

        # 12. Validate average transaction value

        invalid_averages = actual.filter(
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

        assert invalid_averages == 0

        print(
            "PASS: Average transaction "
            "values verified"
        )

        # 13. Validate non-negative metrics

        invalid_metrics = actual.filter(
            (F.col("transaction_count") < 0)
            | (F.col("units_sold") < 0)
            | (F.col("distinct_customers") < 0)
            | (F.col("distinct_products_sold") < 0)
            | (F.col("net_revenue") < 0)
        ).count()

        assert invalid_metrics == 0

        print("PASS: Store metrics are non-negative")

        # 14. Display Delta history

        print(
            "\n=== GOLD STORE PERFORMANCE "
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
            "\nSUCCESS: ALL GOLD STORE "
            "PERFORMANCE VALIDATIONS PASSED!"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
