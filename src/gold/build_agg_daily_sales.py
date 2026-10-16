
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

FACT_SALES_PATH = f"{GOLD_BASE}/fact_sales"
DAILY_SALES_PATH = f"{GOLD_BASE}/agg_daily_sales"

EXPECTED_TRANSACTIONS = 88600

EXPECTED_SOURCE_COUNTS = {
    "retailmax": 49000,
    "freshmart": 39600,
}

EXPECTED_REVENUE = {
    "retailmax": "21606073.80",
    "freshmart": "16896648.70",
}


def summarize_by_source(df):
    return {
        row["source_system"]: {
            "transactions": row["transactions"],
            "units": row["units"],
            "revenue": row["revenue"],
        }
        for row in (
            df.groupBy("source_system")
            .agg(
                F.sum("transaction_count").alias(
                    "transactions"
                ),
                F.sum("units_sold").alias("units"),
                F.sum("net_revenue").alias("revenue")
            )
            .collect()
        )
    }


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(
            "\n=== GOLD DAILY SALES "
            "AGGREGATION PIPELINE ==="
        )

        # 1. Read validated Gold Sales facts

        sales = (
            spark.read.format("delta")
            .load(FACT_SALES_PATH)
        )

        assert sales.count() == EXPECTED_TRANSACTIONS

        print(
            f"Gold Sales transactions: "
            f"{EXPECTED_TRANSACTIONS:,}"
        )

        # 2. Build daily aggregation.
        # Keep financial calculations in decimal
        # types to avoid floating-point errors.

        daily = (
            sales.groupBy(
                "date_key",
                "transaction_date",
                "source_system"
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
                )
            )
        )

        # 3. Validate grouping grain

        duplicate_groups = (
            daily.groupBy(
                "date_key",
                "source_system"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert duplicate_groups == 0

        invalid_groups = daily.filter(
            F.col("date_key").isNull()
            | F.col("transaction_date").isNull()
            | F.col("source_system").isNull()
            | (F.col("transaction_count") <= 0)
        ).count()

        assert invalid_groups == 0

        print(
            "PASS: Daily grouping grain "
            "is unique and valid"
        )

        # 4. Validate calendar keys

        incorrect_dates = daily.filter(
            ~F.col("date_key").eqNullSafe(
                F.date_format(
                    "transaction_date",
                    "yyyyMMdd"
                ).cast("int")
            )
        ).count()

        assert incorrect_dates == 0

        print("PASS: Daily calendar keys verified")

        # 5. Reconcile all source metrics

        totals = summarize_by_source(daily)

        assert set(totals) == set(
            EXPECTED_SOURCE_COUNTS
        )

        for source, expected_count in (
            EXPECTED_SOURCE_COUNTS.items()
        ):
            assert (
                totals[source]["transactions"]
                == expected_count
            ), (
                f"Transaction mismatch for {source}"
            )

            assert (
                str(totals[source]["revenue"])
                == EXPECTED_REVENUE[source]
            ), (
                f"Revenue mismatch for {source}: "
                f"{totals[source]['revenue']}"
            )

        print(
            "PASS: Source transaction counts "
            "and revenue reconciled"
        )

        # 6. Validate units sold against fact_sales

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

        for source in fact_units:
            assert (
                totals[source]["units"]
                == fact_units[source]
            )

        print("PASS: Units sold reconciled")

        # 7. Validate financial equation

        invalid_financials = daily.filter(
            F.abs(
                F.col("gross_sales_amount")
                - F.col("discount_amount")
                - F.col("net_revenue")
            ) > F.lit(0.01)
        ).count()

        assert invalid_financials == 0

        print(
            "PASS: Gross sales - discounts "
            "= net revenue"
        )

        # 8. Validate Date dimension references

        dim_date = (
            spark.read.format("delta")
            .load(f"{GOLD_BASE}/dim_date")
        )

        missing_dates = (
            daily.select("date_key")
            .join(
                dim_date.select("date_key"),
                on="date_key",
                how="left_anti"
            )
            .count()
        )

        assert missing_dates == 0

        print("PASS: All daily dates exist in dim_date")

        # 9. Write Gold aggregation

        print("\n=== WRITING AGG_DAILY_SALES ===")

        (
            daily.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(DAILY_SALES_PATH)
        )

        assert DeltaTable.isDeltaTable(
            spark,
            DAILY_SALES_PATH
        )

        persisted = (
            spark.read.format("delta")
            .load(DAILY_SALES_PATH)
        )

        row_count = persisted.count()

        print(
            f"PASS: Persisted {row_count:,} "
            "daily aggregation rows"
        )

        # 10. Show schema

        print("\n=== DAILY SALES SCHEMA ===")

        persisted.printSchema()

        # 11. Show overall reconciliation

        print("\n=== REVENUE BY SOURCE ===")

        persisted.groupBy(
            "source_system"
        ).agg(
            F.sum("transaction_count").alias(
                "transactions"
            ),
            F.sum("units_sold").alias("units"),
            F.sum("net_revenue").alias(
                "net_revenue"
            )
        ).orderBy(
            "source_system"
        ).show(truncate=False)

        # 12. Show recent daily performance

        print("\n=== LATEST 10 DAILY SALES ROWS ===")

        persisted.orderBy(
            F.col("transaction_date").desc(),
            F.col("source_system")
        ).show(10, truncate=False)

        # 13. Delta history

        print("\n=== DELTA HISTORY ===")

        DeltaTable.forPath(
            spark,
            DAILY_SALES_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: GOLD AGG_DAILY_SALES CREATED!"
        )

        print(
            f"Gold path: {DAILY_SALES_PATH}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
