
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
AGG_PATH = f"{GOLD_BASE}/agg_daily_sales"
DATE_PATH = f"{GOLD_BASE}/dim_date"

EXPECTED_FACT_COUNT = 88600
EXPECTED_AGG_COUNT = 2736

EXPECTED_SOURCES = {
    "retailmax": {
        "transactions": 49000,
        "units": 146797,
        "revenue": Decimal("21606073.80"),
    },
    "freshmart": {
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
            "\n=== INDEPENDENT GOLD DAILY "
            "SALES AGGREGATION VALIDATION ==="
        )

        # 1. Read persisted Delta tables

        fact = (
            spark.read.format("delta")
            .load(FACT_PATH)
        )

        daily = (
            spark.read.format("delta")
            .load(AGG_PATH)
        )

        dim_date = (
            spark.read.format("delta")
            .load(DATE_PATH)
        )

        assert DeltaTable.isDeltaTable(
            spark, AGG_PATH
        )

        print(
            "PASS: Daily Sales aggregation "
            "is a Delta table"
        )

        # 2. Validate row counts

        assert fact.count() == EXPECTED_FACT_COUNT

        assert daily.count() == EXPECTED_AGG_COUNT

        print(
            "PASS: Fact and aggregation "
            "record counts verified"
        )

        # 3. Validate exact schema

        expected_types = {
            "date_key": "int",
            "transaction_date": "date",
            "source_system": "string",
            "transaction_count": "bigint",
            "units_sold": "bigint",
            "gross_sales_amount": "decimal(18,2)",
            "discount_amount": "decimal(18,2)",
            "net_revenue": "decimal(18,2)",
            "average_transaction_value": "decimal(18,2)",
        }

        assert dict(daily.dtypes) == expected_types

        print("PASS: Daily Sales schema verified")

        # 4. Validate aggregation grain

        duplicate_groups = (
            daily.groupBy(
                "date_key",
                "source_system"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        invalid_groups = daily.filter(
            F.col("date_key").isNull()
            | F.col("transaction_date").isNull()
            | F.col("source_system").isNull()
            | (F.col("transaction_count") <= 0)
        ).count()

        assert duplicate_groups == 0
        assert invalid_groups == 0

        print(
            "PASS: One row per company "
            "per sales date"
        )

        # 5. Validate calendar keys

        incorrect_dates = daily.filter(
            ~F.col("date_key").eqNullSafe(
                F.date_format(
                    "transaction_date",
                    "yyyyMMdd"
                ).cast("int")
            )
        ).count()

        assert incorrect_dates == 0

        print("PASS: Date keys match calendar dates")

        # 6. Independently reconstruct aggregation

        expected = (
            fact.groupBy(
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
                (
                    F.col("net_revenue")
                    / F.col("transaction_count")
                ).cast("decimal(18,2)")
            )
        )

        # Compare full rows, not just totals.
        # exceptAll preserves duplicate semantics.

        columns = list(expected.columns)

        missing_or_changed = (
            expected.select(*columns)
            .exceptAll(
                daily.select(*columns)
            )
            .count()
        )

        unexpected_or_changed = (
            daily.select(*columns)
            .exceptAll(
                expected.select(*columns)
            )
            .count()
        )

        assert missing_or_changed == 0, (
            f"{missing_or_changed} expected "
            "rows missing or changed"
        )

        assert unexpected_or_changed == 0, (
            f"{unexpected_or_changed} unexpected "
            "or changed rows"
        )

        print(
            "PASS: Every daily aggregation row "
            "matches independently calculated metrics"
        )

        # 7. Validate source-level totals

        source_totals = (
            daily.groupBy("source_system")
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

        for row in source_totals:
            source = row["source_system"]

            assert source in EXPECTED_SOURCES

            expected_values = EXPECTED_SOURCES[source]

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
            "totals verified"
        )

        # 8. Independently reconcile with fact

        fact_totals = (
            fact.groupBy("source_system")
            .agg(
                F.count("*").alias("transactions"),
                F.sum("quantity").alias("units"),
                F.sum("total_amount").alias(
                    "revenue"
                )
            )
        )

        daily_totals = (
            daily.groupBy("source_system")
            .agg(
                F.sum("transaction_count").alias(
                    "transactions"
                ),
                F.sum("units_sold").alias("units"),
                F.sum("net_revenue").alias(
                    "revenue"
                )
            )
        )

        assert (
            fact_totals.exceptAll(
                daily_totals
            ).count()
        ) == 0

        assert (
            daily_totals.exceptAll(
                fact_totals
            ).count()
        ) == 0

        print(
            "PASS: Transaction, unit and revenue "
            "totals reconcile with fact_sales"
        )

        # 9. Validate financial equations

        incorrect_financials = daily.filter(
            F.abs(
                F.col("gross_sales_amount")
                - F.col("discount_amount")
                - F.col("net_revenue")
            ) > F.lit(0.01)
        ).count()

        assert incorrect_financials == 0

        print(
            "PASS: Gross sales minus discounts "
            "equals net revenue"
        )

        # 10. Validate average transaction value

        incorrect_averages = daily.filter(
            ~F.col(
                "average_transaction_value"
            ).eqNullSafe(
                (
                    F.col("net_revenue")
                    / F.col("transaction_count")
                ).cast("decimal(18,2)")
            )
        ).count()

        assert incorrect_averages == 0

        print(
            "PASS: Average transaction "
            "values verified"
        )

        # 11. Validate Date dimension references

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

        print(
            "PASS: All dates reference "
            "Gold dim_date"
        )

        # 12. Display Delta history

        print("\n=== GOLD DAILY SALES DELTA HISTORY ===")

        DeltaTable.forPath(
            spark,
            AGG_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: ALL GOLD DAILY SALES "
            "AGGREGATION VALIDATIONS PASSED!"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
