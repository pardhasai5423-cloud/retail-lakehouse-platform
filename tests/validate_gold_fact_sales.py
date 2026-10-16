
import os
import sys

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

SILVER_SALES_PATH = f"s3a://{BUCKET}/silver/sales"
GOLD_BASE = f"s3a://{BUCKET}/gold"
GOLD_SALES_PATH = f"{GOLD_BASE}/fact_sales"

EXPECTED_COUNT = 88600

EXPECTED_SOURCES = {
    "retailmax": 49000,
    "freshmart": 39600,
}

EXPECTED_REVENUE = {
    "retailmax": "21606073.80",
    "freshmart": "16896648.70",
}


def source_counts(df):
    return {
        row["source_system"]: row["count"]
        for row in (
            df.groupBy("source_system")
            .count()
            .collect()
        )
    }


def validate_foreign_key(
    fact,
    dimension,
    key,
    dimension_name
):
    """
    Verify that every fact key exists in its
    corresponding dimension and that the
    source system is consistent.
    """

    null_fact_keys = fact.filter(
        F.col(key).isNull()
    ).count()

    assert null_fact_keys == 0, (
        f"Null {key} values in Sales"
    )

    duplicate_dimension_keys = (
        dimension.groupBy(key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    assert duplicate_dimension_keys == 0, (
        f"Duplicate {key} in {dimension_name}"
    )

    missing = (
        fact.select(key)
        .join(
            dimension.select(key),
            on=key,
            how="left_anti"
        )
        .count()
    )

    assert missing == 0, (
        f"{missing} Sales rows have invalid "
        f"{dimension_name} references"
    )

    if "source_system" in dimension.columns:
        mismatches = (
            fact.alias("f")
            .join(
                dimension.alias("d"),
                on=key,
                how="inner"
            )
            .filter(
                ~F.col("f.source_system").eqNullSafe(
                    F.col("d.source_system")
                )
            )
            .count()
        )

        assert mismatches == 0, (
            f"Source mismatch in {dimension_name}"
        )

    print(
        f"PASS: {dimension_name} foreign keys "
        f"and source consistency"
    )


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(
            "\n=== INDEPENDENT GOLD SALES "
            "FACT VALIDATION ==="
        )

        # 1. Read persisted Silver and Gold
        silver = (
            spark.read.format("delta")
            .load(SILVER_SALES_PATH)
        )

        gold = (
            spark.read.format("delta")
            .load(GOLD_SALES_PATH)
        )

        # 2. Validate counts
        assert silver.count() == EXPECTED_COUNT
        assert gold.count() == EXPECTED_COUNT

        print(
            "PASS: Silver and Gold each contain "
            "88,600 transactions"
        )

        # 3. Validate source counts
        assert source_counts(silver) == EXPECTED_SOURCES
        assert source_counts(gold) == EXPECTED_SOURCES

        print("PASS: Source-level counts verified")

        # 4. Validate schema
        expected_types = {
            "transaction_key": "string",
            "source_system": "string",
            "source_transaction_id": "string",
            "customer_key": "string",
            "product_key": "string",
            "store_key": "string",
            "date_key": "int",
            "transaction_date": "date",
            "quantity": "int",
            "unit_price": "decimal(12,2)",
            "discount_amount": "decimal(12,2)",
            "total_amount": "decimal(14,2)",
            "ingestion_timestamp": "timestamp",
            "batch_id": "string",
            "source_file": "string",
        }

        assert dict(gold.dtypes) == expected_types

        print("PASS: Gold Sales fact schema verified")

        # 5. Validate transaction keys
        invalid_keys = gold.filter(
            F.col("transaction_key").isNull()
            | (F.trim("transaction_key") == "")
        ).count()

        duplicate_keys = (
            gold.groupBy("transaction_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        expected_key = F.concat(
            F.col("source_system"),
            F.lit(":"),
            F.col("source_transaction_id")
        )

        invalid_qualification = gold.filter(
            ~F.col("transaction_key").eqNullSafe(
                expected_key
            )
        ).count()

        assert invalid_keys == 0
        assert duplicate_keys == 0
        assert invalid_qualification == 0

        print(
            "PASS: Transaction keys unique "
            "and source-qualified"
        )

        # 6. Verify complete Silver-to-Gold coverage
        missing_gold = (
            silver.select("transaction_key")
            .join(
                gold.select("transaction_key"),
                on="transaction_key",
                how="left_anti"
            )
            .count()
        )

        unexpected_gold = (
            gold.select("transaction_key")
            .join(
                silver.select("transaction_key"),
                on="transaction_key",
                how="left_anti"
            )
            .count()
        )

        assert missing_gold == 0
        assert unexpected_gold == 0

        print(
            "PASS: Complete Silver-to-Gold "
            "transaction coverage"
        )

        # 7. Verify exact attribute preservation
        shared_columns = [
            "transaction_key",
            "source_system",
            "source_transaction_id",
            "customer_key",
            "product_key",
            "store_key",
            "transaction_date",
            "quantity",
            "unit_price",
            "discount_amount",
            "total_amount",
            "ingestion_timestamp",
            "batch_id",
            "source_file",
        ]

        silver_shared = silver.select(*shared_columns)
        gold_shared = gold.select(*shared_columns)

        missing_or_changed = (
            silver_shared.exceptAll(gold_shared)
            .count()
        )

        unexpected_or_changed = (
            gold_shared.exceptAll(silver_shared)
            .count()
        )

        assert missing_or_changed == 0
        assert unexpected_or_changed == 0

        print(
            "PASS: All Silver transaction "
            "attributes preserved exactly"
        )

        # 8. Validate dates and date keys
        invalid_dates = gold.filter(
            F.col("transaction_date").isNull()
            | F.col("date_key").isNull()
        ).count()

        incorrect_date_keys = gold.filter(
            ~F.col("date_key").eqNullSafe(
                F.date_format(
                    "transaction_date",
                    "yyyyMMdd"
                ).cast("int")
            )
        ).count()

        assert invalid_dates == 0
        assert incorrect_date_keys == 0

        print("PASS: Transaction dates and keys verified")

        # 9. Validate financial measures
        invalid_measures = gold.filter(
            F.col("quantity").isNull()
            | (F.col("quantity") <= 0)
            | F.col("unit_price").isNull()
            | (F.col("unit_price") < 0)
            | F.col("discount_amount").isNull()
            | (F.col("discount_amount") < 0)
            | F.col("total_amount").isNull()
            | (F.col("total_amount") < 0)
        ).count()

        assert invalid_measures == 0

        expected_total = (
            F.col("quantity") * F.col("unit_price")
            - F.col("discount_amount")
        )

        incorrect_totals = gold.filter(
            F.abs(
                F.col("total_amount") - expected_total
            ) > F.lit(0.01)
        ).count()

        assert incorrect_totals == 0

        print(
            "PASS: Financial measures and "
            "calculations verified"
        )

        # 10. Validate all dimension relationships
        dimensions = [
            (
                "customer_key",
                "dim_customers",
                "Gold Customers",
            ),
            (
                "product_key",
                "dim_products",
                "Gold Products",
            ),
            (
                "store_key",
                "dim_stores",
                "Gold Stores",
            ),
            (
                "date_key",
                "dim_date",
                "Gold Date",
            ),
        ]

        for key, table, label in dimensions:
            dimension = (
                spark.read.format("delta")
                .load(f"{GOLD_BASE}/{table}")
            )

            validate_foreign_key(
                gold,
                dimension,
                key,
                label
            )

        # 11. Validate source revenue totals
        revenue_rows = (
            gold.groupBy("source_system")
            .agg(
                F.sum("total_amount").alias("revenue")
            )
            .collect()
        )

        actual_revenue = {
            row["source_system"]: str(row["revenue"])
            for row in revenue_rows
        }

        assert actual_revenue == EXPECTED_REVENUE, (
            f"Unexpected revenue: {actual_revenue}"
        )

        print("PASS: Revenue totals match expected values")

        # 12. Independently reconcile Silver revenue
        silver_revenue = {
            row["source_system"]: row["revenue"]
            for row in (
                silver.groupBy("source_system")
                .agg(
                    F.sum("total_amount").alias("revenue")
                )
                .collect()
            )
        }

        gold_revenue = {
            row["source_system"]: row["revenue"]
            for row in revenue_rows
        }

        assert silver_revenue == gold_revenue

        print(
            "PASS: Silver-to-Gold revenue "
            "reconciliation"
        )

        # 13. Validate lineage
        missing_lineage = gold.filter(
            F.col("ingestion_timestamp").isNull()
            | F.col("batch_id").isNull()
            | (F.trim("batch_id") == "")
            | F.col("source_file").isNull()
            | (F.trim("source_file") == "")
        ).count()

        assert missing_lineage == 0

        print("PASS: Sales lineage preserved")

        # 14. Validate Delta metadata
        assert DeltaTable.isDeltaTable(
            spark,
            GOLD_SALES_PATH
        )

        print("PASS: Gold Sales is a Delta table")

        print("\n=== GOLD SALES DELTA HISTORY ===")

        DeltaTable.forPath(
            spark,
            GOLD_SALES_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: ALL GOLD SALES "
            "FACT VALIDATIONS PASSED!"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
