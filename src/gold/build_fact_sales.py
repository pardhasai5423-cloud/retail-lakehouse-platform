
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

SILVER_SALES_PATH = (
    f"s3a://{BUCKET}/silver/sales"
)

GOLD_BASE = f"s3a://{BUCKET}/gold"

GOLD_SALES_PATH = (
    f"{GOLD_BASE}/fact_sales"
)

EXPECTED_SALES = 88600


def validate_dimension_references(
    facts,
    dimension,
    key,
    dimension_name
):
    """
    Validate all fact foreign keys against
    a dimension with unique, non-null keys.
    """

    dimension_keys = dimension.select(key)

    null_keys = dimension_keys.filter(
        F.col(key).isNull()
    ).count()

    duplicates = (
        dimension_keys.groupBy(key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    assert null_keys == 0, (
        f"{dimension_name} contains null keys"
    )

    assert duplicates == 0, (
        f"{dimension_name} contains duplicate keys"
    )

    missing = (
        facts.join(
            dimension_keys,
            on=key,
            how="left_anti"
        ).count()
    )

    assert missing == 0, (
        f"{missing} facts have invalid "
        f"{dimension_name} references"
    )

    print(
        f"PASS: All Sales reference "
        f"valid {dimension_name}"
    )


def build_sales_fact(silver_sales):
    """
    Build one Gold fact row per trusted
    Silver Sales transaction.
    """

    return (
        silver_sales
        .withColumn(
            "date_key",
            F.date_format(
                "transaction_date",
                "yyyyMMdd"
            ).cast("int")
        )
        .select(
            "transaction_key",
            "source_system",
            "source_transaction_id",
            "customer_key",
            "product_key",
            "store_key",
            "date_key",
            "transaction_date",
            "quantity",
            "unit_price",
            "discount_amount",
            "total_amount",
            "ingestion_timestamp",
            "batch_id",
            "source_file"
        )
    )


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(
            "\n=== GOLD SALES FACT PIPELINE ==="
        )

        # 1. Read trusted Silver Sales

        silver_sales = (
            spark.read.format("delta")
            .load(SILVER_SALES_PATH)
        )

        silver_count = silver_sales.count()

        assert silver_count == EXPECTED_SALES, (
            f"Expected {EXPECTED_SALES} Sales, "
            f"found {silver_count}"
        )

        print(
            f"Trusted Silver Sales: "
            f"{silver_count:,}"
        )

        # 2. Build fact table

        fact_sales = build_sales_fact(
            silver_sales
        )

        fact_count = fact_sales.count()

        assert fact_count == EXPECTED_SALES

        print(
            f"Gold Sales Facts: {fact_count:,}"
        )

        # 3. Validate transaction keys

        invalid_keys = fact_sales.filter(
            F.col("transaction_key").isNull()
            | (F.trim("transaction_key") == "")
        ).count()

        duplicate_keys = (
            fact_sales.groupBy("transaction_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert invalid_keys == 0
        assert duplicate_keys == 0

        expected_key = F.concat(
            F.col("source_system"),
            F.lit(":"),
            F.col("source_transaction_id")
        )

        incorrectly_qualified = fact_sales.filter(
            ~F.col("transaction_key").eqNullSafe(
                expected_key
            )
        ).count()

        assert incorrectly_qualified == 0

        print(
            "PASS: Transaction keys unique, "
            "non-null and source-qualified"
        )

        # 4. Validate source counts

        print("\n=== SALES BY SOURCE ===")

        fact_sales.groupBy(
            "source_system"
        ).count().show()

        source_counts = {
            row["source_system"]: row["count"]
            for row in (
                fact_sales.groupBy("source_system")
                .count()
                .collect()
            )
        }

        assert source_counts == {
            "retailmax": 49000,
            "freshmart": 39600,
        }

        print("PASS: Source counts verified")

        # 5. Validate dates and date keys

        invalid_dates = fact_sales.filter(
            F.col("transaction_date").isNull()
            | F.col("date_key").isNull()
        ).count()

        incorrect_date_keys = fact_sales.filter(
            ~F.col("date_key").eqNullSafe(
                F.date_format(
                    "transaction_date",
                    "yyyyMMdd"
                ).cast("int")
            )
        ).count()

        assert invalid_dates == 0
        assert incorrect_date_keys == 0

        print("PASS: Sales dates and date keys verified")

        # 6. Validate measures

        invalid_measures = fact_sales.filter(
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

        print("PASS: Sales measures are valid")

        # 7. Validate financial calculation

        expected_total = (
            F.col("quantity") * F.col("unit_price")
            - F.col("discount_amount")
        )

        incorrect_totals = fact_sales.filter(
            F.abs(
                F.col("total_amount") - expected_total
            ) > F.lit(0.01)
        ).count()

        assert incorrect_totals == 0

        print(
            "PASS: Sales financial calculations verified"
        )

        # 8. Read Gold dimensions

        dim_customers = (
            spark.read.format("delta")
            .load(f"{GOLD_BASE}/dim_customers")
        )

        dim_products = (
            spark.read.format("delta")
            .load(f"{GOLD_BASE}/dim_products")
        )

        dim_stores = (
            spark.read.format("delta")
            .load(f"{GOLD_BASE}/dim_stores")
        )

        dim_date = (
            spark.read.format("delta")
            .load(f"{GOLD_BASE}/dim_date")
        )

        # 9. Validate dimension relationships

        validate_dimension_references(
            fact_sales,
            dim_customers,
            "customer_key",
            "Gold Customers"
        )

        validate_dimension_references(
            fact_sales,
            dim_products,
            "product_key",
            "Gold Products"
        )

        validate_dimension_references(
            fact_sales,
            dim_stores,
            "store_key",
            "Gold Stores"
        )

        validate_dimension_references(
            fact_sales,
            dim_date,
            "date_key",
            "Gold Date"
        )

        # 10. Validate lineage

        missing_lineage = fact_sales.filter(
            F.col("ingestion_timestamp").isNull()
            | F.col("batch_id").isNull()
            | (F.trim("batch_id") == "")
            | F.col("source_file").isNull()
            | (F.trim("source_file") == "")
        ).count()

        assert missing_lineage == 0

        print("PASS: Sales lineage preserved")

        # 11. Write Gold Sales fact

        print("\n=== WRITING GOLD FACT_SALES ===")

        (
            fact_sales.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(GOLD_SALES_PATH)
        )

        print("PASS: Gold Sales fact written")

        # 12. Verify persisted table

        assert DeltaTable.isDeltaTable(
            spark,
            GOLD_SALES_PATH
        )

        persisted = (
            spark.read.format("delta")
            .load(GOLD_SALES_PATH)
        )

        assert persisted.count() == EXPECTED_SALES

        print(
            f"PASS: Persisted count = "
            f"{EXPECTED_SALES:,}"
        )

        # 13. Show schema

        print("\n=== GOLD SALES FACT SCHEMA ===")

        persisted.printSchema()

        # 14. Show revenue by source

        print("\n=== REVENUE BY SOURCE ===")

        persisted.groupBy(
            "source_system"
        ).agg(
            F.count("*").alias("transactions"),
            F.sum("quantity").alias("units_sold"),
            F.round(
                F.sum("total_amount"), 2
            ).alias("total_revenue")
        ).orderBy(
            "source_system"
        ).show(truncate=False)

        # 15. Show Delta history

        print("\n=== DELTA HISTORY ===")

        DeltaTable.forPath(
            spark,
            GOLD_SALES_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: GOLD FACT_SALES CREATED!"
        )

        print(
            f"Gold path: {GOLD_SALES_PATH}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
