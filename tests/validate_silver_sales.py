
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

SILVER_BASE = f"s3a://{BUCKET}/silver"

SALES_PATH = f"{SILVER_BASE}/sales"

QUARANTINE_PATH = (
    f"s3a://{BUCKET}/quarantine/sales"
)


def source_counts(df):
    return {
        row["source_system"]: row["count"]
        for row in df.groupBy(
            "source_system"
        ).count().collect()
    }


def assert_unique_keys(df, label):
    duplicate_count = (
        df.groupBy("transaction_key")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    assert duplicate_count == 0, (
        f"{label} contains duplicate transaction keys"
    )

    print(f"PASS: {label} transaction keys unique")


def assert_references(
    sales,
    reference,
    key_column,
    label,
):
    reference_keys = (
        reference.select(key_column).distinct()
    )

    unmatched = sales.join(
        reference_keys,
        on=key_column,
        how="left_anti"
    ).count()

    assert unmatched == 0, (
        f"{label}: {unmatched} unmatched references"
    )

    print(f"PASS: {label} references valid")


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    sales = None
    quarantine = None

    try:
        print(
            "\n=== INDEPENDENT SILVER SALES "
            "VALIDATION ==="
        )

        # ----------------------------------
        # 1. Read persisted Delta tables
        # ----------------------------------

        sales = (
            spark.read.format("delta")
            .load(SALES_PATH)
            .cache()
        )

        quarantine = (
            spark.read.format("delta")
            .load(QUARANTINE_PATH)
            .cache()
        )

        sales_count = sales.count()
        quarantine_count = quarantine.count()

        # ----------------------------------
        # 2. Validate record counts
        # ----------------------------------

        assert sales_count == 88600
        assert quarantine_count == 1400

        assert sales_count + quarantine_count == 90000

        print("PASS: Silver Sales = 88,600")
        print("PASS: Quarantine = 1,400")
        print("PASS: Total reconciliation = 90,000")

        # ----------------------------------
        # 3. Validate source counts
        # ----------------------------------

        assert source_counts(sales) == {
            "retailmax": 49000,
            "freshmart": 39600,
        }

        assert source_counts(quarantine) == {
            "retailmax": 1000,
            "freshmart": 400,
        }

        print("PASS: Source-level counts verified")

        # ----------------------------------
        # 4. Validate expected schema
        # ----------------------------------

        expected_types = {
            "source_system": "string",
            "source_transaction_id": "string",
            "transaction_key": "string",
            "source_customer_id": "string",
            "customer_key": "string",
            "source_product_id": "string",
            "product_key": "string",
            "source_store_id": "string",
            "store_key": "string",
            "transaction_date": "date",
            "quantity": "int",
            "unit_price": "decimal(12,2)",
            "discount_amount": "decimal(12,2)",
            "total_amount": "decimal(14,2)",
            "ingestion_timestamp": "timestamp",
            "batch_id": "string",
            "source_file": "string",
            "customer_reference_status": "string",
            "product_reference_status": "string",
            "store_reference_status": "string",
            "validation_status": "string",
        }

        for df, label in [
            (sales, "Silver Sales"),
            (quarantine, "Quarantine"),
        ]:
            actual_types = dict(df.dtypes)

            for column, expected_type in expected_types.items():
                assert actual_types.get(column) == expected_type, (
                    f"{label}: {column} expected "
                    f"{expected_type}, found "
                    f"{actual_types.get(column)}"
                )

            assert "quality_issues" in actual_types

            assert actual_types[
                "quality_issues"
            ].startswith("array<")

        print("PASS: Both table schemas verified")

        # ----------------------------------
        # 5. Verify key uniqueness
        # ----------------------------------

        assert_unique_keys(sales, "Silver Sales")
        assert_unique_keys(quarantine, "Quarantine")

        # ----------------------------------
        # 6. Ensure no transaction overlap
        # ----------------------------------

        overlap = (
            sales.select("transaction_key")
            .join(
                quarantine.select("transaction_key"),
                on="transaction_key",
                how="inner"
            )
            .count()
        )

        assert overlap == 0

        print(
            "PASS: No transactions shared between "
            "Silver and Quarantine"
        )

        # ----------------------------------
        # 7. Validate composite keys
        # ----------------------------------

        key_pairs = [
            ("source_transaction_id", "transaction_key"),
            ("source_customer_id", "customer_key"),
            ("source_product_id", "product_key"),
            ("source_store_id", "store_key"),
        ]

        for source_col, key_col in key_pairs:
            expected = F.concat(
                F.col("source_system"),
                F.lit(":"),
                F.col(source_col)
            )

            invalid = sales.filter(
                ~F.col(key_col).eqNullSafe(expected)
            ).count()

            assert invalid == 0, (
                f"Invalid composite key: {key_col}"
            )

        print("PASS: Composite keys verified")

        # ----------------------------------
        # 8. Verify validation statuses
        # ----------------------------------

        invalid_silver = sales.filter(
            F.col("validation_status").isNull()
            | (F.col("validation_status") != "VALID")
            | F.col("quality_issues").isNull()
            | (F.size("quality_issues") != 0)
        ).count()

        invalid_quarantine = quarantine.filter(
            F.col("validation_status").isNull()
            | (F.col("validation_status") != "QUARANTINE")
            | F.col("quality_issues").isNull()
            | (F.size("quality_issues") == 0)
        ).count()

        assert invalid_silver == 0
        assert invalid_quarantine == 0

        print("PASS: Validation statuses verified")

        # ----------------------------------
        # 9. Validate Sales business rules
        # ----------------------------------

        invalid_business_values = sales.filter(
            F.col("transaction_date").isNull()
            | (F.col("transaction_date") > F.current_date())
            | F.col("quantity").isNull()
            | (F.col("quantity") <= 0)
            | F.col("unit_price").isNull()
            | (F.col("unit_price") <= 0)
            | F.col("discount_amount").isNull()
            | (F.col("discount_amount") < 0)
            | F.col("total_amount").isNull()
            | (F.col("total_amount") < 0)
        ).count()

        assert invalid_business_values == 0

        print("PASS: Sales business values valid")

        # ----------------------------------
        # 10. Verify financial calculations
        # ----------------------------------

        gross = (
            F.col("quantity").cast("decimal(18,2)")
            * F.col("unit_price")
        )

        expected_total = gross - F.col("discount_amount")

        invalid_financials = sales.filter(
            (F.col("discount_amount") > gross)
            | (
                F.abs(
                    F.col("total_amount") - expected_total
                ) > F.lit(0.01)
            )
        ).count()

        assert invalid_financials == 0

        print("PASS: Financial calculations verified")

        # ----------------------------------
        # 11. Verify lineage
        # ----------------------------------

        for df, label in [
            (sales, "Silver Sales"),
            (quarantine, "Quarantine"),
        ]:
            missing_lineage = df.filter(
                F.col("ingestion_timestamp").isNull()
                | F.col("batch_id").isNull()
                | (F.trim(F.col("batch_id")) == "")
                | F.col("source_file").isNull()
                | (F.trim(F.col("source_file")) == "")
            ).count()

            assert missing_lineage == 0

            print(f"PASS: {label} lineage preserved")

        # ----------------------------------
        # 12. Verify referential integrity
        # ----------------------------------

        print(
            "\n=== SALES REFERENTIAL INTEGRITY ==="
        )

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

        assert_references(
            sales,
            customers,
            "customer_key",
            "Customer"
        )

        assert_references(
            sales,
            products,
            "product_key",
            "Product"
        )

        assert_references(
            sales,
            stores,
            "store_key",
            "Store"
        )

        # ----------------------------------
        # 13. Verify quarantine issue counts
        # ----------------------------------

        print(
            "\n=== QUARANTINE QUALITY ISSUES ==="
        )

        issue_rows = (
            quarantine
            .select(
                "source_system",
                F.explode("quality_issues").alias("issue")
            )
            .groupBy("source_system", "issue")
            .count()
            .collect()
        )

        actual_issues = {
            (row["source_system"], row["issue"]):
                row["count"]
            for row in issue_rows
        }

        expected_issues = {
            ("retailmax", "NEGATIVE_QUANTITY"): 250,
            ("retailmax", "TOTAL_AMOUNT_MISMATCH"): 250,
            ("retailmax", "INVALID_PRODUCT_REFERENCE"): 250,
            ("retailmax", "MISSING_TRANSACTION_DATE"): 500,
            ("freshmart", "NEGATIVE_QUANTITY"): 200,
            ("freshmart", "TOTAL_AMOUNT_MISMATCH"): 200,
            ("freshmart", "INVALID_PRODUCT_REFERENCE"): 200,
        }

        assert actual_issues == expected_issues, (
            f"Unexpected quarantine issues: "
            f"{actual_issues}"
        )

        print("PASS: Quarantine issue counts verified")

        for (source, issue), count in sorted(
            actual_issues.items()
        ):
            print(f"{source}: {issue} = {count}")

        # ----------------------------------
        # 14. Validate Delta Lake metadata
        # ----------------------------------

        for path, label in [
            (SALES_PATH, "Silver Sales"),
            (QUARANTINE_PATH, "Sales Quarantine"),
        ]:
            assert DeltaTable.isDeltaTable(
                spark,
                path
            )

            print(f"\n=== {label.upper()} HISTORY ===")

            DeltaTable.forPath(
                spark,
                path
            ).history(5).select(
                "version",
                "timestamp",
                "operation"
            ).show(truncate=False)

            print(f"PASS: {label} is a valid Delta table")

        print(
            "\nSUCCESS: ALL SILVER SALES AND "
            "QUARANTINE VALIDATION CHECKS PASSED!"
        )

    finally:
        if sales is not None:
            sales.unpersist()

        if quarantine is not None:
            quarantine.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
