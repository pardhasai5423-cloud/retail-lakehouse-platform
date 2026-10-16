
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

SILVER_PATH = f"s3a://{BUCKET}/silver/customers"
GOLD_PATH = f"s3a://{BUCKET}/gold/dim_customers"


def count_by_source(df):
    return {
        row["source_system"]: row["count"]
        for row in (
            df.groupBy("source_system")
            .count()
            .collect()
        )
    }


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(
            "\n=== INDEPENDENT GOLD "
            "CUSTOMER DIMENSION VALIDATION ==="
        )

        # -----------------------------------
        # 1. Read Silver and Gold Delta tables
        # -----------------------------------

        silver = (
            spark.read.format("delta")
            .load(SILVER_PATH)
        )

        gold = (
            spark.read.format("delta")
            .load(GOLD_PATH)
        )

        silver_count = silver.count()
        gold_count = gold.count()

        assert silver_count == 9000
        assert gold_count == 9000

        print("PASS: Silver Customers = 9,000")
        print("PASS: Gold Customers = 9,000")

        # -----------------------------------
        # 2. Validate source counts
        # -----------------------------------

        expected_counts = {
            "retailmax": 5000,
            "freshmart": 4000,
        }

        assert count_by_source(silver) == expected_counts
        assert count_by_source(gold) == expected_counts

        print("PASS: Source-level counts verified")

        # -----------------------------------
        # 3. Validate Gold schema
        # -----------------------------------

        expected_types = {
            "customer_key": "string",
            "source_system": "string",
            "source_customer_id": "string",
            "first_name": "string",
            "last_name": "string",
            "customer_name": "string",
            "email": "string",
            "phone": "string",
            "city": "string",
            "state": "string",
            "registration_date": "date",
            "registration_year": "int",
            "registration_month": "int",
            "ingestion_timestamp": "timestamp",
        }

        actual_types = dict(gold.dtypes)

        for column, expected_type in expected_types.items():
            assert actual_types.get(column) == expected_type, (
                f"Incorrect type for {column}: "
                f"{actual_types.get(column)}"
            )

        print("PASS: Gold customer schema verified")

        # -----------------------------------
        # 4. Validate unique customer keys
        # -----------------------------------

        null_keys = gold.filter(
            F.col("customer_key").isNull()
            | (F.trim("customer_key") == "")
        ).count()

        duplicate_keys = (
            gold.groupBy("customer_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert null_keys == 0
        assert duplicate_keys == 0

        print("PASS: Customer keys unique and non-null")

        # -----------------------------------
        # 5. Validate source-qualified keys
        # -----------------------------------

        expected_key = F.concat(
            F.col("source_system"),
            F.lit(":"),
            F.col("source_customer_id")
        )

        invalid_keys = gold.filter(
            ~F.col("customer_key").eqNullSafe(expected_key)
        ).count()

        assert invalid_keys == 0

        print("PASS: Source-qualified keys verified")

        # -----------------------------------
        # 6. Validate Silver-to-Gold coverage
        # -----------------------------------

        silver_keys = silver.select(
            "customer_key"
        )

        gold_keys = gold.select(
            "customer_key"
        )

        missing_gold = silver_keys.join(
            gold_keys,
            on="customer_key",
            how="left_anti"
        ).count()

        unexpected_gold = gold_keys.join(
            silver_keys,
            on="customer_key",
            how="left_anti"
        ).count()

        assert missing_gold == 0
        assert unexpected_gold == 0

        print(
            "PASS: Every Silver customer "
            "exists in Gold"
        )

        print(
            "PASS: No unexpected Gold customers"
        )

        # -----------------------------------
        # 7. Validate customer names
        # -----------------------------------

        expected_name = F.concat_ws(
            " ",
            F.col("first_name"),
            F.col("last_name")
        )

        incorrect_names = gold.filter(
            ~F.col("customer_name").eqNullSafe(
                expected_name
            )
        ).count()

        assert incorrect_names == 0

        print("PASS: Customer names verified")

        # -----------------------------------
        # 8. Validate registration attributes
        # -----------------------------------

        incorrect_year = gold.filter(
            ~F.col("registration_year").eqNullSafe(
                F.year("registration_date")
            )
        ).count()

        incorrect_month = gold.filter(
            ~F.col("registration_month").eqNullSafe(
                F.month("registration_date")
            )
        ).count()

        assert incorrect_year == 0
        assert incorrect_month == 0

        print(
            "PASS: Registration year "
            "and month verified"
        )

        # -----------------------------------
        # 9. Verify Silver attributes retained
        # -----------------------------------

        shared_columns = [
            "customer_key",
            "source_system",
            "source_customer_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "city",
            "state",
            "registration_date",
            "ingestion_timestamp",
        ]

        silver_shared = silver.select(
            *shared_columns
        )

        gold_shared = gold.select(
            *shared_columns
        )

        missing_or_changed = (
            silver_shared.exceptAll(gold_shared).count()
        )

        unexpected_or_changed = (
            gold_shared.exceptAll(silver_shared).count()
        )

        assert missing_or_changed == 0
        assert unexpected_or_changed == 0

        print(
            "PASS: Silver customer attributes "
            "preserved exactly"
        )

        # -----------------------------------
        # 10. Validate Delta Lake metadata
        # -----------------------------------

        assert DeltaTable.isDeltaTable(
            spark,
            GOLD_PATH
        )

        print("PASS: Gold table is valid Delta")

        print("\n=== GOLD DELTA HISTORY ===")

        DeltaTable.forPath(
            spark,
            GOLD_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        # -----------------------------------
        # 11. Final result
        # -----------------------------------

        print(
            "\nSUCCESS: ALL GOLD CUSTOMER "
            "DIMENSION VALIDATIONS PASSED!"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
