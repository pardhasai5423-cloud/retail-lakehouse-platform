from src.config.write_safety import require_new_s3_target

import os
import sys

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


SILVER_CUSTOMERS_PATH = get_s3_path(
    "silver", "customers"
)

GOLD_CUSTOMERS_PATH = get_s3_path(
    "gold", "dim_customers"
)


def build_customer_dimension(silver_customers):
    """
    Transform validated Silver Customers into
    an analytics-ready Gold customer dimension.
    """

    return (
        silver_customers
        .select(
            F.col("customer_key"),
            F.col("source_system"),
            F.col("source_customer_id"),
            F.col("first_name"),
            F.col("last_name"),
            F.concat_ws(
                " ",
                F.col("first_name"),
                F.col("last_name")
            ).alias("customer_name"),
            F.col("email"),
            F.col("phone"),
            F.col("city"),
            F.col("state"),
            F.col("registration_date"),
            F.year(
                "registration_date"
            ).alias("registration_year"),
            F.month(
                "registration_date"
            ).alias("registration_month"),
            F.col("ingestion_timestamp")
        )
    )


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(
            "\n=== GOLD CUSTOMER DIMENSION PIPELINE ==="
        )

        # -----------------------------------
        # 1. Read Silver Customers
        # -----------------------------------

        silver_customers = (
            spark.read.format("delta")
            .load(SILVER_CUSTOMERS_PATH)
        )

        silver_count = silver_customers.count()

        print(
            f"Silver Customers: {silver_count:,}"
        )

        assert silver_count == 9000

        # -----------------------------------
        # 2. Build Gold dimension
        # -----------------------------------

        gold_customers = build_customer_dimension(
            silver_customers
        )

        gold_count = gold_customers.count()

        assert gold_count == silver_count

        print(
            f"Gold Customers: {gold_count:,}"
        )

        # -----------------------------------
        # 3. Validate customer keys
        # -----------------------------------

        null_keys = gold_customers.filter(
            F.col("customer_key").isNull()
        ).count()

        duplicate_keys = (
            gold_customers
            .groupBy("customer_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert null_keys == 0
        assert duplicate_keys == 0

        print("PASS: Customer keys unique and non-null")

        # -----------------------------------
        # 4. Validate source counts
        # -----------------------------------

        print("\n=== CUSTOMERS BY SOURCE ===")

        gold_customers.groupBy(
            "source_system"
        ).count().show()

        source_counts = {
            row["source_system"]: row["count"]
            for row in gold_customers.groupBy(
                "source_system"
            ).count().collect()
        }

        assert source_counts == {
            "retailmax": 5000,
            "freshmart": 4000,
        }

        print("PASS: Source counts verified")

        # -----------------------------------
        # 5. Write Gold Delta table
        # -----------------------------------

        print(
            "\n=== WRITING GOLD DIM_CUSTOMERS ==="
        )

        require_new_s3_target(spark, GOLD_CUSTOMERS_PATH)
        (
            gold_customers.write
            .format("delta")
            .mode("errorifexists")
            .option("overwriteSchema", "true")
            .save(GOLD_CUSTOMERS_PATH)
        )

        print("PASS: Gold customer dimension written")

        # -----------------------------------
        # 6. Verify persisted table
        # -----------------------------------

        assert DeltaTable.isDeltaTable(
            spark,
            GOLD_CUSTOMERS_PATH
        )

        persisted = (
            spark.read.format("delta")
            .load(GOLD_CUSTOMERS_PATH)
        )

        assert persisted.count() == 9000

        print("PASS: Persisted count = 9,000")

        print("\n=== GOLD CUSTOMER SCHEMA ===")

        persisted.printSchema()

        print("\n=== SAMPLE GOLD CUSTOMERS ===")

        persisted.select(
            "customer_key",
            "customer_name",
            "city",
            "state",
            "registration_year"
        ).show(10, truncate=False)

        print("\n=== DELTA HISTORY ===")

        DeltaTable.forPath(
            spark,
            GOLD_CUSTOMERS_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: GOLD DIM_CUSTOMERS CREATED!"
        )

        print(
            f"Gold path: {GOLD_CUSTOMERS_PATH}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    require_safe_write_environment()
    main()
