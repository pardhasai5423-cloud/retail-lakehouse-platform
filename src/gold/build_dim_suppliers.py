
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

SILVER_SUPPLIERS_PATH = (
    f"s3a://{BUCKET}/silver/suppliers"
)

GOLD_SUPPLIERS_PATH = (
    f"s3a://{BUCKET}/gold/dim_suppliers"
)

GOLD_PRODUCTS_PATH = (
    f"s3a://{BUCKET}/gold/dim_products"
)


def build_supplier_dimension(silver_suppliers):
    """
    Build a Gold supplier dimension from
    validated Silver supplier records.

    Preserve source-qualified identities and lineage.
    """

    columns = [
        "supplier_key",
        "source_system",
        "source_supplier_id",
        "supplier_name",
        "contact_email",
        "phone",
        "city",
        "state",
        "ingestion_timestamp",
    ]

    for optional_column in [
        "batch_id",
        "source_file",
    ]:
        if optional_column in silver_suppliers.columns:
            columns.append(optional_column)

    return silver_suppliers.select(*columns)


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(
            "\n=== GOLD SUPPLIER DIMENSION PIPELINE ==="
        )

        # 1. Read Silver suppliers
        silver_suppliers = (
            spark.read.format("delta")
            .load(SILVER_SUPPLIERS_PATH)
        )

        silver_count = silver_suppliers.count()
        assert silver_count == 180

        print(f"Silver Suppliers: {silver_count:,}")

        # 2. Build dimension
        gold_suppliers = build_supplier_dimension(
            silver_suppliers
        )

        gold_count = gold_suppliers.count()
        assert gold_count == silver_count

        print(f"Gold Suppliers: {gold_count:,}")

        # 3. Validate supplier keys
        invalid_keys = gold_suppliers.filter(
            F.col("supplier_key").isNull()
            | (F.trim("supplier_key") == "")
        ).count()

        duplicate_keys = (
            gold_suppliers
            .groupBy("supplier_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert invalid_keys == 0
        assert duplicate_keys == 0

        expected_key = F.concat(
            F.col("source_system"),
            F.lit(":"),
            F.col("source_supplier_id")
        )

        incorrectly_qualified = gold_suppliers.filter(
            ~F.col("supplier_key").eqNullSafe(expected_key)
        ).count()

        assert incorrectly_qualified == 0

        print(
            "PASS: Supplier keys unique, non-null "
            "and source-qualified"
        )

        # 4. Validate source counts
        print("\n=== SUPPLIERS BY SOURCE ===")

        gold_suppliers.groupBy(
            "source_system"
        ).count().show()

        source_counts = {
            row["source_system"]: row["count"]
            for row in gold_suppliers.groupBy(
                "source_system"
            ).count().collect()
        }

        assert source_counts == {
            "retailmax": 100,
            "freshmart": 80,
        }

        print("PASS: Source counts verified")

        # 5. Validate product references
        gold_products = (
            spark.read.format("delta")
            .load(GOLD_PRODUCTS_PATH)
        )

        supplier_keys = gold_suppliers.select(
            "supplier_key"
        )

        missing_references = (
            gold_products
            .join(
                supplier_keys,
                on="supplier_key",
                how="left_anti"
            )
            .count()
        )

        assert missing_references == 0

        print(
            "PASS: All Gold products reference "
            "valid Gold suppliers"
        )

        # 6. Validate lineage
        missing_lineage = gold_suppliers.filter(
            F.col("ingestion_timestamp").isNull()
        ).count()

        assert missing_lineage == 0

        print("PASS: Supplier lineage preserved")

        # 7. Write Gold dimension
        print(
            "\n=== WRITING GOLD DIM_SUPPLIERS ==="
        )

        (
            gold_suppliers.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(GOLD_SUPPLIERS_PATH)
        )

        print("PASS: Gold supplier dimension written")

        # 8. Verify persisted table
        assert DeltaTable.isDeltaTable(
            spark,
            GOLD_SUPPLIERS_PATH
        )

        persisted = (
            spark.read.format("delta")
            .load(GOLD_SUPPLIERS_PATH)
        )

        assert persisted.count() == 180

        print("PASS: Persisted count = 180")

        # 9. Display schema
        print("\n=== GOLD SUPPLIER SCHEMA ===")
        persisted.printSchema()

        # 10. Display sample records
        print("\n=== SAMPLE GOLD SUPPLIERS ===")

        persisted.select(
            "supplier_key",
            "supplier_name",
            "city",
            "state"
        ).show(10, truncate=False)

        # 11. Display Delta history
        print("\n=== DELTA HISTORY ===")

        DeltaTable.forPath(
            spark,
            GOLD_SUPPLIERS_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: GOLD DIM_SUPPLIERS CREATED!"
        )

        print(f"Gold path: {GOLD_SUPPLIERS_PATH}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
