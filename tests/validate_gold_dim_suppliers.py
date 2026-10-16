
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

SILVER_SUPPLIERS_PATH = (
    f"s3a://{BUCKET}/silver/suppliers"
)

GOLD_SUPPLIERS_PATH = (
    f"s3a://{BUCKET}/gold/dim_suppliers"
)

GOLD_PRODUCTS_PATH = (
    f"s3a://{BUCKET}/gold/dim_products"
)


def get_source_counts(df):
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
            "\n=== INDEPENDENT GOLD SUPPLIER "
            "DIMENSION VALIDATION ==="
        )

        # 1. Read persisted Delta tables
        silver = (
            spark.read.format("delta")
            .load(SILVER_SUPPLIERS_PATH)
        )

        gold = (
            spark.read.format("delta")
            .load(GOLD_SUPPLIERS_PATH)
        )

        products = (
            spark.read.format("delta")
            .load(GOLD_PRODUCTS_PATH)
        )

        # 2. Validate record counts
        assert silver.count() == 180
        assert gold.count() == 180

        print("PASS: Silver Suppliers = 180")
        print("PASS: Gold Suppliers = 180")

        # 3. Validate source distribution
        expected_sources = {
            "retailmax": 100,
            "freshmart": 80,
        }

        assert get_source_counts(silver) == expected_sources
        assert get_source_counts(gold) == expected_sources

        print("PASS: Source-level counts verified")

        # 4. Validate Gold schema
        expected_types = {
            "supplier_key": "string",
            "source_system": "string",
            "source_supplier_id": "string",
            "supplier_name": "string",
            "contact_email": "string",
            "phone": "string",
            "city": "string",
            "state": "string",
            "ingestion_timestamp": "timestamp",
            "batch_id": "string",
            "source_file": "string",
        }

        actual_types = dict(gold.dtypes)

        for column, expected_type in expected_types.items():
            assert actual_types.get(column) == expected_type, (
                f"{column}: expected {expected_type}, "
                f"found {actual_types.get(column)}"
            )

        print("PASS: Gold supplier schema verified")

        # 5. Validate supplier key uniqueness
        invalid_keys = gold.filter(
            F.col("supplier_key").isNull()
            | (F.trim("supplier_key") == "")
        ).count()

        duplicate_keys = (
            gold.groupBy("supplier_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert invalid_keys == 0
        assert duplicate_keys == 0

        print("PASS: Supplier keys unique and non-null")

        # 6. Validate source-qualified keys
        expected_key = F.concat(
            F.col("source_system"),
            F.lit(":"),
            F.col("source_supplier_id")
        )

        incorrect_keys = gold.filter(
            ~F.col("supplier_key").eqNullSafe(expected_key)
        ).count()

        assert incorrect_keys == 0

        print("PASS: Source-qualified keys verified")

        # 7. Validate Silver-to-Gold coverage
        missing_gold = (
            silver.select("supplier_key")
            .join(
                gold.select("supplier_key"),
                on="supplier_key",
                how="left_anti"
            )
            .count()
        )

        unexpected_gold = (
            gold.select("supplier_key")
            .join(
                silver.select("supplier_key"),
                on="supplier_key",
                how="left_anti"
            )
            .count()
        )

        assert missing_gold == 0
        assert unexpected_gold == 0

        print("PASS: Silver-to-Gold supplier coverage")

        # 8. Verify every attribute was preserved
        shared_columns = [
            "supplier_key",
            "source_system",
            "source_supplier_id",
            "supplier_name",
            "contact_email",
            "phone",
            "city",
            "state",
            "ingestion_timestamp",
            "batch_id",
            "source_file",
        ]

        silver_shared = silver.select(*shared_columns)
        gold_shared = gold.select(*shared_columns)

        missing_or_changed = (
            silver_shared.exceptAll(gold_shared).count()
        )

        unexpected_or_changed = (
            gold_shared.exceptAll(silver_shared).count()
        )

        assert missing_or_changed == 0
        assert unexpected_or_changed == 0

        print(
            "PASS: All Silver supplier attributes "
            "preserved exactly"
        )

        # 9. Validate product-to-supplier references
        supplier_keys = gold.select(
            "supplier_key"
        )

        missing_references = (
            products.join(
                supplier_keys,
                on="supplier_key",
                how="left_anti"
            )
            .count()
        )

        assert missing_references == 0

        print(
            "PASS: All Gold products have "
            "valid Gold supplier references"
        )

        # 10. Validate product-to-supplier source
        # consistency
        product_supplier_pairs = (
            products.alias("p")
            .join(
                gold.alias("s"),
                on="supplier_key",
                how="inner"
            )
        )

        mismatched_sources = (
            product_supplier_pairs
            .filter(
                ~F.col("p.source_system").eqNullSafe(
                    F.col("s.source_system")
                )
            )
            .count()
        )

        assert mismatched_sources == 0

        print(
            "PASS: Product and supplier "
            "source systems match"
        )

        # 11. Validate lineage
        missing_lineage = gold.filter(
            F.col("ingestion_timestamp").isNull()
            | F.col("batch_id").isNull()
            | (F.trim("batch_id") == "")
            | F.col("source_file").isNull()
            | (F.trim("source_file") == "")
        ).count()

        assert missing_lineage == 0

        print("PASS: Supplier lineage preserved")

        # 12. Validate Delta metadata
        assert DeltaTable.isDeltaTable(
            spark,
            GOLD_SUPPLIERS_PATH
        )

        print("PASS: Gold Suppliers is a Delta table")

        print("\n=== GOLD SUPPLIER DELTA HISTORY ===")

        DeltaTable.forPath(
            spark,
            GOLD_SUPPLIERS_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: ALL GOLD SUPPLIER "
            "DIMENSION VALIDATIONS PASSED!"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
