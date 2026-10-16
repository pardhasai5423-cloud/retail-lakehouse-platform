
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

SILVER_PRODUCTS_PATH = (
    f"s3a://{BUCKET}/silver/products"
)

SILVER_SUPPLIERS_PATH = (
    f"s3a://{BUCKET}/silver/suppliers"
)

GOLD_PRODUCTS_PATH = (
    f"s3a://{BUCKET}/gold/dim_products"
)


def source_counts(df):
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
            "\n=== INDEPENDENT GOLD PRODUCT "
            "DIMENSION VALIDATION ==="
        )

        # -----------------------------------
        # 1. Read persisted Delta tables
        # -----------------------------------

        silver = (
            spark.read.format("delta")
            .load(SILVER_PRODUCTS_PATH)
        )

        gold = (
            spark.read.format("delta")
            .load(GOLD_PRODUCTS_PATH)
        )

        suppliers = (
            spark.read.format("delta")
            .load(SILVER_SUPPLIERS_PATH)
        )

        # -----------------------------------
        # 2. Validate record counts
        # -----------------------------------

        silver_count = silver.count()
        gold_count = gold.count()

        assert silver_count == 1800
        assert gold_count == 1800

        print("PASS: Silver Products = 1,800")
        print("PASS: Gold Products = 1,800")

        # -----------------------------------
        # 3. Validate source counts
        # -----------------------------------

        expected_sources = {
            "retailmax": 1000,
            "freshmart": 800,
        }

        assert source_counts(silver) == expected_sources
        assert source_counts(gold) == expected_sources

        print("PASS: Source-level counts verified")

        # -----------------------------------
        # 4. Validate Gold schema
        # -----------------------------------

        expected_types = {
            "product_key": "string",
            "source_system": "string",
            "source_product_id": "string",
            "product_name": "string",
            "category": "string",
            "brand": "string",
            "unit_price": "decimal(12,2)",
            "source_supplier_id": "string",
            "supplier_key": "string",
            "created_date": "date",
            "created_year": "int",
            "created_month": "int",
            "price_quality_status": "string",
            "validation_status": "string",
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

        print("PASS: Gold product schema verified")

        # -----------------------------------
        # 5. Validate product keys
        # -----------------------------------

        null_keys = gold.filter(
            F.col("product_key").isNull()
            | (F.trim("product_key") == "")
        ).count()

        duplicate_keys = (
            gold.groupBy("product_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert null_keys == 0
        assert duplicate_keys == 0

        expected_product_key = F.concat(
            F.col("source_system"),
            F.lit(":"),
            F.col("source_product_id")
        )

        invalid_product_keys = gold.filter(
            ~F.col("product_key").eqNullSafe(
                expected_product_key
            )
        ).count()

        assert invalid_product_keys == 0

        print(
            "PASS: Product keys unique, non-null "
            "and source-qualified"
        )

        # -----------------------------------
        # 6. Validate Silver-to-Gold coverage
        # -----------------------------------

        missing_gold = (
            silver.select("product_key")
            .join(
                gold.select("product_key"),
                on="product_key",
                how="left_anti"
            )
            .count()
        )

        unexpected_gold = (
            gold.select("product_key")
            .join(
                silver.select("product_key"),
                on="product_key",
                how="left_anti"
            )
            .count()
        )

        assert missing_gold == 0
        assert unexpected_gold == 0

        print("PASS: Silver-to-Gold product coverage")

        # -----------------------------------
        # 7. Verify original attributes
        # -----------------------------------

        shared_columns = [
            "product_key",
            "source_system",
            "source_product_id",
            "product_name",
            "category",
            "brand",
            "unit_price",
            "source_supplier_id",
            "created_date",
            "price_quality_status",
            "validation_status",
            "ingestion_timestamp",
            "batch_id",
            "source_file",
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
            "PASS: Silver product attributes "
            "preserved exactly"
        )

        # -----------------------------------
        # 8. Validate price quality
        # -----------------------------------

        missing_prices = gold.filter(
            F.col("unit_price").isNull()
        ).count()

        missing_price_flags = gold.filter(
            F.col("price_quality_status")
            == "MISSING_PRICE"
        ).count()

        valid_price_flags = gold.filter(
            F.col("price_quality_status")
            == "VALID"
        ).count()

        incorrect_missing_flags = gold.filter(
            F.col("unit_price").isNull()
            & (
                F.col("price_quality_status")
                != "MISSING_PRICE"
            )
        ).count()

        incorrect_valid_flags = gold.filter(
            F.col("unit_price").isNotNull()
            & (
                F.col("price_quality_status")
                != "VALID"
            )
        ).count()

        assert missing_prices == 18
        assert missing_price_flags == 18
        assert valid_price_flags == 1782

        assert incorrect_missing_flags == 0
        assert incorrect_valid_flags == 0

        print(
            "PASS: 18 missing prices correctly "
            "preserved and flagged"
        )

        # -----------------------------------
        # 9. Validate positive catalog prices
        # -----------------------------------

        invalid_prices = gold.filter(
            F.col("unit_price").isNotNull()
            & (F.col("unit_price") <= 0)
        ).count()

        assert invalid_prices == 0

        print("PASS: All available prices positive")

        # -----------------------------------
        # 10. Validate product statuses
        # -----------------------------------

        invalid_statuses = gold.filter(
            F.col("validation_status").isNull()
            | (
                F.col("validation_status")
                != "VALID"
            )
        ).count()

        assert invalid_statuses == 0

        print("PASS: Product validation statuses")

        # -----------------------------------
        # 11. Validate supplier key construction
        # -----------------------------------

        expected_supplier_key = F.when(
            F.col("source_supplier_id").isNotNull(),
            F.concat(
                F.col("source_system"),
                F.lit(":"),
                F.col("source_supplier_id")
            )
        ).otherwise(
            F.lit(None).cast("string")
        )

        invalid_supplier_keys = gold.filter(
            ~F.col("supplier_key").eqNullSafe(
                expected_supplier_key
            )
        ).count()

        assert invalid_supplier_keys == 0

        print("PASS: Supplier keys correctly derived")

        # -----------------------------------
        # 12. Validate supplier references
        # -----------------------------------

        supplier_keys = suppliers.select(
            "supplier_key"
        ).distinct()

        missing_supplier_references = (
            gold.join(
                supplier_keys,
                on="supplier_key",
                how="left_anti"
            )
            .count()
        )

        assert missing_supplier_references == 0

        print(
            "PASS: All product supplier "
            "references valid"
        )

        # -----------------------------------
        # 13. Validate derived date attributes
        # -----------------------------------

        incorrect_year = gold.filter(
            ~F.col("created_year").eqNullSafe(
                F.year("created_date")
            )
        ).count()

        incorrect_month = gold.filter(
            ~F.col("created_month").eqNullSafe(
                F.month("created_date")
            )
        ).count()

        assert incorrect_year == 0
        assert incorrect_month == 0

        print(
            "PASS: Product creation year "
            "and month verified"
        )

        # -----------------------------------
        # 14. Validate lineage
        # -----------------------------------

        missing_lineage = gold.filter(
            F.col("ingestion_timestamp").isNull()
            | F.col("batch_id").isNull()
            | (F.trim("batch_id") == "")
            | F.col("source_file").isNull()
            | (F.trim("source_file") == "")
        ).count()

        assert missing_lineage == 0

        print("PASS: Product lineage preserved")

        # -----------------------------------
        # 15. Validate Delta table
        # -----------------------------------

        assert DeltaTable.isDeltaTable(
            spark,
            GOLD_PRODUCTS_PATH
        )

        print("PASS: Gold Products is a Delta table")

        print("\n=== GOLD PRODUCT DELTA HISTORY ===")

        DeltaTable.forPath(
            spark,
            GOLD_PRODUCTS_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        # -----------------------------------
        # 16. Final summary
        # -----------------------------------

        print(
            "\nSUCCESS: ALL GOLD PRODUCT "
            "DIMENSION VALIDATIONS PASSED!"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
