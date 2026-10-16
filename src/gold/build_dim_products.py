
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

SILVER_PRODUCTS_PATH = (
    f"s3a://{BUCKET}/silver/products"
)

SILVER_SUPPLIERS_PATH = (
    f"s3a://{BUCKET}/silver/suppliers"
)

GOLD_PRODUCTS_PATH = (
    f"s3a://{BUCKET}/gold/dim_products"
)


def build_product_dimension(silver_products):
    """
    Convert validated Silver Products into
    an analytics-ready Gold product dimension.

    Preserve missing prices and quality flags.
    """

    products = silver_products.withColumn(
        "supplier_key",
        F.when(
            F.col("source_supplier_id").isNotNull(),
            F.concat(
                F.col("source_system"),
                F.lit(":"),
                F.col("source_supplier_id")
            )
        ).otherwise(
            F.lit(None).cast("string")
        )
    )

    products = (
        products
        .withColumn(
            "created_year",
            F.year("created_date")
        )
        .withColumn(
            "created_month",
            F.month("created_date")
        )
    )

    selected_columns = [
        "product_key",
        "source_system",
        "source_product_id",
        "product_name",
        "category",
        "brand",
        "unit_price",
        "source_supplier_id",
        "supplier_key",
        "created_date",
        "created_year",
        "created_month",
        "price_quality_status",
        "validation_status",
        "ingestion_timestamp",
    ]

    for optional_column in [
        "batch_id",
        "source_file",
    ]:
        if optional_column in products.columns:
            selected_columns.append(
                optional_column
            )

    return products.select(
        *selected_columns
    )


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(
            "\n=== GOLD PRODUCT DIMENSION PIPELINE ==="
        )

        # ----------------------------------
        # 1. Read Silver Products
        # ----------------------------------

        silver_products = (
            spark.read.format("delta")
            .load(SILVER_PRODUCTS_PATH)
        )

        silver_count = silver_products.count()

        print(
            f"Silver Products: {silver_count:,}"
        )

        assert silver_count == 1800

        # ----------------------------------
        # 2. Build Gold dimension
        # ----------------------------------

        gold_products = build_product_dimension(
            silver_products
        )

        gold_count = gold_products.count()

        assert gold_count == silver_count

        print(
            f"Gold Products: {gold_count:,}"
        )

        # ----------------------------------
        # 3. Validate product keys
        # ----------------------------------

        invalid_keys = gold_products.filter(
            F.col("product_key").isNull()
            | (
                F.trim("product_key") == ""
            )
        ).count()

        duplicate_keys = (
            gold_products
            .groupBy("product_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert invalid_keys == 0
        assert duplicate_keys == 0

        print(
            "PASS: Product keys unique and non-null"
        )

        # ----------------------------------
        # 4. Validate source counts
        # ----------------------------------

        print("\n=== PRODUCTS BY SOURCE ===")

        gold_products.groupBy(
            "source_system"
        ).count().show()

        source_counts = {
            row["source_system"]: row["count"]
            for row in gold_products.groupBy(
                "source_system"
            ).count().collect()
        }

        assert source_counts == {
            "retailmax": 1000,
            "freshmart": 800,
        }

        print("PASS: Source counts verified")

        # ----------------------------------
        # 5. Validate price quality
        # ----------------------------------

        missing_prices = gold_products.filter(
            F.col("unit_price").isNull()
        ).count()

        flagged_missing_prices = gold_products.filter(
            F.col("price_quality_status")
            == "MISSING_PRICE"
        ).count()

        assert missing_prices == 18
        assert flagged_missing_prices == 18

        invalid_prices = gold_products.filter(
            F.col("unit_price").isNotNull()
            & (F.col("unit_price") <= 0)
        ).count()

        assert invalid_prices == 0

        print(
            "PASS: 18 missing product prices "
            "preserved and flagged"
        )

        # ----------------------------------
        # 6. Validate product status
        # ----------------------------------

        invalid_status = gold_products.filter(
            F.col("validation_status").isNull()
            | (
                F.col("validation_status")
                != "VALID"
            )
        ).count()

        assert invalid_status == 0

        print("PASS: All Gold products are valid")

        # ----------------------------------
        # 7. Validate supplier keys
        # ----------------------------------

        silver_suppliers = (
            spark.read.format("delta")
            .load(SILVER_SUPPLIERS_PATH)
        )

        supplier_keys = silver_suppliers.select(
            "supplier_key"
        ).distinct()

        missing_suppliers = (
            gold_products
            .join(
                supplier_keys,
                on="supplier_key",
                how="left_anti"
            )
            .count()
        )

        assert missing_suppliers == 0

        print(
            "PASS: All product supplier "
            "references valid"
        )

        # ----------------------------------
        # 8. Validate lineage
        # ----------------------------------

        missing_lineage = gold_products.filter(
            F.col("ingestion_timestamp").isNull()
        ).count()

        assert missing_lineage == 0

        print("PASS: Product lineage preserved")

        # ----------------------------------
        # 9. Write Gold Delta table
        # ----------------------------------

        print(
            "\n=== WRITING GOLD DIM_PRODUCTS ==="
        )

        (
            gold_products.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(GOLD_PRODUCTS_PATH)
        )

        print(
            "PASS: Gold product dimension written"
        )

        # ----------------------------------
        # 10. Verify persisted table
        # ----------------------------------

        assert DeltaTable.isDeltaTable(
            spark,
            GOLD_PRODUCTS_PATH
        )

        persisted = (
            spark.read.format("delta")
            .load(GOLD_PRODUCTS_PATH)
        )

        assert persisted.count() == 1800

        print(
            "PASS: Persisted count = 1,800"
        )

        # ----------------------------------
        # 11. Show schema
        # ----------------------------------

        print(
            "\n=== GOLD PRODUCT DIMENSION SCHEMA ==="
        )

        persisted.printSchema()

        # ----------------------------------
        # 12. Show price quality
        # ----------------------------------

        print(
            "\n=== PRODUCT PRICE QUALITY ==="
        )

        persisted.groupBy(
            "price_quality_status"
        ).count().show()

        # ----------------------------------
        # 13. Show sample products
        # ----------------------------------

        print(
            "\n=== SAMPLE GOLD PRODUCTS ==="
        )

        persisted.select(
            "product_key",
            "product_name",
            "category",
            "brand",
            "unit_price",
            "supplier_key"
        ).show(10, truncate=False)

        # ----------------------------------
        # 14. Delta history
        # ----------------------------------

        print("\n=== DELTA HISTORY ===")

        DeltaTable.forPath(
            spark,
            GOLD_PRODUCTS_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: GOLD DIM_PRODUCTS CREATED!"
        )

        print(
            f"Gold path: {GOLD_PRODUCTS_PATH}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
