
import os
import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)

sys.path.insert(
    0,
    os.path.join(PROJECT_ROOT, "src", "silver")
)

from transform_customers import create_spark


BUCKET = "retail-lakehouse-parsam-2026"

SUPPLIERS_PATH = f"s3a://{BUCKET}/silver/suppliers"
PRODUCTS_PATH = f"s3a://{BUCKET}/silver/products"


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== VALIDATING SILVER SUPPLIERS ===")

        suppliers = (
            spark.read.format("delta")
            .load(SUPPLIERS_PATH)
        )

        products = (
            spark.read.format("delta")
            .load(PRODUCTS_PATH)
        )

        # 1. Validate supplier record counts.
        total = suppliers.count()

        assert total == 180, (
            f"Expected 180 suppliers, found {total}"
        )

        print("PASS: Total suppliers = 180")

        counts = {
            row["source_system"]: row["count"]
            for row in suppliers.groupBy(
                "source_system"
            ).count().collect()
        }

        assert counts == {
            "retailmax": 100,
            "freshmart": 80,
        }

        print("PASS: RetailMax = 100")
        print("PASS: FreshMart = 80")

        # 2. Validate unique supplier identifiers.
        duplicate_count = (
            suppliers
            .groupBy(
                "source_system",
                "source_supplier_id"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert duplicate_count == 0

        print("PASS: No duplicate supplier IDs")

        # 3. Validate mandatory attributes.
        invalid_required = suppliers.filter(
            F.col("source_system").isNull()
            | F.col("source_supplier_id").isNull()
            | (F.trim(F.col("source_supplier_id")) == "")
            | F.col("supplier_key").isNull()
            | F.col("supplier_name").isNull()
            | (F.trim(F.col("supplier_name")) == "")
        ).count()

        assert invalid_required == 0

        print("PASS: Mandatory supplier fields present")

        # 4. Validate composite supplier keys.
        invalid_keys = suppliers.filter(
            ~F.col("supplier_key").eqNullSafe(
                F.concat(
                    F.col("source_system"),
                    F.lit(":"),
                    F.col("source_supplier_id")
                )
            )
        ).count()

        assert invalid_keys == 0

        print("PASS: Supplier keys correctly constructed")

        # 5. Validate email quality flags.
        invalid_email_flags = suppliers.filter(
            F.col("email_quality_status").isNull()
            | (F.col("email_quality_status") != "VALID")
        ).count()

        assert invalid_email_flags == 0

        print("PASS: All supplier email quality flags VALID")

        # 6. Validate overall status.
        invalid_status = suppliers.filter(
            F.col("validation_status").isNull()
            | (F.col("validation_status") != "VALID")
        ).count()

        assert invalid_status == 0

        print("PASS: All suppliers passed validation")

        # 7. Validate source lineage.
        missing_lineage = suppliers.filter(
            F.col("ingestion_timestamp").isNull()
            | F.col("batch_id").isNull()
            | F.col("source_file").isNull()
        ).count()

        assert missing_lineage == 0

        print("PASS: Supplier lineage metadata preserved")

        # 8. Verify Delta Lake table.
        assert DeltaTable.isDeltaTable(
            spark, SUPPLIERS_PATH
        )

        print("PASS: Valid Suppliers Delta table")

        print("\n=== PRODUCT-SUPPLIER INTEGRITY ===")

        # 9. Confirm product count.
        product_count = products.count()

        assert product_count == 1800

        print("PASS: Products available = 1,800")

        # 10. Identify products with no matching supplier.
        unmatched_products = products.alias("p").join(
            suppliers.alias("s"),
            (
                (F.col("p.source_system")
                 == F.col("s.source_system"))
                &
                (F.col("p.source_supplier_id")
                 == F.col("s.source_supplier_id"))
            ),
            "left_anti"
        )

        unmatched_count = unmatched_products.count()

        print(
            f"Products with missing supplier references: "
            f"{unmatched_count}"
        )

        if unmatched_count > 0:
            print("\n=== SAMPLE UNMATCHED PRODUCTS ===")

            unmatched_products.select(
                "source_system",
                "source_product_id",
                "product_name",
                "source_supplier_id"
            ).show(20, truncate=False)

        assert unmatched_count == 0, (
            f"{unmatched_count} products reference "
            "missing suppliers"
        )

        print(
            "PASS: All products reference valid "
            "Silver suppliers"
        )

        # 11. Confirm join does not multiply product rows.
        joined_count = products.alias("p").join(
            suppliers.alias("s"),
            (
                (F.col("p.source_system")
                 == F.col("s.source_system"))
                &
                (F.col("p.source_supplier_id")
                 == F.col("s.source_supplier_id"))
            ),
            "inner"
        ).count()

        assert joined_count == product_count, (
            "Product-supplier join changed the row count"
        )

        print(
            "PASS: Product-supplier join preserves "
            "all 1,800 product rows"
        )

        # 12. Display Delta history.
        print("\n=== SUPPLIER DELTA HISTORY ===")

        DeltaTable.forPath(
            spark, SUPPLIERS_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: ALL SUPPLIER AND "
            "REFERENTIAL INTEGRITY CHECKS PASSED!"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
