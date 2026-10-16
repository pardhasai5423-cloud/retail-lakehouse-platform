
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
SILVER_PATH = f"s3a://{BUCKET}/silver/inventory"
GOLD_BASE = f"s3a://{BUCKET}/gold"
GOLD_PATH = f"{GOLD_BASE}/fact_inventory"

EXPECTED_COUNT = 3600

EXPECTED_SOURCES = {
    "retailmax": 2000,
    "freshmart": 1600,
}

EXPECTED_STOCK = {
    "OUT_OF_STOCK": 3,
    "REORDER_REQUIRED": 436,
    "SUFFICIENT_STOCK": 3161,
}

EXPECTED_FRESHNESS = {
    "CURRENT": 889,
    "STALE": 2711,
}


def count_by(df, column):
    return {
        row[column]: row["count"]
        for row in (
            df.groupBy(column)
            .count()
            .collect()
        )
    }



def validate_dimension(
    fact,
    dimension,
    fact_key,
    dimension_key,
    name,
    check_source=True
):
    """
    Independently verify dimension uniqueness,
    non-null fact references, key coverage,
    and source-system consistency.
    """

    # 1. Check dimension keys

    dim_keys = dimension.select(dimension_key)

    assert dim_keys.filter(
        F.col(dimension_key).isNull()
    ).count() == 0, (
        f"{name} has null dimension keys"
    )

    duplicate_keys = (
        dim_keys.groupBy(dimension_key)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    assert duplicate_keys == 0, (
        f"{name} has duplicate dimension keys"
    )

    # 2. Check fact foreign keys

    null_fact_keys = fact.filter(
        F.col(fact_key).isNull()
    ).count()

    assert null_fact_keys == 0, (
        f"Null {fact_key} in Inventory"
    )

    # 3. Check foreign-key coverage.
    # Use aliases to avoid ambiguous references.

    fact_keys = fact.select(
        F.col(fact_key).alias("fact_fk")
    )

    dimension_keys = dimension.select(
        F.col(dimension_key).alias("dimension_pk")
    )

    missing = (
        fact_keys.alias("f")
        .join(
            dimension_keys.alias("d"),
            F.col("f.fact_fk")
            == F.col("d.dimension_pk"),
            "left_anti"
        )
        .count()
    )

    assert missing == 0, (
        f"{missing} Inventory records "
        f"missing {name}"
    )

    # 4. Check source-system consistency
    # for Product and Store dimensions.

    if check_source:

        fact_refs = fact.select(
            F.col(fact_key).alias("fact_fk"),
            F.col("source_system").alias(
                "fact_source"
            )
        )

        dimension_refs = dimension.select(
            F.col(dimension_key).alias(
                "dimension_pk"
            ),
            F.col("source_system").alias(
                "dimension_source"
            )
        )

        mismatches = (
            fact_refs.alias("f")
            .join(
                dimension_refs.alias("d"),
                F.col("f.fact_fk")
                == F.col("d.dimension_pk"),
                "inner"
            )
            .filter(
                ~F.col("f.fact_source").eqNullSafe(
                    F.col("d.dimension_source")
                )
            )
            .count()
        )

        assert mismatches == 0, (
            f"{name} has source-system mismatches"
        )

    print(
        f"PASS: {name} references and "
        "dimension integrity"
    )



def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(
            "\n=== INDEPENDENT GOLD INVENTORY "
            "FACT VALIDATION ==="
        )

        # 1. Read persisted Silver and Gold

        silver = (
            spark.read.format("delta")
            .load(SILVER_PATH)
        )

        gold = (
            spark.read.format("delta")
            .load(GOLD_PATH)
        )

        # 2. Validate record counts

        assert silver.count() == EXPECTED_COUNT
        assert gold.count() == EXPECTED_COUNT

        print(
            "PASS: Silver and Gold each contain "
            "3,600 inventory records"
        )

        # 3. Validate source counts

        assert count_by(
            silver, "source_system"
        ) == EXPECTED_SOURCES

        assert count_by(
            gold, "source_system"
        ) == EXPECTED_SOURCES

        print("PASS: Source-level counts verified")

        # 4. Validate Gold schema

        expected_types = {
            "inventory_key": "string",
            "source_system": "string",
            "source_inventory_id": "string",
            "product_key": "string",
            "store_key": "string",
            "last_updated_date_key": "int",
            "stock_quantity": "int",
            "reorder_level": "int",
            "stock_status": "string",
            "freshness_status": "string",
            "last_updated": "date",
            "ingestion_timestamp": "timestamp",
            "batch_id": "string",
            "source_file": "string",
        }

        assert dict(gold.dtypes) == expected_types

        print("PASS: Gold Inventory schema verified")

        # 5. Validate inventory keys

        invalid_keys = gold.filter(
            F.col("inventory_key").isNull()
            | (F.trim("inventory_key") == "")
        ).count()

        duplicates = (
            gold.groupBy("inventory_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        expected_key = F.concat(
            F.col("source_system"),
            F.lit(":"),
            F.col("source_inventory_id")
        )

        incorrect_keys = gold.filter(
            ~F.col("inventory_key").eqNullSafe(
                expected_key
            )
        ).count()

        assert invalid_keys == 0
        assert duplicates == 0
        assert incorrect_keys == 0

        print(
            "PASS: Inventory keys unique "
            "and source-qualified"
        )

        # 6. Verify Silver-to-Gold key coverage

        missing_gold = (
            silver.select("inventory_key")
            .join(
                gold.select("inventory_key"),
                on="inventory_key",
                how="left_anti"
            )
            .count()
        )

        unexpected_gold = (
            gold.select("inventory_key")
            .join(
                silver.select("inventory_key"),
                on="inventory_key",
                how="left_anti"
            )
            .count()
        )

        assert missing_gold == 0
        assert unexpected_gold == 0

        print(
            "PASS: Complete Silver-to-Gold "
            "inventory coverage"
        )

        # 7. Verify exact preservation of
        # shared attributes, including renamed
        # freshness classification.

        shared_columns = [
            "inventory_key",
            "source_system",
            "source_inventory_id",
            "product_key",
            "store_key",
            "stock_quantity",
            "reorder_level",
            "stock_status",
            "last_updated",
            "ingestion_timestamp",
            "batch_id",
            "source_file",
        ]

        silver_shared = (
            silver.select(
                *shared_columns,
                F.col(
                    "inventory_freshness_status"
                ).alias("freshness_status")
            )
        )

        gold_shared = gold.select(
            *shared_columns,
            "freshness_status"
        )

        assert (
            silver_shared.exceptAll(
                gold_shared
            ).count()
        ) == 0

        assert (
            gold_shared.exceptAll(
                silver_shared
            ).count()
        ) == 0

        print(
            "PASS: Silver Inventory attributes "
            "preserved exactly"
        )

        # 8. Validate quantities

        invalid_quantities = gold.filter(
            F.col("stock_quantity").isNull()
            | (F.col("stock_quantity") < 0)
            | F.col("reorder_level").isNull()
            | (F.col("reorder_level") < 0)
        ).count()

        assert invalid_quantities == 0

        print("PASS: Stock quantities and thresholds valid")

        # 9. Independently recalculate stock status

        expected_status = (
            F.when(
                F.col("stock_quantity") == 0,
                F.lit("OUT_OF_STOCK")
            )
            .when(
                F.col("stock_quantity")
                <= F.col("reorder_level"),
                F.lit("REORDER_REQUIRED")
            )
            .otherwise(
                F.lit("SUFFICIENT_STOCK")
            )
        )

        incorrect_statuses = gold.filter(
            ~F.col("stock_status").eqNullSafe(
                expected_status
            )
        ).count()

        assert incorrect_statuses == 0

        assert count_by(
            gold, "stock_status"
        ) == EXPECTED_STOCK

        print(
            "PASS: Stock classifications "
            "and expected counts verified"
        )

        # 10. Validate freshness values and counts

        invalid_freshness = gold.filter(
            F.col("freshness_status").isNull()
            | ~F.col("freshness_status").isin(
                "CURRENT",
                "STALE"
            )
        ).count()

        assert invalid_freshness == 0

        assert count_by(
            gold, "freshness_status"
        ) == EXPECTED_FRESHNESS

        print(
            "PASS: Freshness classifications "
            "and counts verified"
        )

        # 11. Validate update dates and keys

        incorrect_date_keys = gold.filter(
            F.col("last_updated").isNull()
            | F.col("last_updated_date_key").isNull()
            | ~F.col(
                "last_updated_date_key"
            ).eqNullSafe(
                F.date_format(
                    "last_updated",
                    "yyyyMMdd"
                ).cast("int")
            )
        ).count()

        assert incorrect_date_keys == 0

        print(
            "PASS: Inventory update dates "
            "and date keys verified"
        )

        # 12. Validate Product dimension

        dim_products = (
            spark.read.format("delta")
            .load(f"{GOLD_BASE}/dim_products")
        )

        validate_dimension(
            gold,
            dim_products,
            "product_key",
            "product_key",
            "Gold Products"
        )

        # 13. Validate Store dimension

        dim_stores = (
            spark.read.format("delta")
            .load(f"{GOLD_BASE}/dim_stores")
        )

        validate_dimension(
            gold,
            dim_stores,
            "store_key",
            "store_key",
            "Gold Stores"
        )

        # 14. Validate Date dimension

        dim_date = (
            spark.read.format("delta")
            .load(f"{GOLD_BASE}/dim_date")
        )

        validate_dimension(
            gold,
            dim_date,
            "last_updated_date_key",
            "date_key",
            "Gold Date",
            check_source=False
        )

        # 15. Validate lineage

        missing_lineage = gold.filter(
            F.col("ingestion_timestamp").isNull()
            | F.col("batch_id").isNull()
            | (F.trim("batch_id") == "")
            | F.col("source_file").isNull()
            | (F.trim("source_file") == "")
        ).count()

        assert missing_lineage == 0

        print("PASS: Inventory lineage preserved")

        # 16. Validate Delta format and history

        assert DeltaTable.isDeltaTable(
            spark,
            GOLD_PATH
        )

        print("PASS: Gold Inventory is a Delta table")

        print("\n=== GOLD INVENTORY DELTA HISTORY ===")

        DeltaTable.forPath(
            spark,
            GOLD_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        # 17. Summary

        print(
            "\nSUCCESS: ALL GOLD INVENTORY "
            "FACT VALIDATIONS PASSED!"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
