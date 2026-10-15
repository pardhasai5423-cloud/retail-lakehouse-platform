
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.dirname(__file__))

from transform_customers import (
    create_spark,
    BRONZE_BASE,
)

from transform_products import (
    transform_retailmax_products,
    transform_freshmart_products,
)


def clean_blank_values(df):
    """
    Convert empty strings to null and normalize
    product attributes.
    """

    text_columns = [
        "source_product_id",
        "product_name",
        "category",
        "brand",
        "source_supplier_id",
    ]

    for column_name in text_columns:
        df = df.withColumn(
            column_name,
            F.when(
                F.trim(F.col(column_name)) == "",
                F.lit(None)
            ).otherwise(F.trim(F.col(column_name)))
        )

    return df


def add_price_quality_status(df):
    """
    Assign a quality status without inventing prices.
    """

    return df.withColumn(
        "price_quality_status",
        F.when(
            F.col("unit_price").isNull(),
            F.lit("MISSING_PRICE")
        )
        .when(
            F.col("unit_price") < 0,
            F.lit("INVALID_NEGATIVE_PRICE")
        )
        .when(
            F.col("unit_price") == 0,
            F.lit("ZERO_PRICE_REVIEW")
        )
        .otherwise(F.lit("VALID"))
    )


def add_validation_status(df):
    """
    Flag records that violate mandatory business rules.
    """

    return df.withColumn(
        "validation_status",
        F.when(
            F.col("source_product_id").isNull(),
            F.lit("MISSING_PRODUCT_ID")
        )
        .when(
            F.col("product_name").isNull(),
            F.lit("MISSING_PRODUCT_NAME")
        )
        .when(
            F.col("unit_price") < 0,
            F.lit("NEGATIVE_PRICE")
        )
        .otherwise(F.lit("VALID"))
    )


def deduplicate_products(df):
    """
    Retain one record per source system and product ID.
    Prefer complete records, then latest ingestion.
    """

    completeness_columns = [
        "product_name",
        "category",
        "brand",
        "unit_price",
        "source_supplier_id",
        "created_date",
    ]

    score = F.lit(0)

    for column_name in completeness_columns:
        score = score + F.when(
            F.col(column_name).isNotNull(),
            1
        ).otherwise(0)

    df = df.withColumn("_completeness_score", score)

    hash_columns = [
        "source_system",
        "source_product_id",
        "product_name",
        "category",
        "brand",
        "unit_price",
        "source_supplier_id",
        "created_date",
    ]

    df = df.withColumn(
        "_record_hash",
        F.sha2(
            F.to_json(
                F.struct(
                    *[F.col(c) for c in hash_columns]
                ),
                options={"ignoreNullFields": "false"}
            ),
            256
        )
    )

    window_spec = (
        Window
        .partitionBy("source_system", "source_product_id")
        .orderBy(
            F.col("_completeness_score").desc(),
            F.col("ingestion_timestamp").desc_nulls_last(),
            F.col("_record_hash").asc(),
            F.col("source_file").asc_nulls_last()
        )
    )

    return (
        df.withColumn(
            "_row_number",
            F.row_number().over(window_spec)
        )
        .filter(F.col("_row_number") == 1)
        .drop(
            "_row_number",
            "_completeness_score",
            "_record_hash"
        )
    )


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== PRODUCT CLEANSING PIPELINE ===")

        retailmax_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/retailmax/products")
        )

        freshmart_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/freshmart/products")
        )

        retailmax = transform_retailmax_products(
            retailmax_bronze
        )

        freshmart = transform_freshmart_products(
            freshmart_bronze
        )

        unified = retailmax.unionByName(freshmart)

        print(f"Input products: {unified.count():,}")

        cleaned = clean_blank_values(unified)

        # Add quality and validation statuses.
        validated = add_validation_status(
            add_price_quality_status(cleaned)
        )

        # Separate valid and invalid records.
        valid_products = validated.filter(
            F.col("validation_status") == "VALID"
        )

        quarantine_products = validated.filter(
            F.col("validation_status") != "VALID"
        )

        print("\n=== VALIDATION RESULTS ===")

        validated.groupBy(
            "validation_status"
        ).count().show()

        print("\n=== PRICE QUALITY RESULTS ===")

        validated.groupBy(
            "source_system",
            "price_quality_status"
        ).count().orderBy(
            "source_system",
            "price_quality_status"
        ).show()

        print("\n=== QUARANTINE COUNT ===")

        quarantine_count = quarantine_products.count()

        print(f"Records requiring quarantine: {quarantine_count}")

        print("\n=== DEDUPLICATING VALID PRODUCTS ===")

        silver_products = deduplicate_products(valid_products)

        output_count = silver_products.count()

        print(f"Cleaned product records: {output_count:,}")

        duplicate_count = (
            silver_products
            .groupBy("source_system", "source_product_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print(f"Remaining duplicate IDs: {duplicate_count}")

        print("\n=== FINAL COUNTS BY SOURCE ===")

        silver_products.groupBy(
            "source_system"
        ).count().show()

        assert unified.count() == 1800
        assert output_count == 1800
        assert quarantine_count == 0
        assert duplicate_count == 0

        print(
            "\nSUCCESS: PRODUCT CLEANSING "
            "AND VALIDATION COMPLETED!"
        )

        print(
            "\nNOTE: No Silver table has been written yet."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
