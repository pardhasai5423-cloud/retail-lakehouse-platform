
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.dirname(__file__))

from transform_customers import (
    create_spark,
    BRONZE_BASE,
)

from transform_sales import (
    transform_retailmax_sales,
    transform_freshmart_sales,
)


BUCKET = "retail-lakehouse-parsam-2026"
SILVER_BASE = f"s3a://{BUCKET}/silver"


def clean_identifiers(df):
    """
    Normalize source IDs and rebuild composite keys.
    Blank identifiers become null.
    """

    key_pairs = [
        ("source_transaction_id", "transaction_key"),
        ("source_customer_id", "customer_key"),
        ("source_product_id", "product_key"),
        ("source_store_id", "store_key"),
    ]

    for source_column, key_column in key_pairs:

        df = df.withColumn(
            source_column,
            F.when(
                F.trim(F.col(source_column)) == "",
                F.lit(None)
            ).otherwise(
                F.trim(F.col(source_column))
            )
        )

        df = df.withColumn(
            key_column,
            F.when(
                F.col(source_column).isNotNull(),
                F.concat(
                    F.col("source_system"),
                    F.lit(":"),
                    F.col(source_column)
                )
            )
        )

    return df


def deduplicate_sales(df):
    """
    Deduplicate by source-qualified transaction ID.

    Prefer more complete records, then more recent
    ingestion timestamps, then a stable hash.
    """

    completeness_columns = [
        "source_customer_id",
        "source_product_id",
        "source_store_id",
        "transaction_date",
        "quantity",
        "unit_price",
        "discount_amount",
        "total_amount",
    ]

    completeness = F.lit(0)

    for column_name in completeness_columns:
        completeness = completeness + F.when(
            F.col(column_name).isNotNull(),
            1
        ).otherwise(0)

    df = df.withColumn(
        "_completeness",
        completeness
    )

    hash_columns = [
        "source_system",
        "source_transaction_id",
        "source_customer_id",
        "source_product_id",
        "source_store_id",
        "transaction_date",
        "quantity",
        "unit_price",
        "discount_amount",
        "total_amount",
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
        .partitionBy(
            "source_system",
            "source_transaction_id"
        )
        .orderBy(
            F.col("_completeness").desc(),
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
            "_completeness",
            "_record_hash"
        )
    )


def add_reference_status(
    df,
    reference_df,
    key_column,
    status_column,
):
    """
    Add a match/missing status without changing
    transaction counts.
    """

    reference_keys = (
        reference_df
        .select(key_column)
        .distinct()
        .withColumn(
            "_reference_exists",
            F.lit(1)
        )
    )

    joined = df.join(
        F.broadcast(reference_keys),
        on=key_column,
        how="left"
    )

    return (
        joined.withColumn(
            status_column,
            F.when(
                F.col("_reference_exists").isNull(),
                F.lit("MISSING_REFERENCE")
            ).otherwise(F.lit("MATCHED"))
        )
        .drop("_reference_exists")
    )


def add_quality_flags(df):
    """
    Record every detected issue.

    quality_issues is an array so a transaction
    can have more than one quality problem.
    """

    gross = (
        F.col("quantity").cast("decimal(18,2)")
        * F.col("unit_price")
    )

    expected_total = (
        gross - F.col("discount_amount")
    )

    total_difference = F.abs(
        F.col("total_amount") - expected_total
    )

    flags = [
        (
            F.col("source_transaction_id").isNull(),
            "MISSING_TRANSACTION_ID"
        ),
        (
            F.col("source_customer_id").isNull(),
            "MISSING_CUSTOMER_ID"
        ),
        (
            F.col("source_product_id").isNull(),
            "MISSING_PRODUCT_ID"
        ),
        (
            F.col("source_store_id").isNull(),
            "MISSING_STORE_ID"
        ),
        (
            F.col("transaction_date").isNull(),
            "MISSING_TRANSACTION_DATE"
        ),
        (
            F.col("transaction_date") > F.current_date(),
            "FUTURE_TRANSACTION_DATE"
        ),
        (
            F.col("quantity").isNull(),
            "MISSING_QUANTITY"
        ),
        (
            F.col("quantity") == 0,
            "ZERO_QUANTITY"
        ),
        (
            F.col("quantity") < 0,
            "NEGATIVE_QUANTITY"
        ),
        (
            F.col("unit_price").isNull(),
            "MISSING_UNIT_PRICE"
        ),
        (
            F.col("unit_price") < 0,
            "NEGATIVE_UNIT_PRICE"
        ),
        (
            F.col("unit_price") == 0,
            "ZERO_UNIT_PRICE_REVIEW"
        ),
        (
            F.col("discount_amount").isNull(),
            "MISSING_DISCOUNT"
        ),
        (
            F.col("discount_amount") < 0,
            "NEGATIVE_DISCOUNT"
        ),
        (
            F.col("total_amount").isNull(),
            "MISSING_TOTAL_AMOUNT"
        ),
        (
            F.col("total_amount") < 0,
            "NEGATIVE_TOTAL_AMOUNT"
        ),
        (
            (
                (F.col("quantity") > 0)
                & (F.col("unit_price") >= 0)
                & (F.col("discount_amount") > gross)
            ),
            "DISCOUNT_EXCEEDS_GROSS"
        ),
        (
            (
                F.col("quantity").isNotNull()
                & F.col("unit_price").isNotNull()
                & F.col("discount_amount").isNotNull()
                & F.col("total_amount").isNotNull()
                & (total_difference > F.lit(0.01))
            ),
            "TOTAL_AMOUNT_MISMATCH"
        ),
        (
            F.col("customer_reference_status")
            == "MISSING_REFERENCE",
            "INVALID_CUSTOMER_REFERENCE"
        ),
        (
            F.col("product_reference_status")
            == "MISSING_REFERENCE",
            "INVALID_PRODUCT_REFERENCE"
        ),
        (
            F.col("store_reference_status")
            == "MISSING_REFERENCE",
            "INVALID_STORE_REFERENCE"
        ),
    ]

    issue_columns = [
        F.when(
            condition,
            F.lit(issue_name)
        )
        for condition, issue_name in flags
    ]

    df = df.withColumn(
        "quality_issues",
        F.array_compact(F.array(*issue_columns))
    )

    df = df.withColumn(
        "validation_status",
        F.when(
            F.size(F.col("quality_issues")) == 0,
            F.lit("VALID")
        ).otherwise(F.lit("QUARANTINE"))
    )

    return df


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== SALES CLEANSING PIPELINE ===")

        # --------------------------------
        # 1. Read Bronze Sales
        # --------------------------------

        retailmax_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/retailmax/sales")
        )

        freshmart_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/freshmart/sales")
        )

        # --------------------------------
        # 2. Standardize source schemas
        # --------------------------------

        retailmax = transform_retailmax_sales(
            retailmax_bronze
        )

        freshmart = transform_freshmart_sales(
            freshmart_bronze
        )

        unified = retailmax.unionByName(freshmart)

        input_count = unified.count()

        print(f"Input Sales records: {input_count:,}")

        assert input_count == 90000

        # --------------------------------
        # 3. Clean identifiers
        # --------------------------------

        cleaned = clean_identifiers(unified)

        # --------------------------------
        # 4. Deduplicate transactions
        # --------------------------------

        deduplicated = deduplicate_sales(
            cleaned
        )

        deduplicated_count = deduplicated.count()

        print(
            f"Records after deduplication: "
            f"{deduplicated_count:,}"
        )

        assert deduplicated_count == 90000

        # --------------------------------
        # 5. Read Silver master tables
        # --------------------------------

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

        # --------------------------------
        # 6. Check reference integrity
        # --------------------------------

        referenced = add_reference_status(
            deduplicated,
            customers,
            "customer_key",
            "customer_reference_status"
        )

        referenced = add_reference_status(
            referenced,
            products,
            "product_key",
            "product_reference_status"
        )

        referenced = add_reference_status(
            referenced,
            stores,
            "store_key",
            "store_reference_status"
        )

        # --------------------------------
        # 7. Add quality flags
        # --------------------------------

        classified = add_quality_flags(
            referenced
        ).cache()

        print("\n=== VALIDATION STATUS ===")

        classified.groupBy(
            "validation_status"
        ).count().show()

        # --------------------------------
        # 8. Issue-level reporting
        # --------------------------------

        print("\n=== QUALITY ISSUES BY TYPE ===")

        (
            classified
            .select(
                "source_system",
                F.explode("quality_issues").alias("issue")
            )
            .groupBy(
                "source_system",
                "issue"
            )
            .count()
            .orderBy(
                "source_system",
                "issue"
            )
            .show(100, truncate=False)
        )

        # --------------------------------
        # 9. Split valid and quarantine
        # --------------------------------

        valid_sales = classified.filter(
            F.col("validation_status") == "VALID"
        )

        quarantine_sales = classified.filter(
            F.col("validation_status") == "QUARANTINE"
        )

        valid_count = valid_sales.count()
        quarantine_count = quarantine_sales.count()

        print("\n=== CLEANSING SUMMARY ===")

        print(f"Input records: {input_count:,}")
        print(f"Valid records: {valid_count:,}")
        print(
            f"Quarantined records: "
            f"{quarantine_count:,}"
        )

        assert (
            valid_count + quarantine_count
            == deduplicated_count
        )

        print("\n=== VALID SALES BY SOURCE ===")

        valid_sales.groupBy(
            "source_system"
        ).count().show()

        print("\n=== QUARANTINED SALES BY SOURCE ===")

        quarantine_sales.groupBy(
            "source_system"
        ).count().show()

        print("\n=== SAMPLE QUARANTINED TRANSACTIONS ===")

        quarantine_sales.select(
            "source_system",
            "source_transaction_id",
            "transaction_date",
            "quantity",
            "product_key",
            "total_amount",
            "quality_issues"
        ).show(20, truncate=False)

        # --------------------------------
        # 10. Final checks
        # --------------------------------

        invalid_valid_rows = valid_sales.filter(
            F.size(F.col("quality_issues")) > 0
        ).count()

        assert invalid_valid_rows == 0

        print(
            "\nSUCCESS: SALES CLEANSING "
            "AND QUARANTINE CLASSIFICATION COMPLETED!"
        )

        print(
            "\nNOTE: No Silver Sales or "
            "quarantine table has been written yet."
        )

        classified.unpersist()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
