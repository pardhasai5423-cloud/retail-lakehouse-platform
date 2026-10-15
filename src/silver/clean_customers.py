
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.dirname(__file__))

from transform_customers import (
    create_spark,
    transform_retailmax_customers,
    transform_freshmart_customers,
    BRONZE_BASE,
)


def clean_blank_values(df):
    """
    Convert empty strings into null values for
    customer attributes.
    """

    text_columns = [
        "source_customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "city",
        "state",
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


def calculate_completeness(df):
    """
    Count populated customer attributes.
    Higher scores represent more complete records.
    """

    attributes = [
        "first_name",
        "last_name",
        "email",
        "phone",
        "city",
        "state",
        "registration_date",
    ]

    score = F.lit(0)

    for attribute in attributes:
        score = score + F.when(
            F.col(attribute).isNotNull(),
            1
        ).otherwise(0)

    return df.withColumn(
        "_completeness_score",
        score
    )


def deduplicate_customers(df):
    """
    Keep one record per source system and source customer ID.
    """

    df = calculate_completeness(df)

    # Hash the business attributes to provide a
    # reproducible tie-breaker.
    hash_columns = [
        "source_system",
        "source_customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "city",
        "state",
        "registration_date",
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
            "source_customer_id"
        )
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
        print("\n=== READING BRONZE CUSTOMERS ===")

        retailmax_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/retailmax/customers")
        )

        freshmart_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/freshmart/customers")
        )

        retailmax = transform_retailmax_customers(
            retailmax_bronze
        )

        freshmart = transform_freshmart_customers(
            freshmart_bronze
        )

        customers = retailmax.unionByName(freshmart)

        input_count = customers.count()

        print(f"Input customer records: {input_count:,}")

        print("\n=== CLEANING BLANK VALUES ===")

        cleaned = clean_blank_values(customers)

        print("\n=== DEDUPLICATING CUSTOMERS ===")

        deduplicated = deduplicate_customers(cleaned)

        output_count = deduplicated.count()
        removed_count = input_count - output_count

        print(f"Records after deduplication: {output_count:,}")
        print(f"Duplicate records removed: {removed_count:,}")

        print("\n=== OUTPUT COUNTS BY COMPANY ===")

        deduplicated.groupBy(
            "source_system"
        ).count().show()

        print("\n=== MISSING EMAILS AFTER CLEANING ===")

        deduplicated.groupBy(
            "source_system"
        ).agg(
            F.sum(
                F.when(
                    F.col("email").isNull(),
                    1
                ).otherwise(0)
            ).alias("missing_emails")
        ).show()

        print("\n=== CHECKING REMAINING DUPLICATES ===")

        remaining_duplicates = (
            deduplicated
            .groupBy("source_system", "source_customer_id")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print(
            "Remaining duplicate customer IDs:",
            remaining_duplicates
        )

        assert input_count == 9090
        assert output_count == 9000
        assert removed_count == 90
        assert remaining_duplicates == 0

        print(
            "\nSUCCESS: CUSTOMER CLEANSING "
            "AND DEDUPLICATION COMPLETED!"
        )

        print(
            "\nNOTE: No Silver table has been written yet."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
