
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.dirname(__file__))

from transform_customers import create_spark, BRONZE_BASE

from transform_stores import (
    transform_retailmax_stores,
    transform_freshmart_stores,
)


ZIP_PATTERN = r"^[0-9]{5}(-[0-9]{4})?$"


def clean_blank_values(df):
    """
    Trim text fields, normalize blanks to null,
    and reconstruct source-qualified store keys.
    """

    text_columns = [
        "source_store_id",
        "store_name",
        "city",
        "state",
        "zip_code",
    ]

    for column_name in text_columns:
        df = df.withColumn(
            column_name,
            F.when(
                F.trim(F.col(column_name)) == "",
                F.lit(None)
            ).otherwise(
                F.trim(F.col(column_name))
            )
        )

    df = df.withColumn(
        "state",
        F.upper(F.col("state"))
    )

    df = df.withColumn(
        "store_key",
        F.when(
            F.col("source_store_id").isNotNull(),
            F.concat(
                F.col("source_system"),
                F.lit(":"),
                F.col("source_store_id")
            )
        )
    )

    return df


def add_zip_quality_status(df):
    """
    Flag missing or malformed ZIP codes.
    """

    return df.withColumn(
        "zip_quality_status",
        F.when(
            F.col("zip_code").isNull(),
            F.lit("MISSING_ZIP")
        )
        .when(
            ~F.col("zip_code").rlike(ZIP_PATTERN),
            F.lit("INVALID_ZIP")
        )
        .otherwise(F.lit("VALID"))
    )


def add_opening_date_status(df):
    """
    Identify missing or future store opening dates.
    """

    return df.withColumn(
        "opening_date_status",
        F.when(
            F.col("opened_date").isNull(),
            F.lit("MISSING_DATE")
        )
        .when(
            F.col("opened_date") > F.current_date(),
            F.lit("FUTURE_DATE_REVIEW")
        )
        .otherwise(F.lit("VALID"))
    )


def add_validation_status(df):
    """
    Check fields required to identify a store
    and its geographic location.
    """

    return df.withColumn(
        "validation_status",
        F.when(
            F.col("source_store_id").isNull(),
            F.lit("MISSING_STORE_ID")
        )
        .when(
            F.col("store_name").isNull(),
            F.lit("MISSING_STORE_NAME")
        )
        .when(
            F.col("city").isNull(),
            F.lit("MISSING_CITY")
        )
        .when(
            F.col("state").isNull(),
            F.lit("MISSING_STATE")
        )
        .otherwise(F.lit("VALID"))
    )


def deduplicate_stores(df):
    """
    Keep one record per source system and store ID.

    Prioritize:
      1. Record completeness
      2. Latest ingestion timestamp
      3. Stable record hash for deterministic ordering
    """

    completeness_columns = [
        "store_name",
        "city",
        "state",
        "zip_code",
        "opened_date",
    ]

    completeness_score = F.lit(0)

    for column_name in completeness_columns:
        completeness_score = (
            completeness_score
            + F.when(
                F.col(column_name).isNotNull(),
                1
            ).otherwise(0)
        )

    df = df.withColumn(
        "_completeness_score",
        completeness_score
    )

    hash_columns = [
        "source_system",
        "source_store_id",
        "store_name",
        "city",
        "state",
        "zip_code",
        "opened_date",
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
            "source_store_id"
        )
        .orderBy(
            F.col("_completeness_score").desc(),
            F.col(
                "ingestion_timestamp"
            ).desc_nulls_last(),
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
        print("\n=== STORE CLEANSING PIPELINE ===")

        # Read source Bronze tables.
        retailmax_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/retailmax/stores")
        )

        freshmart_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/freshmart/stores")
        )

        # Standardize source schemas.
        retailmax = transform_retailmax_stores(
            retailmax_bronze
        )

        freshmart = transform_freshmart_stores(
            freshmart_bronze
        )

        unified = retailmax.unionByName(freshmart)

        input_count = unified.count()

        print(f"Input stores: {input_count:,}")

        # Clean and validate.
        cleaned = clean_blank_values(unified)

        validated = add_validation_status(
            add_opening_date_status(
                add_zip_quality_status(cleaned)
            )
        )

        print("\n=== VALIDATION STATUS ===")

        validated.groupBy(
            "validation_status"
        ).count().show()

        print("\n=== ZIP QUALITY STATUS ===")

        validated.groupBy(
            "source_system",
            "zip_quality_status"
        ).count().orderBy(
            "source_system"
        ).show()

        print("\n=== OPENING DATE STATUS ===")

        validated.groupBy(
            "source_system",
            "opening_date_status"
        ).count().orderBy(
            "source_system"
        ).show()

        # Separate valid and invalid records.
        valid_stores = validated.filter(
            F.col("validation_status") == "VALID"
        )

        quarantine_stores = validated.filter(
            F.col("validation_status") != "VALID"
        )

        quarantine_count = quarantine_stores.count()

        print(
            f"\nRecords requiring quarantine: "
            f"{quarantine_count}"
        )

        # Deduplicate valid records.
        silver_stores = deduplicate_stores(
            valid_stores
        )

        output_count = silver_stores.count()

        print(
            f"Cleaned store records: "
            f"{output_count:,}"
        )

        duplicate_count = (
            silver_stores
            .groupBy(
                "source_system",
                "source_store_id"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print(
            f"Remaining duplicate store IDs: "
            f"{duplicate_count}"
        )

        print("\n=== FINAL COUNTS BY SOURCE ===")

        silver_stores.groupBy(
            "source_system"
        ).count().show()

        # Assertions specific to our development dataset.
        assert input_count == 90
        assert output_count == 90
        assert quarantine_count == 0
        assert duplicate_count == 0

        print(
            "\nSUCCESS: STORE CLEANSING "
            "AND VALIDATION COMPLETED!"
        )

        print(
            "\nNOTE: No Silver Stores table "
            "has been written yet."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
