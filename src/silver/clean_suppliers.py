
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.insert(0, os.path.dirname(__file__))

from transform_customers import (
    create_spark,
    BRONZE_BASE,
)

from transform_suppliers import (
    transform_retailmax_suppliers,
    transform_freshmart_suppliers,
)


EMAIL_PATTERN = (
    r"^[A-Za-z0-9._%+-]+@"
    r"[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
)


def clean_blank_values(df):
    """
    Normalize supplier text fields and convert
    blank strings to null.
    """

    text_columns = [
        "source_supplier_id",
        "supplier_name",
        "contact_email",
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
            ).otherwise(
                F.trim(F.col(column_name))
            )
        )

    # Normalize email and state values.
    df = df.withColumn(
        "contact_email",
        F.lower(F.col("contact_email"))
    )

    df = df.withColumn(
        "state",
        F.upper(F.col("state"))
    )

    # Rebuild key from the cleaned source ID.
    df = df.withColumn(
        "supplier_key",
        F.concat(
            F.col("source_system"),
            F.lit(":"),
            F.col("source_supplier_id")
        )
    )

    return df


def add_email_quality_status(df):
    """
    Classify supplier email quality without
    discarding records.
    """

    return df.withColumn(
        "email_quality_status",
        F.when(
            F.col("contact_email").isNull(),
            F.lit("MISSING_EMAIL")
        )
        .when(
            ~F.col("contact_email").rlike(
                EMAIL_PATTERN
            ),
            F.lit("INVALID_EMAIL")
        )
        .otherwise(F.lit("VALID"))
    )


def add_validation_status(df):
    """
    Validate mandatory supplier attributes.
    """

    return df.withColumn(
        "validation_status",
        F.when(
            F.col("source_supplier_id").isNull(),
            F.lit("MISSING_SUPPLIER_ID")
        )
        .when(
            F.col("supplier_name").isNull(),
            F.lit("MISSING_SUPPLIER_NAME")
        )
        .otherwise(F.lit("VALID"))
    )


def deduplicate_suppliers(df):
    """
    Retain one record per source system and supplier ID.

    Prefer more complete records, followed by
    the most recent ingestion timestamp.
    """

    completeness_columns = [
        "supplier_name",
        "contact_email",
        "phone",
        "city",
        "state",
    ]

    score = F.lit(0)

    for column_name in completeness_columns:
        score = score + F.when(
            F.col(column_name).isNotNull(),
            1
        ).otherwise(0)

    df = df.withColumn(
        "_completeness_score",
        score
    )

    hash_columns = [
        "source_system",
        "source_supplier_id",
        "supplier_name",
        "contact_email",
        "phone",
        "city",
        "state",
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
            "source_supplier_id"
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
        print("\n=== SUPPLIER CLEANSING PIPELINE ===")

        # Read Bronze Delta tables.
        retailmax_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/retailmax/suppliers")
        )

        freshmart_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/freshmart/suppliers")
        )

        # Standardize both companies.
        retailmax = transform_retailmax_suppliers(
            retailmax_bronze
        )

        freshmart = transform_freshmart_suppliers(
            freshmart_bronze
        )

        unified = retailmax.unionByName(freshmart)

        input_count = unified.count()
        print(f"Input suppliers: {input_count:,}")

        # Clean and validate.
        cleaned = clean_blank_values(unified)

        validated = add_validation_status(
            add_email_quality_status(cleaned)
        )

        print("\n=== VALIDATION STATUS ===")

        validated.groupBy(
            "validation_status"
        ).count().show()

        print("\n=== EMAIL QUALITY STATUS ===")

        validated.groupBy(
            "source_system",
            "email_quality_status"
        ).count().orderBy(
            "source_system",
            "email_quality_status"
        ).show()

        # Separate valid and invalid suppliers.
        valid_suppliers = validated.filter(
            F.col("validation_status") == "VALID"
        )

        quarantine_suppliers = validated.filter(
            F.col("validation_status") != "VALID"
        )

        quarantine_count = quarantine_suppliers.count()

        print(
            f"\nRecords requiring quarantine: "
            f"{quarantine_count}"
        )

        # Deduplicate valid records.
        silver_suppliers = deduplicate_suppliers(
            valid_suppliers
        )

        output_count = silver_suppliers.count()

        print(
            f"Cleaned supplier records: "
            f"{output_count:,}"
        )

        duplicate_count = (
            silver_suppliers
            .groupBy(
                "source_system",
                "source_supplier_id"
            )
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        print(
            f"Remaining duplicate IDs: "
            f"{duplicate_count}"
        )

        print("\n=== FINAL COUNTS BY SOURCE ===")

        silver_suppliers.groupBy(
            "source_system"
        ).count().show()

        # Validate expected development data.
        assert input_count == 180
        assert output_count == 180
        assert quarantine_count == 0
        assert duplicate_count == 0

        print(
            "\nSUCCESS: SUPPLIER CLEANSING "
            "AND VALIDATION COMPLETED!"
        )

        print(
            "\nNOTE: No Silver supplier table "
            "has been written yet."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
