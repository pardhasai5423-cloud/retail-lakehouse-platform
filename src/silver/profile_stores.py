
import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(__file__))

from transform_customers import (
    create_spark,
    BRONZE_BASE,
)

from transform_stores import (
    transform_retailmax_stores,
    transform_freshmart_stores,
)


def missing(column_name):
    """
    Detect null or blank string values.
    """
    return (
        F.col(column_name).isNull()
        | (F.trim(F.col(column_name)) == "")
    )


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== STORE DATA QUALITY PROFILING ===")

        # 1. Read Bronze Delta tables from S3.
        retailmax_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/retailmax/stores")
        )

        freshmart_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/freshmart/stores")
        )

        # 2. Standardize source schemas.
        retailmax = transform_retailmax_stores(
            retailmax_bronze
        )

        freshmart = transform_freshmart_stores(
            freshmart_bronze
        )

        stores = retailmax.unionByName(freshmart)

        print("\n=== 1. STORE COUNTS ===")

        stores.groupBy("source_system").count().show()

        # ------------------------------------------
        # 3. Duplicate store IDs
        # ------------------------------------------

        print("\n=== 2. DUPLICATE STORE IDS ===")

        duplicates = (
            stores
            .groupBy(
                "source_system",
                "source_store_id"
            )
            .count()
            .filter(F.col("count") > 1)
        )

        duplicates.show(20, truncate=False)

        print(
            "Duplicate store ID groups:",
            duplicates.count()
        )

        # ------------------------------------------
        # 4. Missing mandatory fields
        # ------------------------------------------

        print("\n=== 3. MISSING STORE ATTRIBUTES ===")

        stores.groupBy("source_system").agg(
            F.sum(
                F.when(
                    missing("source_store_id"), 1
                ).otherwise(0)
            ).alias("missing_store_ids"),

            F.sum(
                F.when(
                    missing("store_name"), 1
                ).otherwise(0)
            ).alias("missing_store_names"),

            F.sum(
                F.when(
                    missing("city"), 1
                ).otherwise(0)
            ).alias("missing_cities"),

            F.sum(
                F.when(
                    missing("state"), 1
                ).otherwise(0)
            ).alias("missing_states"),

            F.sum(
                F.when(
                    missing("zip_code"), 1
                ).otherwise(0)
            ).alias("missing_zip_codes")
        ).show()

        # ------------------------------------------
        # 5. ZIP code quality
        # ------------------------------------------

        print("\n=== 4. ZIP CODE FORMAT QUALITY ===")

        # Accept 5-digit ZIP or ZIP+4.
        zip_pattern = r"^[0-9]{5}(-[0-9]{4})?$"

        stores.groupBy("source_system").agg(
            F.sum(
                F.when(
                    ~missing("zip_code")
                    & ~F.col("zip_code").rlike(
                        zip_pattern
                    ),
                    1
                ).otherwise(0)
            ).alias("invalid_zip_formats")
        ).show()

        print("\n=== SAMPLE INVALID ZIP CODES ===")

        stores.filter(
            ~missing("zip_code")
            & ~F.col("zip_code").rlike(zip_pattern)
        ).select(
            "source_system",
            "source_store_id",
            "zip_code"
        ).show(15, truncate=False)

        # ------------------------------------------
        # 6. Opening date quality
        # ------------------------------------------

        print("\n=== 5. OPENING DATE QUALITY ===")

        stores.groupBy("source_system").agg(
            F.sum(
                F.when(
                    F.col("opened_date").isNull(),
                    1
                ).otherwise(0)
            ).alias("null_opening_dates"),

            F.sum(
                F.when(
                    F.col("opened_date")
                    > F.current_date(),
                    1
                ).otherwise(0)
            ).alias("future_opening_dates")
        ).show()

        print("\n=== SAMPLE FUTURE OPENING DATES ===")

        stores.filter(
            F.col("opened_date") > F.current_date()
        ).select(
            "source_system",
            "source_store_id",
            "store_name",
            "opened_date"
        ).show(15, truncate=False)

        # ------------------------------------------
        # 7. State code quality
        # ------------------------------------------

        print("\n=== 6. STATE CODE FORMAT QUALITY ===")

        stores.groupBy("source_system").agg(
            F.sum(
                F.when(
                    ~missing("state")
                    & ~F.col("state").rlike(
                        r"^[A-Z]{2}$"
                    ),
                    1
                ).otherwise(0)
            ).alias("invalid_state_formats")
        ).show()

        print("\n=== DISTINCT STATE CODES ===")

        stores.select("state").distinct().orderBy(
            "state"
        ).show(100, truncate=False)

        # ------------------------------------------
        # 8. Potential cross-company location overlap
        # ------------------------------------------

        print("\n=== 7. CROSS-COMPANY SHARED LOCATIONS ===")

        normalized = (
            stores
            .withColumn(
                "normalized_city",
                F.lower(
                    F.trim(F.col("city"))
                )
            )
            .withColumn(
                "normalized_zip",
                F.trim(F.col("zip_code"))
            )
        )

        shared_locations = (
            normalized
            .filter(
                ~missing("normalized_city")
                & ~missing("normalized_zip")
            )
            .groupBy(
                "normalized_city",
                "state",
                "normalized_zip"
            )
            .agg(
                F.countDistinct(
                    "source_system"
                ).alias("source_count"),

                F.count("*").alias(
                    "store_records"
                )
            )
            .filter(F.col("source_count") > 1)
        )

        print(
            "Shared city/state/ZIP combinations:",
            shared_locations.count()
        )

        shared_locations.show(
            20, truncate=False
        )

        print("\n=== PROFILING COMPLETED ===")

        print(
            "\nNOTE: No Silver Stores table "
            "has been written yet."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
