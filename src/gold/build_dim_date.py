from src.config.write_safety import require_new_s3_target

import os
import sys

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from src.config.settings import get_s3_path


PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)

sys.path.insert(
    0,
    os.path.join(PROJECT_ROOT, "src", "silver")
)

from transform_customers import create_spark
from src.config.settings import require_safe_write_environment


SILVER_BASE = get_s3_path("silver")

GOLD_DATE_PATH = get_s3_path(
    "gold", "dim_date"
)


DATE_SOURCES = [
    ("sales", "transaction_date"),
    ("customers", "registration_date"),
    ("products", "created_date"),
    ("stores", "opened_date"),
    ("inventory", "last_updated"),
]


def get_date_boundaries(spark):
    """
    Find the minimum and maximum dates across
    all relevant Silver datasets.
    """

    min_dates = []
    max_dates = []

    for dataset, column in DATE_SOURCES:
        df = (
            spark.read.format("delta")
            .load(f"{SILVER_BASE}/{dataset}")
        )

        boundaries = (
            df.select(
                F.min(
                    F.to_date(F.col(column))
                ).alias("min_date"),
                F.max(
                    F.to_date(F.col(column))
                ).alias("max_date")
            )
            .first()
        )

        if boundaries["min_date"] is not None:
            min_dates.append(boundaries["min_date"])

        if boundaries["max_date"] is not None:
            max_dates.append(boundaries["max_date"])

        print(
            f"{dataset}: "
            f"{boundaries['min_date']} "
            f"to {boundaries['max_date']}"
        )

    assert min_dates, "No valid source dates found"
    assert max_dates, "No valid source dates found"

    return min(min_dates), max(max_dates)


def build_date_dimension(spark, start_date, end_date):
    """
    Generate one calendar row per day.
    """

    dates = spark.sql(
        f"""
        SELECT explode(
            sequence(
                to_date('{start_date}'),
                to_date('{end_date}'),
                interval 1 day
            )
        ) AS calendar_date
        """
    )

    return (
        dates
        .withColumn(
            "date_key",
            F.date_format(
                "calendar_date", "yyyyMMdd"
            ).cast("int")
        )
        .withColumn(
            "year",
            F.year("calendar_date")
        )
        .withColumn(
            "quarter",
            F.quarter("calendar_date")
        )
        .withColumn(
            "month",
            F.month("calendar_date")
        )
        .withColumn(
            "month_name",
            F.date_format(
                "calendar_date", "MMMM"
            )
        )
        .withColumn(
            "day_of_month",
            F.dayofmonth("calendar_date")
        )
        .withColumn(
            "day_of_week",
            F.dayofweek("calendar_date")
        )
        .withColumn(
            "day_name",
            F.date_format(
                "calendar_date", "EEEE"
            )
        )
        .withColumn(
            "week_of_year",
            F.weekofyear("calendar_date")
        )
        .withColumn(
            "is_weekend",
            F.dayofweek("calendar_date").isin(1, 7)
        )
        .withColumn(
            "year_month",
            F.date_format(
                "calendar_date", "yyyy-MM"
            )
        )
        .select(
            "date_key",
            "calendar_date",
            "year",
            "quarter",
            "month",
            "month_name",
            "day_of_month",
            "day_of_week",
            "day_name",
            "week_of_year",
            "is_weekend",
            "year_month"
        )
    )


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(
            "\n=== GOLD DATE DIMENSION PIPELINE ==="
        )

        # 1. Discover source date boundaries

        print("\n=== SOURCE DATE RANGES ===")

        start_date, end_date = get_date_boundaries(
            spark
        )

        print(
            f"\nOverall date range: "
            f"{start_date} to {end_date}"
        )

        # 2. Generate calendar

        dim_date = build_date_dimension(
            spark,
            start_date,
            end_date
        )

        expected_days = (
            end_date - start_date
        ).days + 1

        actual_days = dim_date.count()

        assert actual_days == expected_days

        print(
            f"Calendar days generated: "
            f"{actual_days:,}"
        )

        # 3. Validate date keys

        invalid_keys = dim_date.filter(
            F.col("date_key").isNull()
        ).count()

        duplicate_keys = (
            dim_date
            .groupBy("date_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert invalid_keys == 0
        assert duplicate_keys == 0

        print(
            "PASS: Date keys unique and non-null"
        )

        # 4. Validate calendar date uniqueness

        duplicate_dates = (
            dim_date
            .groupBy("calendar_date")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert duplicate_dates == 0

        print("PASS: Calendar dates unique")

        # 5. Validate boundaries

        actual_bounds = dim_date.agg(
            F.min("calendar_date").alias("min_date"),
            F.max("calendar_date").alias("max_date")
        ).first()

        assert actual_bounds["min_date"] == start_date
        assert actual_bounds["max_date"] == end_date

        print("PASS: Calendar boundaries verified")

        # 6. Validate weekend classification

        incorrect_weekends = dim_date.filter(
            ~F.col("is_weekend").eqNullSafe(
                F.col("day_of_week").isin(1, 7)
            )
        ).count()

        assert incorrect_weekends == 0

        print("PASS: Weekend classification verified")

        # 7. Write Gold Delta table

        print("\n=== WRITING GOLD DIM_DATE ===")

        require_new_s3_target(spark, GOLD_DATE_PATH)
        (
            dim_date.write
            .format("delta")
            .mode("errorifexists")
            .option("overwriteSchema", "true")
            .save(GOLD_DATE_PATH)
        )

        print("PASS: Gold Date dimension written")

        # 8. Verify persisted Delta table

        assert DeltaTable.isDeltaTable(
            spark,
            GOLD_DATE_PATH
        )

        persisted = (
            spark.read.format("delta")
            .load(GOLD_DATE_PATH)
        )

        assert persisted.count() == expected_days

        print(
            f"PASS: Persisted count = "
            f"{expected_days:,}"
        )

        # 9. Display schema

        print("\n=== GOLD DATE SCHEMA ===")
        persisted.printSchema()

        # 10. Display sample dates

        print("\n=== SAMPLE CALENDAR DATES ===")

        persisted.orderBy(
            "calendar_date"
        ).show(10, truncate=False)

        # 11. Display Delta history

        print("\n=== DELTA HISTORY ===")

        DeltaTable.forPath(
            spark,
            GOLD_DATE_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: GOLD DIM_DATE CREATED!"
        )

        print(
            f"Gold path: {GOLD_DATE_PATH}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    require_safe_write_environment()
    main()
