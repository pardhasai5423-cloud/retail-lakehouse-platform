
import os
import sys
from datetime import date, timedelta

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
SILVER_BASE = f"s3a://{BUCKET}/silver"
GOLD_DATE_PATH = f"s3a://{BUCKET}/gold/dim_date"

DATE_SOURCES = [
    ("sales", "transaction_date"),
    ("customers", "registration_date"),
    ("products", "created_date"),
    ("stores", "opened_date"),
    ("inventory", "last_updated"),
]

EXPECTED_START = date(2015, 1, 16)
EXPECTED_END = date(2026, 9, 29)
EXPECTED_DAYS = 4275


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print(
            "\n=== INDEPENDENT GOLD DATE "
            "DIMENSION VALIDATION ==="
        )

        # 1. Read the persisted Delta table
        gold = (
            spark.read.format("delta")
            .load(GOLD_DATE_PATH)
        )

        assert DeltaTable.isDeltaTable(
            spark, GOLD_DATE_PATH
        )
        print("PASS: Gold Date is a Delta table")

        # 2. Validate schema
        expected_types = {
            "date_key": "int",
            "calendar_date": "date",
            "year": "int",
            "quarter": "int",
            "month": "int",
            "month_name": "string",
            "day_of_month": "int",
            "day_of_week": "int",
            "day_name": "string",
            "week_of_year": "int",
            "is_weekend": "boolean",
            "year_month": "string",
        }

        actual_types = dict(gold.dtypes)

        assert actual_types == expected_types, (
            f"Unexpected schema: {actual_types}"
        )
        print("PASS: Date dimension schema verified")

        # 3. Validate row count
        count = gold.count()
        assert count == EXPECTED_DAYS

        print(f"PASS: Calendar contains {count:,} days")

        # 4. Validate keys and dates
        invalid = gold.filter(
            F.col("date_key").isNull()
            | F.col("calendar_date").isNull()
        ).count()

        duplicate_keys = (
            gold.groupBy("date_key")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        duplicate_dates = (
            gold.groupBy("calendar_date")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        assert invalid == 0
        assert duplicate_keys == 0
        assert duplicate_dates == 0

        print("PASS: Dates and keys unique and non-null")

        # 5. Validate date boundaries
        bounds = gold.agg(
            F.min("calendar_date").alias("start"),
            F.max("calendar_date").alias("end")
        ).first()

        assert bounds["start"] == EXPECTED_START
        assert bounds["end"] == EXPECTED_END

        assert (
            EXPECTED_END - EXPECTED_START
        ).days + 1 == EXPECTED_DAYS

        print("PASS: Date boundaries verified")

        # 6. Verify no missing calendar days
        expected_calendar = spark.sql(
            f"""
            SELECT explode(
                sequence(
                    to_date('{EXPECTED_START}'),
                    to_date('{EXPECTED_END}'),
                    interval 1 day
                )
            ) AS calendar_date
            """
        )

        missing_days = expected_calendar.join(
            gold.select("calendar_date"),
            on="calendar_date",
            how="left_anti"
        ).count()

        unexpected_days = (
            gold.select("calendar_date")
            .join(
                expected_calendar,
                on="calendar_date",
                how="left_anti"
            )
            .count()
        )

        assert missing_days == 0
        assert unexpected_days == 0

        print("PASS: No missing or unexpected dates")

        # 7. Validate date key
        incorrect_keys = gold.filter(
            ~F.col("date_key").eqNullSafe(
                F.date_format(
                    "calendar_date", "yyyyMMdd"
                ).cast("int")
            )
        ).count()

        assert incorrect_keys == 0

        print("PASS: Date keys match YYYYMMDD")

        # 8. Validate calendar attributes
        expected_attributes = {
            "year": F.year("calendar_date"),
            "quarter": F.quarter("calendar_date"),
            "month": F.month("calendar_date"),
            "month_name": F.date_format(
                "calendar_date", "MMMM"
            ),
            "day_of_month": F.dayofmonth(
                "calendar_date"
            ),
            "day_of_week": F.dayofweek(
                "calendar_date"
            ),
            "day_name": F.date_format(
                "calendar_date", "EEEE"
            ),
            "week_of_year": F.weekofyear(
                "calendar_date"
            ),
            "is_weekend": F.dayofweek(
                "calendar_date"
            ).isin(1, 7),
            "year_month": F.date_format(
                "calendar_date", "yyyy-MM"
            ),
        }

        for column, expression in expected_attributes.items():
            incorrect = gold.filter(
                ~F.col(column).eqNullSafe(expression)
            ).count()

            assert incorrect == 0, (
                f"Incorrect calendar attribute: {column}"
            )

        print("PASS: All calendar attributes verified")

        # 9. Validate known calendar dates
        examples = [
            (date(2024, 2, 29), False),  # Thursday
            (date(2026, 9, 26), True),   # Saturday
            (date(2026, 9, 27), True),   # Sunday
            (date(2026, 9, 28), False),  # Monday
        ]

        for example_date, expected_weekend in examples:
            # Dates outside the calendar range are skipped.
            if not (
                EXPECTED_START
                <= example_date
                <= EXPECTED_END
            ):
                continue

            record = gold.filter(
                F.col("calendar_date")
                == F.lit(example_date)
            ).first()

            assert record is not None
            assert record["is_weekend"] == expected_weekend

        print("PASS: Known calendar dates verified")

        # 10. Validate source date coverage
        date_keys = gold.select("calendar_date")

        for dataset, date_column in DATE_SOURCES:
            source = (
                spark.read.format("delta")
                .load(f"{SILVER_BASE}/{dataset}")
            )

            source_dates = (
                source.select(
                    F.to_date(
                        F.col(date_column)
                    ).alias("calendar_date")
                )
                .filter(
                    F.col("calendar_date").isNotNull()
                )
                .distinct()
            )

            missing_source_dates = (
                source_dates.join(
                    date_keys,
                    on="calendar_date",
                    how="left_anti"
                )
                .count()
            )

            assert missing_source_dates == 0, (
                f"Missing dates for {dataset}"
            )

            print(
                f"PASS: All {dataset} dates covered"
            )

        # 11. Validate Delta history
        print("\n=== GOLD DATE DELTA HISTORY ===")

        DeltaTable.forPath(
            spark, GOLD_DATE_PATH
        ).history(5).select(
            "version",
            "timestamp",
            "operation"
        ).show(truncate=False)

        print(
            "\nSUCCESS: ALL GOLD DATE "
            "DIMENSION VALIDATIONS PASSED!"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
