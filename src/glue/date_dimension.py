"""Reusable date dimension generation for AWS Glue."""

from pyspark.sql import functions as F


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
