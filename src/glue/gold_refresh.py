"""Gold date dimension and aggregate refresh execution."""

from urllib.parse import urlparse

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.glue.gold_aggregations import AGGREGATION_BUILDERS


AGGREGATION_INPUTS = {
    "agg_daily_sales": ("fact_sales",),
    "agg_store_performance": ("fact_sales", "dim_stores"),
    "agg_category_sales": ("fact_sales", "dim_products"),
    "agg_inventory_status": ("fact_inventory", "dim_products"),
}

PROTECTED_PREFIX = "test-runs/run-001"


def validate_gold_target(path):
    """Validate isolated S3 Gold paths and safe local test paths."""
    if not isinstance(path, str) or not path.strip():
        raise ValueError("Gold target path is required")

    parsed = urlparse(path)

    if parsed.scheme not in ("", "file", "s3", "s3a"):
        raise ValueError("Unsupported Gold target scheme")

    if parsed.scheme in ("s3", "s3a") and not parsed.netloc:
        raise ValueError("S3 bucket is required")

    parts = [part for part in parsed.path.split("/") if part]

    if any(part in (".", "..") for part in parts):
        raise ValueError("Unsafe Gold target path")

    if any(
        parts[i:i + 2] == ["test-runs", "run-001"]
        for i in range(len(parts) - 1)
    ):
        raise ValueError(
            "Protected original dataset cannot be modified"
        )

    if not parts or parts[-2:-1] != ["gold"]:
        raise ValueError(
            "Gold target must be inside a gold/<table> directory"
        )

    if parsed.scheme in ("s3", "s3a"):
        from src.pipeline.gold_safety import (
            APPROVED_GOLD_TARGETS,
        )

        canonical = (
            f"s3a://{parsed.netloc}{parsed.path}"
        )

        if (
            parsed.params
            or parsed.query
            or parsed.fragment
            or canonical not in APPROVED_GOLD_TARGETS
        ):
            raise ValueError(
                "S3 Gold destination is outside the approved "
                "isolated prefix or table allowlist"
            )

    return path


def build_incremental_date_dimension(spark, silver_base):
    """Build dates covering current Silver business dates."""
    if not isinstance(silver_base, str) or not silver_base.strip():
        raise ValueError("Silver base path is required")

    date_sources = {
        "customers": "registration_date",
        "products": "created_date",
        "stores": "opened_date",
        "sales": "transaction_date",
        "inventory": "last_updated",
    }

    boundaries = []

    for dataset, column in date_sources.items():
        path = f"{silver_base.rstrip('/')}/{dataset}"
        df = spark.read.format("delta").load(path)

        dates = df.select(
            F.to_date(F.col(column)).alias("calendar_date")
        ).filter(F.col("calendar_date").isNotNull())

        boundaries.append(dates)

    combined = boundaries[0]

    for dates in boundaries[1:]:
        combined = combined.unionByName(dates)

    bounds = combined.agg(
        F.min("calendar_date").alias("start_date"),
        F.max("calendar_date").alias("end_date"),
    ).first()

    if bounds["start_date"] is None:
        raise ValueError("No valid Silver dates available")

    from src.glue.date_dimension import build_date_dimension

    return build_date_dimension(
        spark,
        bounds["start_date"],
        bounds["end_date"],
    )


def merge_date_dimension(spark, df, target_path):
    """Insert previously unseen calendar dates."""
    validate_gold_target(target_path)

    if "date_key" not in df.columns:
        raise ValueError("Date dimension requires date_key")

    if df.filter(F.col("date_key").isNull()).limit(1).count():
        raise ValueError("Date dimension contains null date keys")

    if (
        df.groupBy("date_key").count()
        .filter(F.col("count") > 1)
        .limit(1).count()
    ):
        raise ValueError("Date dimension contains duplicate date keys")

    if not DeltaTable.isDeltaTable(spark, target_path):
        df.write.format("delta").mode("errorifexists").save(
            target_path
        )
        return {"status": "created", "table": "dim_date"}

    (
        DeltaTable.forPath(spark, target_path)
        .alias("target")
        .merge(
            df.alias("source"),
            "target.date_key = source.date_key",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    return {"status": "merged", "table": "dim_date"}


def execute_date_refresh(
    spark,
    silver_base,
    gold_base,
    *,
    allow_writes=False,
):
    target = f"{gold_base.rstrip('/')}/dim_date"
    validate_gold_target(target)

    if not allow_writes:
        return {
            "status": "dry_run",
            "table": "dim_date",
            "target": target,
        }

    df = build_incremental_date_dimension(spark, silver_base)
    return merge_date_dimension(spark, df, target)


def execute_aggregate_refresh(
    spark,
    table,
    gold_base,
    *,
    allow_writes=False,
):
    """Recalculate an aggregate from current Gold tables."""
    if table not in AGGREGATION_BUILDERS:
        raise ValueError(f"Unsupported Gold aggregate: {table}")

    if not isinstance(gold_base, str) or not gold_base.strip():
        raise ValueError("Gold base path is required")

    base = gold_base.rstrip("/")
    target = f"{base}/{table}"
    validate_gold_target(target)

    inputs = AGGREGATION_INPUTS[table]

    if not allow_writes:
        return {
            "status": "dry_run",
            "table": table,
            "inputs": inputs,
            "target": target,
        }

    dataframes = [
        spark.read.format("delta").load(f"{base}/{name}")
        for name in inputs
    ]

    result = AGGREGATION_BUILDERS[table](*dataframes)

    (
        result.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(target)
    )

    return {"status": "refreshed", "table": table}
