"""Safe local execution preview for incremental Bronze ingestion."""

from pathlib import Path

from src.glue.jobs.run_bronze import build_job_plan
from src.glue.jobs.bronze_ingestion import (
    read_bronze_batch,
    prepare_bronze_batch,
)


def preview_bronze_ingestion(
    spark,
    local_csv_path,
    source_key,
    version_id=None,
    etag=None,
):
    """
    Validate and transform one local CSV batch.

    This function performs no Delta or S3 writes.
    """

    path = Path(local_csv_path).resolve(strict=True)

    if not path.is_file():
        raise ValueError("Input must be a local CSV file")

    plan = build_job_plan(
        source_key,
        version_id=version_id,
        etag=etag,
    )

    df, batch_plan = read_bronze_batch(
        spark,
        str(path),
        source_key,
    )

    bronze_df = prepare_bronze_batch(
        df,
        batch_plan,
        plan["batch_id"],
        source_key,
    )

    # Force Spark to parse every source column.
    # A simple count() may skip CSV field validation.
    from pyspark.sql import functions as F

    source_columns = [
        field.name
        for field in bronze_df.schema.fields
        if not field.name.startswith("_")
    ]

    validated_df = bronze_df.select(
        F.to_json(
            F.struct(
                *[F.col(name) for name in source_columns]
            )
        ).alias("_validated_record")
    )

    row_count = validated_df.rdd.count()

    return {
        "batch_id": plan["batch_id"],
        "company": plan["company"],
        "dataset": plan["dataset"],
        "row_count": row_count,
        "columns": bronze_df.columns,
        "target": plan["bronze_target"],
        "dataframe": bronze_df,
    }
