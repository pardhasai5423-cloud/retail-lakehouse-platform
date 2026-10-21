"""AWS Glue-compatible Bronze CSV ingestion transformations."""

from pyspark.sql import functions as F

from src.glue.batch_planner import plan_batch
from src.glue.source_schema import load_source_schema


def read_bronze_batch(spark, source_path, source_key):
    """
    Read one incoming CSV with an explicit schema.

    The source key identifies the company and dataset.
    All source columns remain strings in Bronze.
    """

    plan = plan_batch(source_key)
    schema = load_source_schema(plan.company, plan.dataset)

    df = (
        spark.read
        .option("header", "true")
        .option("enforceSchema", "false")
        .option("mode", "FAILFAST")
        .schema(schema)
        .csv(source_path)
    )

    return df, plan


def prepare_bronze_batch(df, plan, batch_id, source_key):
    """
    Attach ingestion metadata to the raw source records.

    This function does not write any data.
    """

    if not isinstance(batch_id, str) or not batch_id.strip():
        raise ValueError("A non-empty batch ID is required")

    return (
        df.withColumn(
            "_source_system",
            F.lit(plan.company),
        )
        .withColumn(
            "_source_dataset",
            F.lit(plan.dataset),
        )
        .withColumn(
            "_source_file",
            F.lit(source_key),
        )
        .withColumn(
            "_batch_id",
            F.lit(batch_id),
        )
        .withColumn(
            "_ingestion_timestamp",
            F.current_timestamp(),
        )
    )
