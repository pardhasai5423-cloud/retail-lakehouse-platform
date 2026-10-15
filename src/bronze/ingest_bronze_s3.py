
import os
import uuid
from datetime import datetime, timezone

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    lit,
)

BUCKET = "retail-lakehouse-parsam-2026"
REGION = "us-east-2"

COMPANIES = ["retailmax", "freshmart"]

DATASETS = [
    "customers",
    "products",
    "stores",
    "sales",
    "inventory",
    "suppliers",
]


def create_spark():
    os.environ["AWS_PROFILE"] = "retail-lakehouse"

    builder = (
        SparkSession.builder
        .appName("RetailLakehouseS3BronzeIngestion")
        .master("local[2]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.profile.ProfileCredentialsProvider"
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            f"s3.{REGION}.amazonaws.com"
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            REGION
        )
        .config(
            "spark.delta.logStore.class",
            "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
        )
    )

    return configure_spark_with_delta_pip(
        builder,
        extra_packages=[
            "org.apache.hadoop:hadoop-aws:3.3.4"
        ]
    ).getOrCreate()


def ingest_dataset(spark, company, dataset, batch_id):
    source_path = (
        f"s3a://{BUCKET}/raw/{company}/{dataset}.csv"
    )

    target_path = (
        f"s3a://{BUCKET}/bronze/{company}/{dataset}"
    )

    print(f"\nIngesting {company}.{dataset}")
    print(f"Source: {source_path}")
    print(f"Target: {target_path}")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .csv(source_path)
    )

    df = (
        df.withColumn("_source_system", lit(company))
        .withColumn("_source_dataset", lit(dataset))
        .withColumn("_source_file", input_file_name())
        .withColumn("_batch_id", lit(batch_id))
        .withColumn(
            "_ingestion_timestamp",
            current_timestamp()
        )
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(target_path)
    )

    verified_df = (
        spark.read
        .format("delta")
        .load(target_path)
    )

    count = verified_df.count()

    print(
        f"SUCCESS: {company}.{dataset} "
        f"— {count:,} rows"
    )

    return count


def main():
    batch_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + uuid.uuid4().hex[:8]
    )

    print("=" * 60)
    print("AWS S3 BRONZE INGESTION")
    print("Batch ID:", batch_id)
    print("Bucket:", BUCKET)
    print("=" * 60)

    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    total_records = 0

    try:
        for company in COMPANIES:
            for dataset in DATASETS:
                total_records += ingest_dataset(
                    spark, company, dataset, batch_id
                )

        print("\n" + "=" * 60)
        print("S3 BRONZE INGESTION COMPLETED")
        print(f"Total records: {total_records:,}")
        print("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
