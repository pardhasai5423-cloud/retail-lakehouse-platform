
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

BUCKET = "retail-lakehouse-parsam-2026"
REGION = "us-east-2"

# Use the named AWS CLI profile without embedding credentials.
os.environ["AWS_PROFILE"] = "retail-lakehouse"

builder = (
    SparkSession.builder
    .appName("RetailLakehouseS3ConnectionTest")
    .master("local[2]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    .config(
        "spark.sql.shuffle.partitions",
        "4"
    )
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
)

spark = configure_spark_with_delta_pip(
    builder,
    extra_packages=[
        "org.apache.hadoop:hadoop-aws:3.3.4"
    ]
).getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

try:
    path = f"s3a://{BUCKET}/raw/retailmax/customers.csv"

    print("\n=== TESTING S3 CONNECTION ===")
    print("Reading:", path)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(path)
    )

    print("\n=== SOURCE SCHEMA ===")
    df.printSchema()

    print("\n=== ROW COUNT ===")
    print(df.count())

    print("\n=== SAMPLE RECORDS ===")
    df.show(5, truncate=False)

    print("\nSUCCESS: PySpark can read CSV data from Amazon S3!")

finally:
    spark.stop()
