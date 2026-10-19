
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.config.settings import (
    get_bucket,
    get_region,
    get_aws_profile,
    get_s3_path,
)


BUCKET = get_bucket()
REGION = get_region()

BRONZE_BASE = get_s3_path("bronze")


def create_spark():
    """
    Create SparkSession with Delta Lake and AWS S3 support.
    """

    os.environ["AWS_PROFILE"] = get_aws_profile()

    builder = (
        SparkSession.builder
        .appName("SilverCustomerTransformation")
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


def transform_retailmax_customers(df):
    """
    Standardize RetailMax customer columns.
    """

    return df.select(
        F.lit("retailmax").alias("source_system"),

        F.col("customer_id").alias("source_customer_id"),

        F.concat(
            F.lit("retailmax:"),
            F.col("customer_id")
        ).alias("customer_key"),

        F.trim(F.col("first_name")).alias("first_name"),
        F.trim(F.col("last_name")).alias("last_name"),

        F.lower(
            F.trim(F.col("email"))
        ).alias("email"),

        F.trim(F.col("phone")).alias("phone"),
        F.trim(F.col("city")).alias("city"),
        F.upper(F.trim(F.col("state"))).alias("state"),

        F.to_date(
            F.col("created_at"),
            "yyyy-MM-dd"
        ).alias("registration_date"),

        F.col("_ingestion_timestamp").alias(
            "ingestion_timestamp"
        ),

        F.col("_batch_id").alias("batch_id"),
        F.col("_source_file").alias("source_file")
    )


def transform_freshmart_customers(df):
    """
    Standardize FreshMart customer columns.
    """

    return df.select(
        F.lit("freshmart").alias("source_system"),

        F.col("cust_no").alias("source_customer_id"),

        F.concat(
            F.lit("freshmart:"),
            F.col("cust_no")
        ).alias("customer_key"),

        F.trim(F.col("fname")).alias("first_name"),
        F.trim(F.col("lname")).alias("last_name"),

        F.lower(
            F.trim(F.col("email_address"))
        ).alias("email"),

        F.trim(F.col("mobile_no")).alias("phone"),
        F.trim(F.col("city_name")).alias("city"),
        F.upper(F.trim(F.col("state_code"))).alias("state"),

        F.to_date(
            F.col("signup_date"),
            "MM/dd/yyyy"
        ).alias("registration_date"),

        F.col("_ingestion_timestamp").alias(
            "ingestion_timestamp"
        ),

        F.col("_batch_id").alias("batch_id"),
        F.col("_source_file").alias("source_file")
    )


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== READING CUSTOMER BRONZE TABLES ===")

        retailmax_bronze = (
            spark.read
            .format("delta")
            .load(f"{BRONZE_BASE}/retailmax/customers")
        )

        freshmart_bronze = (
            spark.read
            .format("delta")
            .load(f"{BRONZE_BASE}/freshmart/customers")
        )

        print("\n=== TRANSFORMING RETAILMAX CUSTOMERS ===")

        retailmax_silver = transform_retailmax_customers(
            retailmax_bronze
        )

        print("\n=== TRANSFORMING FRESHMART CUSTOMERS ===")

        freshmart_silver = transform_freshmart_customers(
            freshmart_bronze
        )

        print("\n=== COMBINING CUSTOMER DATA ===")

        unified_customers = retailmax_silver.unionByName(
            freshmart_silver
        )

        print("\n=== UNIFIED CUSTOMER SCHEMA ===")
        unified_customers.printSchema()

        print("\n=== SOURCE RECORD COUNTS ===")

        unified_customers.groupBy(
            "source_system"
        ).count().show()

        print("\n=== SAMPLE STANDARDIZED RECORDS ===")

        unified_customers.select(
            "source_system",
            "source_customer_id",
            "customer_key",
            "email",
            "registration_date"
        ).show(10, truncate=False)

        total = unified_customers.count()

        print(f"\nTotal unified customer records: {total:,}")

        assert total == 9090, (
            f"Expected 9090 customers, got {total}"
        )

        print(
            "\nSUCCESS: CUSTOMER SCHEMA "
            "STANDARDIZATION COMPLETED!"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
