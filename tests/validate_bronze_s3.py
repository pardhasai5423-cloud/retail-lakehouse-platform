
import os

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

BUCKET = "retail-lakehouse-parsam-2026"
REGION = "us-east-2"

EXPECTED_COUNTS = {
    "retailmax": {
        "customers": 5050,
        "products": 1000,
        "stores": 50,
        "sales": 50000,
        "inventory": 2000,
        "suppliers": 100,
    },
    "freshmart": {
        "customers": 4040,
        "products": 800,
        "stores": 40,
        "sales": 40000,
        "inventory": 1600,
        "suppliers": 80,
    },
}

METADATA_COLUMNS = [
    "_source_system",
    "_source_dataset",
    "_source_file",
    "_batch_id",
    "_ingestion_timestamp",
]


def create_spark():
    os.environ["AWS_PROFILE"] = "retail-lakehouse"

    builder = (
        SparkSession.builder
        .appName("ValidateS3Bronze")
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


def read_table(spark, company, dataset):
    path = f"s3a://{BUCKET}/bronze/{company}/{dataset}"
    return spark.read.format("delta").load(path)


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        total = 0
        batch_ids = set()

        print("\n=== VALIDATING S3 BRONZE TABLES ===")

        for company, datasets in EXPECTED_COUNTS.items():
            for dataset, expected in datasets.items():
                df = read_table(spark, company, dataset)
                actual = df.count()

                assert actual == expected, (
                    f"{company}.{dataset}: "
                    f"expected {expected}, got {actual}"
                )

                for name in METADATA_COLUMNS:
                    assert name in df.columns, (
                        f"{company}.{dataset}: missing {name}"
                    )

                    assert df.filter(col(name).isNull()).count() == 0, (
                        f"{company}.{dataset}: null {name}"
                    )

                assert df.filter(
                    col("_source_system") != company
                ).count() == 0

                assert df.filter(
                    col("_source_dataset") != dataset
                ).count() == 0

                ids = [
                    row["_batch_id"]
                    for row in df.select("_batch_id").distinct().collect()
                ]

                assert len(ids) == 1, (
                    f"{company}.{dataset}: multiple batch IDs"
                )

                batch_ids.add(ids[0])
                total += actual

                print(
                    f"PASS: {company}.{dataset} "
                    f"({actual:,} rows)"
                )

        assert total == 104760, (
            f"Expected 104760 total rows, got {total}"
        )

        print(f"\nPASS: Total Bronze records = {total:,}")

        # The full ingestion run should use one batch ID.
        assert len(batch_ids) == 1, (
            f"Expected one batch ID, found {len(batch_ids)}"
        )

        print("PASS: All tables share one ingestion batch ID")

        # Verify duplicate customer IDs were preserved.
        for company, id_col, expected in [
            ("retailmax", "customer_id", 50),
            ("freshmart", "cust_no", 40),
        ]:
            df = read_table(spark, company, "customers")

            duplicates = (
                df.count()
                - df.select(id_col).distinct().count()
            )

            assert duplicates == expected, (
                f"{company}: expected {expected} duplicates, "
                f"got {duplicates}"
            )

            print(
                f"PASS: {company} duplicates preserved: "
                f"{duplicates}"
            )

        # Verify intentionally introduced sales issues.
        for company, qty_col, product_col, neg_expected, invalid_expected in [
            ("retailmax", "quantity", "product_id", 250, 250),
            ("freshmart", "units_sold", "sku_code", 200, 200),
        ]:
            df = read_table(spark, company, "sales")

            negatives = df.filter(
                col(qty_col).cast("int") < 0
            ).count()

            invalid_products = df.filter(
                col(product_col).startswith("INVALID-PRODUCT-")
            ).count()

            assert negatives == neg_expected
            assert invalid_products == invalid_expected

            print(
                f"PASS: {company} negatives={negatives}, "
                f"invalid products={invalid_products}"
            )

        print("\nSUCCESS: ALL S3 BRONZE VALIDATIONS PASSED!")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
