
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
    lit,
)
from delta import configure_spark_with_delta_pip
from src.config.write_safety import require_new_local_target


ROOT = Path(__file__).resolve().parents[2]
SOURCE_DIR = ROOT / "data" / "source_dirty"
BRONZE_DIR = ROOT / "data" / "bronze"

COMPANIES = ["retailmax", "freshmart"]

DATASETS = [
    "customers",
    "products",
    "stores",
    "sales",
    "inventory",
    "suppliers",
]


def create_spark_session():
    builder = (
        SparkSession.builder
        .appName("RetailLakehouseBronzeIngestion")
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
    )

    return configure_spark_with_delta_pip(
        builder
    ).getOrCreate()


def ingest_dataset(spark, company, dataset):
    source_path = SOURCE_DIR / company / f"{dataset}.csv"
    target_path = BRONZE_DIR / company / dataset

    if not source_path.exists():
        raise FileNotFoundError(
            f"Missing source file: {source_path}"
        )

    print(f"\nIngesting {company}.{dataset}", flush=True)

    # Preserve source columns as strings.
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .csv(str(source_path))
    )

    # Add ingestion metadata.
    bronze_df = (
        df
        .withColumn("_source_system", lit(company))
        .withColumn("_source_dataset", lit(dataset))
        .withColumn("_source_file", input_file_name())
        .withColumn("_ingestion_timestamp", current_timestamp())
    )

    require_new_local_target(target_path)

    # Write only to a new local Delta target.
    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .save(str(target_path))
    )

    # Verify persisted data.
    result = (
        spark.read
        .format("delta")
        .load(str(target_path))
    )

    count = result.count()

    print(
        f"SUCCESS: {company}.{dataset} — {count:,} rows",
        flush=True
    )

    return count


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        total = 0

        for company in COMPANIES:
            for dataset in DATASETS:
                total += ingest_dataset(
                    spark, company, dataset
                )

        print("\n" + "=" * 50)
        print("BRONZE INGESTION COMPLETED")
        print(f"Total records: {total:,}")
        print("=" * 50)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
