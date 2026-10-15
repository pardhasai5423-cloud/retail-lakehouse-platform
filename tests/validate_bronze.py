
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, sum as spark_sum, when

ROOT = Path(__file__).resolve().parents[1]
BRONZE = ROOT / "data" / "bronze"

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

def create_spark():
    builder = (
        SparkSession.builder
        .appName("ValidateBronzeLayer")
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
    return configure_spark_with_delta_pip(builder).getOrCreate()

def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        total = 0

        for company, datasets in EXPECTED_COUNTS.items():
            for dataset, expected in datasets.items():
                path = BRONZE / company / dataset
                df = spark.read.format("delta").load(str(path))

                actual = df.count()
                assert actual == expected, (
                    f"{company}.{dataset}: "
                    f"expected {expected}, got {actual}"
                )

                required_metadata = [
                    "_source_system",
                    "_source_dataset",
                    "_source_file",
                    "_ingestion_timestamp",
                ]

                for column in required_metadata:
                    assert column in df.columns, (
                        f"Missing metadata column: {column}"
                    )

                assert df.filter(
                    col("_source_system") != company
                ).count() == 0

                assert df.filter(
                    col("_source_dataset") != dataset
                ).count() == 0

                assert df.filter(
                    col("_ingestion_timestamp").isNull()
                ).count() == 0

                total += actual
                print(
                    f"PASS: {company}.{dataset} "
                    f"({actual:,} rows)"
                )

        assert total == 104760
        print(f"\nPASS: Total Bronze records = {total:,}")

        # Confirm intentionally duplicated customers remain.
        for company, id_col, expected_duplicates in [
            ("retailmax", "customer_id", 50),
            ("freshmart", "cust_no", 40),
        ]:
            df = spark.read.format("delta").load(
                str(BRONZE / company / "customers")
            )

            duplicates = (
                df.count()
                - df.select(id_col).distinct().count()
            )

            assert duplicates == expected_duplicates
            print(
                f"PASS: {company} duplicates preserved: "
                f"{duplicates}"
            )

        # Confirm negative quantities and invalid product IDs.
        for company, qty_col, product_col, expected_negative, expected_invalid in [
            ("retailmax", "quantity", "product_id", 250, 250),
            ("freshmart", "units_sold", "sku_code", 200, 200),
        ]:
            df = spark.read.format("delta").load(
                str(BRONZE / company / "sales")
            )

            negatives = df.filter(
                col(qty_col).cast("int") < 0
            ).count()

            invalid_products = df.filter(
                col(product_col).startswith("INVALID-PRODUCT-")
            ).count()

            assert negatives == expected_negative
            assert invalid_products == expected_invalid

            print(
                f"PASS: {company} negatives={negatives}, "
                f"invalid products={invalid_products}"
            )

        print("\nSUCCESS: ALL BRONZE VALIDATIONS PASSED!")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
