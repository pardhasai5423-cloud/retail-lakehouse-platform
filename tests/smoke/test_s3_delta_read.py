
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from src.bronze.ingest_bronze_s3 import create_spark


DELTA_PATH = (
    "s3a://retail-lakehouse-parsam-2026/"
    "bronze/retailmax/customers"
)


def main():
    print("=== S3 DELTA READ SMOKE TEST ===", flush=True)

    spark = None

    try:
        spark = create_spark()

        print(f"Spark version: {spark.version}", flush=True)
        print(f"Delta path: {DELTA_PATH}", flush=True)

        df = (
            spark.read
            .format("delta")
            .load(DELTA_PATH)
        )

        record_count = df.count()

        print(
            f"Record count: {record_count:,}",
            flush=True,
        )

        assert record_count > 0, (
            "Bronze Customers Delta table is empty"
        )

        print("\n=== TABLE SCHEMA ===", flush=True)
        df.printSchema()

        print(
            "\nPASS: S3 Delta table read successful!",
            flush=True,
        )

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
