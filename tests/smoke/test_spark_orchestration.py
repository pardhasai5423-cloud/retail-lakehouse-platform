
import sys
from pathlib import Path

# Allow imports from the project root when executed directly.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from src.bronze.ingest_bronze_s3 import create_spark


def main():
    print("=== SPARK ORCHESTRATION SMOKE TEST ===", flush=True)

    spark = None

    try:
        spark = create_spark()

        print(
            f"Spark version: {spark.version}",
            flush=True,
        )

        result = (
            spark.range(1, 101)
            .agg({"id": "sum"})
            .collect()[0][0]
        )

        assert result == 5050, (
            f"Unexpected Spark calculation result: {result}"
        )

        print(
            f"Spark calculation result: {result}",
            flush=True,
        )

        delta_extension = spark.conf.get(
            "spark.sql.extensions"
        )

        assert (
            "DeltaSparkSessionExtension" in delta_extension
        ), "Delta Lake extension is missing"

        print("Delta Lake extension: configured", flush=True)
        print("PASS: Spark smoke test successful!", flush=True)

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
