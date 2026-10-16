
"""
Export validated Gold Delta tables from S3 to CSV for Power BI.

Each Gold table becomes a single CSV file under data/powerbi/.
The export includes a manifest containing source and export counts.

This is a local reporting extract, not a replacement for Gold Delta.
"""

import csv
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "src" / "silver"))

from transform_customers import create_spark


BUCKET = "retail-lakehouse-parsam-2026"
GOLD_ROOT = f"s3a://{BUCKET}/gold"

OUTPUT_DIR = PROJECT_ROOT / "data" / "powerbi"

GOLD_TABLES = {
    "dim_customers": 9000,
    "dim_products": 1800,
    "dim_suppliers": 180,
    "dim_stores": 90,
    "dim_date": 4275,
    "fact_sales": 88600,
    "fact_inventory": 3600,
    "agg_daily_sales": 2736,
    "agg_store_performance": 90,
    "agg_category_sales": 14,
    "agg_inventory_status": 59,
}


def export_table(spark, table_name, expected_count):
    source_path = f"{GOLD_ROOT}/{table_name}"
    output_path = OUTPUT_DIR / f"{table_name}.csv"

    print(f"\n{'=' * 65}")
    print(f"Exporting: {table_name}")
    print(f"Source: {source_path}")

    df = spark.read.format("delta").load(source_path)

    actual_count = df.count()

    if actual_count != expected_count:
        raise ValueError(
            f"{table_name}: expected {expected_count} rows, "
            f"but Gold contains {actual_count}"
        )

    # Gold tables are modest in size for this portfolio project.
    # Collecting them to the driver allows one CSV file per table.
    # For production-scale data, use distributed Spark exports.
    rows = df.toLocalIterator()
    columns = df.columns

    temporary_path = output_path.with_suffix(".csv.tmp")

    try:
        with temporary_path.open(
            "w", newline="", encoding="utf-8"
        ) as file:
            writer = csv.writer(file)
            writer.writerow(columns)

            exported_count = 0

            for row in rows:
                writer.writerow([
                    (
                        value.isoformat()
                        if isinstance(value, (datetime,))
                        else value
                    )
                    for value in row
                ])
                exported_count += 1

        if exported_count != actual_count:
            raise ValueError(
                f"{table_name}: export count mismatch. "
                f"Source={actual_count}, Export={exported_count}"
            )

        os.replace(temporary_path, output_path)

    finally:
        if temporary_path.exists():
            temporary_path.unlink()

    file_size = output_path.stat().st_size

    print(f"Gold rows:     {actual_count:,}")
    print(f"Exported rows: {exported_count:,}")
    print(f"File size:     {file_size:,} bytes")
    print("STATUS: PASS")

    return {
        "table": table_name,
        "source": source_path,
        "file": output_path.name,
        "source_rows": actual_count,
        "exported_rows": exported_count,
        "columns": columns,
        "file_size_bytes": file_size,
        "status": "PASS",
    }


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    os.environ.setdefault("AWS_PROFILE", "retail-lakehouse")

    print("=" * 65)
    print("GOLD DELTA -> POWER BI CSV EXPORT")
    print("=" * 65)

    spark = create_spark()
    results = []

    try:
        for table_name, expected_count in GOLD_TABLES.items():
            result = export_table(
                spark, table_name, expected_count
            )
            results.append(result)

        manifest = {
            "exported_at_utc": datetime.now(
                timezone.utc
            ).isoformat(),
            "bucket": BUCKET,
            "table_count": len(results),
            "total_rows": sum(
                item["exported_rows"] for item in results
            ),
            "tables": results,
        }

        manifest_path = OUTPUT_DIR / "export_manifest.json"
        temporary_manifest = OUTPUT_DIR / "export_manifest.json.tmp"

        with temporary_manifest.open(
            "w", encoding="utf-8"
        ) as file:
            json.dump(manifest, file, indent=2)

        os.replace(temporary_manifest, manifest_path)

        print(f"\n{'=' * 65}")
        print("EXPORT SUMMARY")
        print("=" * 65)

        for item in results:
            print(
                f"{item['table']:<28} "
                f"{item['exported_rows']:>10,} rows  PASS"
            )

        print("-" * 65)
        print(f"Tables exported: {len(results)}")
        print(f"Total rows:      {manifest['total_rows']:,}")
        print(f"Manifest:        {manifest_path}")
        print("SUCCESS: ALL GOLD TABLES EXPORTED AND VERIFIED")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
