"""Read-only safety checks before the first automated Silver run."""

from src.pipeline.safety import BUCKET
from src.pipeline.silver_safety import (
    get_silver_target,
    validate_silver_target,
)

EXPECTED_EMPTY_DATASETS = (
    "products",
    "stores",
    "suppliers",
    "inventory",
    "sales",
    "sales_quarantine",
)


def s3_prefix_has_objects(s3_client, target):
    """Check whether a Silver destination already contains S3 objects."""
    validate_silver_target(target)

    prefix = target.removeprefix(
        f"s3a://{BUCKET}/"
    ).rstrip("/") + "/"

    response = s3_client.list_objects_v2(
        Bucket=BUCKET,
        Prefix=prefix,
        MaxKeys=1,
    )

    return bool(response.get("Contents"))


def validate_first_silver_run_state(spark, s3_client):
    """Fail closed unless customers exist and all other targets are empty."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    customers_target = get_silver_target("customers")
    validate_silver_target(customers_target)

    if not DeltaTable.isDeltaTable(spark, customers_target):
        raise RuntimeError(
            "STOP: Existing Silver customers Delta table is missing"
        )

    customers = spark.read.format("delta").load(customers_target)

    counts = {
        row["source_system"]: row["count"]
        for row in customers.groupBy("source_system").count().collect()
    }

    if counts != {"retailmax": 5000, "freshmart": 4000}:
        raise RuntimeError(
            f"STOP: Unexpected Silver customers counts: {counts}"
        )

    if customers.count() != 9000:
        raise RuntimeError("STOP: Silver customers count mismatch")

    if customers.select("customer_key").distinct().count() != 9000:
        raise RuntimeError("STOP: Duplicate Silver customer keys")

    print(
        "PASS: Existing Silver customers table has "
        "9,000 unique records",
        flush=True,
    )

    destinations = []

    for dataset in EXPECTED_EMPTY_DATASETS:
        target = get_silver_target(dataset)

        if s3_prefix_has_objects(s3_client, target):
            raise RuntimeError(
                f"STOP: Silver destination is not empty: {target}"
            )

        destinations.append(target)

        print(
            f"PASS: Empty Silver destination: {dataset}",
            flush=True,
        )

    return {
        "status": "ready",
        "customers_count": 9000,
        "empty_destinations": destinations,
    }
