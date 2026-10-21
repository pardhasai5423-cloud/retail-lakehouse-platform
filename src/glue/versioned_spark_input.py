"""Read an exact S3 object version into a verified Spark DataFrame."""

import hashlib
import io
import re

from src.glue.batch_planner import plan_batch
from src.glue.config import get_glue_config
from src.glue.source_schema import load_source_schema


MAX_BATCH_BYTES = 50 * 1024 * 1024


def read_verified_staging_batch(
    spark,
    s3_client,
    source_key,
    batch_id,
    staging_key,
    staging_version_id,
    expected_sha256,
):
    """
    Read the exact S3 staging version and verify its contents.

    Returns a Spark DataFrame and the source batch plan.
    No S3 writes occur.
    """

    from pyspark.sql import Row

    plan = plan_batch(source_key)
    config = get_glue_config()

    if not isinstance(staging_version_id, str) or not staging_version_id.strip():
        raise ValueError("A staging VersionId is required")

    if (
        not isinstance(expected_sha256, str)
        or re.fullmatch(r"[0-9a-fA-F]{64}", expected_sha256) is None
    ):
        raise ValueError("Invalid expected SHA-256 checksum")

    expected_key = (
        f"{config['prefix']}/staging/"
        f"{plan.company}/{plan.dataset}/"
        f"{batch_id}/{plan.filename}"
    )

    if staging_key != expected_key:
        raise ValueError("Staging key does not match approved batch path")

    response = s3_client.get_object(
        Bucket=config["bucket"],
        Key=staging_key,
        VersionId=staging_version_id,
    )

    body = response["Body"]

    try:
        if response.get("VersionId") != staging_version_id:
            raise RuntimeError("Staging version mismatch")

        data = body.read(MAX_BATCH_BYTES + 1)
    finally:
        body.close()

    if not data:
        raise ValueError("Staged CSV is empty")

    if len(data) > MAX_BATCH_BYTES:
        raise ValueError("Staged CSV exceeds 50 MiB")

    actual_sha256 = hashlib.sha256(data).hexdigest()

    if actual_sha256 != expected_sha256.lower():
        raise RuntimeError("Staged CSV checksum mismatch")

    # Parse CSV with Python's CSV parser and then distribute records
    # through Spark. This avoids an unversioned Spark S3 read.
    import csv

    text = data.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text, newline=""))

    schema = load_source_schema(plan.company, plan.dataset)
    expected_columns = [field.name for field in schema.fields]

    if reader.fieldnames != expected_columns:
        raise ValueError("Staged CSV header does not match source schema")

    rows = []

    for record in reader:
        if None in record:
            raise ValueError("Staged CSV contains extra columns")

        if any(value is None for value in record.values()):
            raise ValueError("Staged CSV contains missing columns")

        rows.append(
            Row(**{
                column: record[column]
                for column in expected_columns
            })
        )

    if not rows:
        raise ValueError("Staged CSV contains no data rows")

    df = spark.createDataFrame(rows, schema=schema)

    return df, plan
