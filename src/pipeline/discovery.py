"""Read-only discovery of incoming S3 CSV batches."""

from src.glue.batch_planner import plan_batch
from src.glue.object_identity import build_object_batch_id
from src.pipeline.safety import (
    BUCKET,
    ALLOWED_PREFIX,
)

RAW_PREFIX = f"{ALLOWED_PREFIX}/raw/"


def discover_batches(s3_client):
    """List valid incoming CSV objects without modifying S3."""
    paginator = s3_client.get_paginator("list_objects_v2")

    batches = []

    for page in paginator.paginate(
        Bucket=BUCKET,
        Prefix=RAW_PREFIX,
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            try:
                plan = plan_batch(key)
            except ValueError:
                continue

            if obj.get("Size", 0) <= 0:
                continue

            etag = obj.get("ETag")

            if not etag:
                raise ValueError(
                    f"Missing ETag for incoming object: {key}"
                )

            batch_id = build_object_batch_id(
                key,
                etag=etag,
            )

            batches.append({
                "batch_id": batch_id,
                "company": plan.company,
                "dataset": plan.dataset,
                "source_key": key,
                "etag": etag,
                "size_bytes": obj["Size"],
            })

    return sorted(
        batches,
        key=lambda batch: (
            batch["dataset"],
            batch["company"],
            batch["source_key"],
        ),
    )
