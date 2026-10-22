"""Controlled, overwrite-protected upload of initial CSV batches."""

import hashlib
from pathlib import Path

from src.glue.batch_planner import plan_batch
from src.pipeline.safety import (
    BUCKET,
    ALLOWED_PREFIX,
    require_write_approval,
)


def upload_initial_batch(s3_client, item, *, allow_writes=False):
    """Upload one manifest item without replacing an existing S3 object."""
    require_write_approval(allow_writes)

    key = item["source_key"]
    expected_prefix = f"{ALLOWED_PREFIX}/raw/"

    if not key.startswith(expected_prefix):
        raise ValueError("Upload destination is outside approved raw prefix")

    plan = plan_batch(key)

    if (
        plan.company != item["company"]
        or plan.dataset != item["dataset"]
    ):
        raise ValueError("Manifest identity mismatch")

    path = Path(item["local_path"]).resolve(strict=True)

    approved_root = Path("data/source_dirty").resolve(strict=True)

    if not path.is_relative_to(approved_root):
        raise ValueError("Source file is outside approved local directory")

    if path != approved_root / plan.company / f"{plan.dataset}.csv":
        raise ValueError("Unexpected local source file")

    data = path.read_bytes()

    if not data:
        raise ValueError("Cannot upload an empty source file")

    actual_sha256 = hashlib.sha256(data).hexdigest()

    if (
        len(data) != item["size_bytes"]
        or actual_sha256 != item["sha256"]
    ):
        raise ValueError("Source file differs from staging manifest")

    s3_client.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=data,
        IfNoneMatch="*",
        ChecksumSHA256=__import__("base64").b64encode(
            hashlib.sha256(data).digest()
        ).decode("ascii"),
    )

    metadata = s3_client.head_object(
        Bucket=BUCKET,
        Key=key,
    )

    if metadata["ContentLength"] != len(data):
        raise RuntimeError("Uploaded object size verification failed")

    return {
        "status": "uploaded",
        "source_key": key,
        "size_bytes": len(data),
        "sha256": actual_sha256,
        "etag": metadata.get("ETag"),
        "version_id": metadata.get("VersionId"),
    }
