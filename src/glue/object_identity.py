"""Identify immutable S3 objects for incremental processing."""

import hashlib

from src.glue.batch_planner import plan_batch


def build_object_batch_id(
    source_key: str,
    version_id: str = None,
    etag: str = None,
) -> str:
    """
    Generate a stable batch ID from an S3 object identity.

    Prefer S3 VersionId. Fall back to ETag when versioning
    is unavailable.

    The source key must follow the approved incoming layout.
    """

    plan_batch(source_key)

    if version_id is not None:
        if not isinstance(version_id, str) or not version_id.strip():
            raise ValueError("Invalid S3 VersionId")
        identity = f"version:{version_id}"

    elif etag is not None:
        if not isinstance(etag, str) or not etag.strip():
            raise ValueError("Invalid S3 ETag")
        identity = f"etag:{etag.strip().strip(chr(34))}"

    else:
        raise ValueError(
            "S3 VersionId or ETag is required for batch identity"
        )

    raw_identity = f"{source_key}\n{identity}"

    digest = hashlib.sha256(
        raw_identity.encode("utf-8")
    ).hexdigest()[:24]

    return f"batch-{digest}"
