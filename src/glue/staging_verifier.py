"""Verify staged S3 objects before distributed Spark ingestion."""

import re

from src.glue.config import get_glue_config
from src.glue.staging import get_staging_path


def verify_staged_object(
    s3_client,
    source_key,
    batch_id,
    expected_sha256,
    expected_size,
):
    """
    Verify staging metadata and capture its exact S3 version.

    Performs only HeadObject. No AWS writes.
    """

    if (
        not isinstance(expected_sha256, str)
        or re.fullmatch(r"[0-9a-fA-F]{64}", expected_sha256) is None
    ):
        raise ValueError("Invalid expected SHA-256 checksum")

    if (
        not isinstance(expected_size, int)
        or isinstance(expected_size, bool)
        or expected_size <= 0
    ):
        raise ValueError("Invalid expected staging size")

    target = get_staging_path(source_key, batch_id)
    config = get_glue_config()

    prefix = f"s3://{config['bucket']}/"
    key = target[len(prefix):]

    response = s3_client.head_object(
        Bucket=config["bucket"],
        Key=key,
    )

    metadata = response.get("Metadata", {})

    if (
        response.get("ContentLength") != expected_size
        or metadata.get("batch-id") != batch_id
        or metadata.get("sha256", "").lower()
        != expected_sha256.lower()
        or metadata.get("source-key") != source_key
    ):
        raise RuntimeError(
            "Staged S3 object metadata or size mismatch"
        )

    version_id = response.get("VersionId")

    if not isinstance(version_id, str) or not version_id.strip():
        raise RuntimeError(
            "Staged S3 object has no usable VersionId"
        )

    return {
        "staging_path": target,
        "staging_key": key,
        "staging_version_id": version_id,
        "sha256": expected_sha256.lower(),
        "size_bytes": expected_size,
    }
