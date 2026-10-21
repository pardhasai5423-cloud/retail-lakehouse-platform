"""Create-only S3 staging for verified incoming CSV batches."""

import hashlib
from pathlib import Path

from src.glue.config import get_glue_config
from src.glue.staging import get_staging_path


def stage_verified_batch(
    s3_client,
    source_key,
    batch_id,
    local_csv_path,
    expected_sha256,
):
    """
    Upload a verified local CSV to its deterministic S3 staging path.

    The upload is conditional: an existing object cannot be overwritten.

    This function performs an S3 write when called.
    """

    if (
        not isinstance(expected_sha256, str)
        or len(expected_sha256) != 64
        or any(c not in "0123456789abcdefABCDEF"
               for c in expected_sha256)
    ):
        raise ValueError("A valid SHA-256 checksum is required")

    target = get_staging_path(source_key, batch_id)
    config = get_glue_config()

    local_path = Path(local_csv_path)

    if not local_path.is_file():
        raise FileNotFoundError(
            f"Verified CSV file not found: {local_path}"
        )

    # Keep a bounded, immutable snapshot of the file.
    # Verify and upload the exact same bytes.
    max_batch_bytes = 50 * 1024 * 1024

    with local_path.open("rb") as file:
        data = file.read(max_batch_bytes + 1)

    if not data:
        raise ValueError("Cannot stage an empty CSV batch")

    if len(data) > max_batch_bytes:
        raise ValueError("CSV batch exceeds 50 MB staging limit")

    actual_sha256 = hashlib.sha256(data).hexdigest()

    if actual_sha256 != expected_sha256.lower():
        raise ValueError(
            "Local CSV SHA-256 checksum mismatch"
        )

    prefix = f"s3://{config['bucket']}/"
    if not target.startswith(prefix):
        raise RuntimeError("Staging target bucket mismatch")

    target_key = target[len(prefix):]

    # A single conditional PutObject is used to prevent overwrites.
    # This implementation is intended for small, bounded CSV batches.
    from botocore.exceptions import ClientError

    try:
        response = s3_client.put_object(
            Bucket=config["bucket"],
            Key=target_key,
            Body=data,
            ContentLength=len(data),
            IfNoneMatch="*",
            Metadata={
                "batch-id": batch_id,
                "sha256": actual_sha256,
                "source-key": source_key,
            },
        )
        status = "created"

    except ClientError as error:
        code = error.response.get("Error", {}).get("Code")

        if code not in ("PreconditionFailed", "412"):
            raise

        # Read and verify the existing object's actual bytes.
        # Metadata alone is not sufficient to prove integrity.
        existing = s3_client.get_object(
            Bucket=config["bucket"],
            Key=target_key,
        )

        body = existing["Body"]

        try:
            existing_data = body.read(50 * 1024 * 1024 + 1)
        finally:
            body.close()

        metadata = existing.get("Metadata", {})

        if (
            len(existing_data) != len(data)
            or hashlib.sha256(existing_data).hexdigest()
            != actual_sha256
            or metadata.get("batch-id") != batch_id
            or metadata.get("sha256") != actual_sha256
            or metadata.get("source-key") != source_key
        ):
            raise RuntimeError(
                "Existing staging object failed integrity validation"
            )

        response = existing
        status = "reused"

    return {
        "staging_path": target,
        "batch_id": batch_id,
        "sha256": actual_sha256,
        "etag": response.get("ETag"),
        "status": status,
    }
