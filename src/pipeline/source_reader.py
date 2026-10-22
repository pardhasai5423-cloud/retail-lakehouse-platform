"""Read S3 source batches using ETag conditions and SHA-256 checks."""

import hashlib
import re

from src.glue.batch_planner import plan_batch
from src.glue.object_identity import build_object_batch_id
from src.pipeline.safety import BUCKET, ALLOWED_PREFIX

SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def download_verified_batch(
    s3_client,
    batch,
    destination_path,
    *,
    expected_sha256,
):
    """Download an identified S3 object without modifying S3."""
    from pathlib import Path

    key = batch["source_key"]

    if not key.startswith(f"{ALLOWED_PREFIX}/raw/"):
        raise ValueError("Source key outside approved raw prefix")

    plan = plan_batch(key)

    if (
        plan.company != batch["company"]
        or plan.dataset != batch["dataset"]
    ):
        raise ValueError("Batch identity mismatch")

    etag = batch["etag"]

    if not isinstance(etag, str) or not etag.strip():
        raise ValueError("Missing source ETag")

    if batch["batch_id"] != build_object_batch_id(
        key, etag=etag
    ):
        raise ValueError("Batch ID does not match source identity")

    if not isinstance(expected_sha256, str) or not SHA256_PATTERN.fullmatch(
        expected_sha256
    ):
        raise ValueError("A trusted SHA-256 checksum is required")

    expected_size = batch["size_bytes"]

    if not isinstance(expected_size, int) or expected_size <= 0:
        raise ValueError("Invalid expected source size")

    destination = Path(destination_path)

    if not destination.parent.is_dir():
        raise FileNotFoundError(destination.parent)

    if destination.exists():
        raise FileExistsError(destination)

    response = s3_client.get_object(
        Bucket=BUCKET,
        Key=key,
        IfMatch=etag,
    )

    try:
        if response.get("ETag") != etag:
            raise RuntimeError("S3 ETag changed during download")

        if response.get("ContentLength") != expected_size:
            raise RuntimeError("S3 source size changed")

        digest = hashlib.sha256()
        downloaded = 0

        try:
            with destination.open("xb") as output:
                for chunk in response["Body"].iter_chunks(
                    chunk_size=1024 * 1024
                ):
                    if not chunk:
                        continue

                    downloaded += len(chunk)

                    if downloaded > expected_size:
                        raise IOError("S3 download exceeds expected size")

                    output.write(chunk)
                    digest.update(chunk)

            if downloaded != expected_size:
                raise IOError("S3 download size mismatch")

            if digest.hexdigest() != expected_sha256:
                raise IOError("S3 download SHA-256 mismatch")

        except Exception:
            destination.unlink(missing_ok=True)
            raise

    finally:
        response["Body"].close()

    return {
        "local_path": str(destination),
        "source_key": key,
        "batch_id": batch["batch_id"],
        "size_bytes": downloaded,
        "sha256": digest.hexdigest(),
    }
