"""Read-only S3 object metadata validation for Glue ingestion."""

from src.glue.batch_planner import plan_batch
from src.glue.config import get_glue_config
from src.glue.object_identity import build_object_batch_id


def validate_s3_object(
    s3_client,
    source_key,
    version_id,
):
    """
    Validate the exact S3 object version before ingestion.

    Performs a read-only S3 HeadObject request.
    Does not download or modify the object.
    """

    if not isinstance(version_id, str) or not version_id.strip():
        raise ValueError(
            "A valid S3 VersionId is required"
        )

    batch = plan_batch(source_key)
    config = get_glue_config()

    response = s3_client.head_object(
        Bucket=config["bucket"],
        Key=source_key,
        VersionId=version_id,
    )

    actual_version = response.get("VersionId")

    if actual_version != version_id:
        raise RuntimeError(
            "S3 object version mismatch: "
            f"expected {version_id!r}, "
            f"received {actual_version!r}"
        )

    size = response.get("ContentLength")

    if not isinstance(size, int) or size <= 0:
        raise ValueError(
            "S3 source object must be non-empty"
        )

    batch_id = build_object_batch_id(
        source_key,
        version_id=actual_version,
    )

    return {
        "bucket": config["bucket"],
        "source_key": source_key,
        "version_id": actual_version,
        "company": batch.company,
        "dataset": batch.dataset,
        "size_bytes": size,
        "etag": response.get("ETag"),
        "batch_id": batch_id,
    }


def download_s3_object_version(
    s3_client,
    source_key,
    version_id,
    destination_path,
    expected_sha256=None,
):
    """
    Download an exact S3 object version to a local file.

    Intended for controlled staging before Spark ingestion.
    Does not write to S3.
    """
    from pathlib import Path
    import hashlib
    import re

    if expected_sha256 is not None:
        if (
            not isinstance(expected_sha256, str)
            or re.fullmatch(
                r"[0-9a-fA-F]{64}", expected_sha256
            ) is None
        ):
            raise ValueError("Invalid expected SHA-256 checksum")

    if not isinstance(version_id, str) or not version_id.strip():
        raise ValueError("A valid S3 VersionId is required")

    plan_batch(source_key)
    config = get_glue_config()

    destination = Path(destination_path)

    if destination.exists():
        raise FileExistsError(
            f"Destination already exists: {destination}"
        )

    if not destination.parent.is_dir():
        raise FileNotFoundError(
            f"Destination directory does not exist: "
            f"{destination.parent}"
        )

    response = s3_client.get_object(
        Bucket=config["bucket"],
        Key=source_key,
        VersionId=version_id,
    )

    actual_version = response.get("VersionId")

    if actual_version != version_id:
        response["Body"].close()
        raise RuntimeError(
            "Downloaded S3 object version does not match "
            "the requested version"
        )

    expected_size = response.get("ContentLength")

    try:
        if not isinstance(expected_size, int) or expected_size <= 0:
            raise ValueError("Invalid S3 object content length")

        downloaded_bytes = 0
        sha256 = hashlib.sha256()

        with destination.open("xb") as output:
            for chunk in response["Body"].iter_chunks(
                chunk_size=1024 * 1024
            ):
                if chunk:
                    downloaded_bytes += len(chunk)

                    if downloaded_bytes > expected_size:
                        raise IOError(
                            "Downloaded data exceeds expected S3 object size"
                        )

                    output.write(chunk)
                    sha256.update(chunk)

        if downloaded_bytes != expected_size:
            raise IOError(
                "S3 download size mismatch: "
                f"expected {expected_size}, "
                f"received {downloaded_bytes}"
            )

        if (
            expected_sha256 is not None
            and sha256.hexdigest().lower()
            != expected_sha256.lower()
        ):
            raise IOError("S3 download SHA-256 checksum mismatch")

    except Exception:
        destination.unlink(missing_ok=True)
        raise
    finally:
        response["Body"].close()

    return str(destination)
