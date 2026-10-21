"""Configuration and safety checks for AWS Glue ETL jobs."""

import os

DEFAULT_BUCKET = "retail-lakehouse-parsam-2026"
DEFAULT_REGION = "us-east-2"

GLUE_ALLOWED_PREFIX = "test-runs/glue-incremental-v1"
PROTECTED_PREFIX = "test-runs/run-001"


def get_glue_config():
    """Validate the target environment before any Glue write."""

    bucket = os.environ.get("LAKEHOUSE_BUCKET", DEFAULT_BUCKET)
    region = os.environ.get("LAKEHOUSE_REGION", DEFAULT_REGION)
    prefix = os.environ.get("LAKEHOUSE_PREFIX", "")

    if bucket != DEFAULT_BUCKET:
        raise RuntimeError(
            f"Glue write blocked: unapproved bucket {bucket!r}"
        )

    if region != DEFAULT_REGION:
        raise RuntimeError(
            f"Glue write blocked: unapproved region {region!r}"
        )

    if prefix != GLUE_ALLOWED_PREFIX:
        raise RuntimeError(
            "Glue write blocked: LAKEHOUSE_PREFIX must be exactly "
            f"{GLUE_ALLOWED_PREFIX!r}"
        )

    return {
        "bucket": bucket,
        "region": region,
        "prefix": prefix,
    }


def get_glue_s3_path(*parts):
    """Construct an S3 path inside the approved Glue prefix."""

    config = get_glue_config()

    suffix = "/".join(
        str(part).strip("/")
        for part in parts
        if str(part).strip("/")
    )

    # Reject path traversal and malformed path components.
    for part in parts:
        component = str(part)
        if (
            component.startswith("/")
            or component.endswith("/")
            or component in ("", ".", "..")
            or any(segment in ("", ".", "..") for segment in component.split("/"))
            or "\\" in component
            or "://" in component
        ):
            raise ValueError(f"Invalid S3 path component: {component!r}")

    base = (
        f"s3://{config['bucket']}/"
        f"{config['prefix']}"
    )

    return f"{base}/{suffix}" if suffix else base
