
import os


DEFAULT_BUCKET = "retail-lakehouse-parsam-2026"
DEFAULT_REGION = "us-east-2"
DEFAULT_AWS_PROFILE = "retail-lakehouse"


def get_bucket():
    return os.getenv(
        "LAKEHOUSE_BUCKET",
        DEFAULT_BUCKET,
    )


def get_region():
    return os.getenv(
        "LAKEHOUSE_REGION",
        DEFAULT_REGION,
    )


def get_aws_profile():
    return os.getenv(
        "LAKEHOUSE_AWS_PROFILE",
        DEFAULT_AWS_PROFILE,
    )

def get_s3_path(*parts):
    bucket = get_bucket()

    prefix = os.getenv("LAKEHOUSE_PREFIX", "").strip("/")

    path_parts = []

    if prefix:
        path_parts.append(prefix)

    path_parts.extend(
        str(part).strip("/")
        for part in parts
        if str(part).strip("/")
    )

    key = "/".join(path_parts)

    return f"s3a://{bucket}/{key}"


ALLOWED_EXECUTION_PREFIX = "test-runs/run-001"


def require_safe_write_environment():
    """
    Block data writes unless the approved test prefix is configured.
    """
    raw_prefix = os.getenv("LAKEHOUSE_PREFIX", "")
    prefix = raw_prefix.strip("/")

    if prefix != ALLOWED_EXECUTION_PREFIX or raw_prefix != prefix:
        raise RuntimeError(
            "DATA WRITE BLOCKED: "
            "LAKEHOUSE_PREFIX must be exactly "
            f"'{ALLOWED_EXECUTION_PREFIX}'. "
            f"Received: {raw_prefix!r}"
        )

    print(
        "PASS: Write safety check approved prefix:",
        prefix,
    )
