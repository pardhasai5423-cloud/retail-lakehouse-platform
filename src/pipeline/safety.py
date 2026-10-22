"""Safety controls for the local incremental lakehouse runner."""

from urllib.parse import urlparse

BUCKET = "retail-lakehouse-parsam-2026"
REGION = "us-east-2"
ALLOWED_PREFIX = "test-runs/glue-incremental-v1"
PROTECTED_PREFIX = "test-runs/run-001"


def pipeline_path(*parts):
    """Build a Spark-compatible path inside the isolated S3 prefix."""
    for part in parts:
        if (
            not isinstance(part, str)
            or not part
            or part in (".", "..")
            or "/" in part
            or "\\" in part
        ):
            raise ValueError(f"Invalid S3 path component: {part!r}")

    suffix = "/".join(parts)
    key = f"{ALLOWED_PREFIX}/{suffix}" if suffix else ALLOWED_PREFIX

    return f"s3a://{BUCKET}/{key}"


def validate_pipeline_path(path):
    """Reject paths outside the isolated pipeline namespace."""
    parsed = urlparse(path)

    if parsed.scheme != "s3a" or parsed.netloc != BUCKET:
        raise ValueError("Unexpected S3 scheme or bucket")

    key = parsed.path.lstrip("/")

    if not (
        key == ALLOWED_PREFIX
        or key.startswith(ALLOWED_PREFIX + "/")
    ):
        raise ValueError("Path is outside the approved pipeline prefix")

    if any(part in (".", "..") for part in key.split("/")):
        raise ValueError("Unsafe S3 path")

    return path


def require_write_approval(allow_writes=False):
    """Require explicit opt-in before any pipeline write."""
    if allow_writes is not True:
        raise RuntimeError(
            "Pipeline writes are disabled. "
            "Explicit write approval is required."
        )
