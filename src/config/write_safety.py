"""
Shared write-safety utilities for the Retail Lakehouse project.

Default behavior:
- Reject unapproved S3 prefixes.
- Reject writes to existing S3 datasets.
- Reject overwriting existing local outputs.
"""

from pathlib import Path

from src.config.settings import (
    get_bucket,
    require_safe_write_environment,
)


def require_new_local_target(path):
    """Reject a local write if the target already exists."""
    target = Path(path)

    if target.exists():
        raise RuntimeError(
            "LOCAL WRITE BLOCKED: Target already exists: "
            f"{target}"
        )

    return target


def require_new_s3_target(spark, path):
    """
    Reject S3 writes outside the approved bucket/prefix
    or to an existing target.

    Uses Spark's Hadoop filesystem configuration.
    """
    require_safe_write_environment()

    approved_root = (
        f"s3a://{get_bucket()}/test-runs/run-001/"
    )

    if not path.startswith(approved_root):
        raise RuntimeError(
            "S3 WRITE BLOCKED: Target is outside "
            f"the approved test prefix: {path}"
        )

    jvm = spark._jvm
    hadoop_conf = spark._jsc.hadoopConfiguration()

    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    filesystem = hadoop_path.getFileSystem(hadoop_conf)

    if filesystem.exists(hadoop_path):
        raise RuntimeError(
            "S3 WRITE BLOCKED: Target already exists: "
            f"{path}"
        )

    return path
