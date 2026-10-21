"""Plan version-pinned S3-to-Delta Bronze ingestion."""

from src.glue.s3_object_reader import validate_s3_object
from src.glue.staging import get_staging_path
from src.glue.jobs.run_bronze import build_job_plan


def plan_bronze_execution(s3_client, source_key, version_id):
    """
    Validate an exact S3 source version and build its Bronze plan.

    Performs an S3 HeadObject request, but no S3 writes.
    """

    source = validate_s3_object(
        s3_client,
        source_key,
        version_id,
    )

    plan = build_job_plan(
        source_key,
        version_id=source["version_id"],
    )

    staging_path = get_staging_path(
        source_key,
        plan["batch_id"],
    )

    return {
        **plan,
        "source_version_id": source["version_id"],
        "source_size_bytes": source["size_bytes"],
        "source_etag": source["etag"],
        "staging_path": staging_path,
        "execution_ready": False,
        "dry_run": True,
    }
