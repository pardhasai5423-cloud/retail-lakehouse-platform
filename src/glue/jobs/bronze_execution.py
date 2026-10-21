"""Version-pinned S3-to-Delta Bronze ingestion workflow."""

import hashlib
import tempfile
from pathlib import Path

from src.glue.jobs.bronze_coordinator import plan_bronze_execution
from src.glue.s3_object_reader import download_s3_object_version
from src.glue.staging_writer import stage_verified_batch
from src.glue.staging_verifier import verify_staged_object
from src.glue.versioned_spark_input import read_verified_staging_batch
from src.glue.jobs.bronze_ingestion import (
    read_bronze_batch,
    prepare_bronze_batch,
)
from src.glue.bronze_writer import append_bronze_batch


def execute_bronze_batch(
    spark,
    s3_client,
    source_key,
    version_id,
    *,
    allow_writes=False,
    expected_sha256=None,
):
    """
    Coordinate an incoming CSV batch through Bronze ingestion.

    By default, only an S3 HeadObject request is performed.
    Actual execution requires allow_writes=True and a trusted checksum.
    """

    plan = plan_bronze_execution(
        s3_client, source_key, version_id
    )

    if not allow_writes:
        return plan

    if (
        not isinstance(expected_sha256, str)
        or len(expected_sha256) != 64
        or any(c not in "0123456789abcdefABCDEF"
               for c in expected_sha256)
    ):
        raise ValueError(
            "A trusted SHA-256 checksum is required for execution"
        )

    with tempfile.TemporaryDirectory(
        prefix="retail-glue-bronze-"
    ) as directory:
        local_path = Path(directory) / "source.csv"

        download_s3_object_version(
            s3_client,
            source_key,
            version_id,
            local_path,
            expected_sha256=expected_sha256,
        )

        actual_sha256 = hashlib.sha256(
            local_path.read_bytes()
        ).hexdigest()

        stage = stage_verified_batch(
            s3_client,
            source_key,
            plan["batch_id"],
            local_path,
            actual_sha256,
        )

        verified_stage = verify_staged_object(
            s3_client,
            source_key,
            plan["batch_id"],
            actual_sha256,
            local_path.stat().st_size,
        )

        if verified_stage["staging_path"] != stage["staging_path"]:
            raise RuntimeError(
                "Verified staging path does not match uploaded path"
            )

        df, batch_plan = read_verified_staging_batch(
            spark,
            s3_client,
            source_key,
            plan["batch_id"],
            verified_stage["staging_key"],
            verified_stage["staging_version_id"],
            actual_sha256,
        )

        bronze_df = prepare_bronze_batch(
            df,
            batch_plan,
            plan["batch_id"],
            source_key,
        )

        target = append_bronze_batch(
            bronze_df,
            source_key,
            plan["batch_id"],
        )

    return {
        **plan,
        "staging_path": verified_stage["staging_path"],
        "staging_version_id": verified_stage["staging_version_id"],
        "bronze_target": target,
        "sha256": actual_sha256,
        "execution_ready": True,
        "dry_run": False,
    }
