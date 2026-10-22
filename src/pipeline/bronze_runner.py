"""Local PySpark Bronze processing for verified S3 batches."""

import tempfile
from contextlib import contextmanager
from pathlib import Path

from src.glue.jobs.bronze_ingestion import (
    read_bronze_batch,
    prepare_bronze_batch,
)
from src.pipeline.source_reader import download_verified_batch
from src.pipeline.safety import pipeline_path


def get_bronze_target(batch):
    """Return the isolated S3A Bronze destination."""
    return pipeline_path(
        "bronze",
        batch["company"],
        batch["dataset"],
    )


@contextmanager
def prepare_verified_bronze(
    spark,
    s3_client,
    batch,
    *,
    expected_sha256,
):
    """
    Yield a verified Bronze DataFrame while its CSV remains available.

    No S3 writes are performed here. The temporary CSV remains
    available until the caller exits the context manager.
    """
    with tempfile.TemporaryDirectory(
        prefix="retail-local-bronze-"
    ) as directory:
        local_path = Path(directory) / "source.csv"

        verified = download_verified_batch(
            s3_client,
            batch,
            local_path,
            expected_sha256=expected_sha256,
        )

        df, plan = read_bronze_batch(
            spark,
            verified["local_path"],
            batch["source_key"],
        )

        bronze_df = prepare_bronze_batch(
            df,
            plan,
            batch["batch_id"],
            batch["source_key"],
        )

        yield {
            "dataframe": bronze_df,
            "batch_id": batch["batch_id"],
            "source_key": batch["source_key"],
            "target": get_bronze_target(batch),
            "local_path": str(local_path),
        }
