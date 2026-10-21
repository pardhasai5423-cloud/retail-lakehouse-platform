"""Entry point for incremental AWS Glue Bronze ingestion."""

import argparse

from src.glue.batch_planner import plan_batch
from src.glue.bronze_writer import (
    get_bronze_target,
    get_transaction_identity,
)
from src.glue.config import get_glue_config
from src.glue.object_identity import build_object_batch_id


def build_job_plan(source_key, version_id=None, etag=None):
    """Validate an incoming S3 object and prepare its job plan."""

    config = get_glue_config()
    batch = plan_batch(source_key)

    batch_id = build_object_batch_id(
        source_key,
        version_id=version_id,
        etag=etag,
    )

    target = get_bronze_target(source_key)

    app_id, txn_version = get_transaction_identity(
        source_key, batch_id
    )

    return {
        "bucket": config["bucket"],
        "region": config["region"],
        "source_key": batch.source_key,
        "company": batch.company,
        "dataset": batch.dataset,
        "batch_id": batch_id,
        "bronze_target": target,
        "transaction_app_id": app_id,
        "transaction_version": txn_version,
        "dry_run": True,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Plan incremental Bronze ingestion"
    )

    parser.add_argument("--source-key", required=True)

    identity = parser.add_mutually_exclusive_group(required=True)
    identity.add_argument("--version-id")
    identity.add_argument("--etag")

    args = parser.parse_args()

    plan = build_job_plan(
        source_key=args.source_key,
        version_id=args.version_id,
        etag=args.etag,
    )

    print("BRONZE INGESTION DRY RUN")
    for key, value in plan.items():
        print(f"{key}: {value}")

    print("No AWS reads or writes were performed.")


if __name__ == "__main__":
    main()
