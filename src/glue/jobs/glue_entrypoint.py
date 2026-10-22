"""AWS Glue entry point — dry-run only until explicitly approved."""

import argparse
import json
import os

from src.glue.config import get_glue_config, get_glue_s3_path
from src.glue.batch_planner import plan_batch
from src.glue.object_identity import build_object_batch_id


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--company", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--source-key", required=True)
    parser.add_argument("--version-id", required=True)
    parser.add_argument("--expected-sha256")
    parser.add_argument("--allow-writes", action="store_true")
    parser.add_argument("--lakehouse-bucket")
    parser.add_argument("--lakehouse-region")
    parser.add_argument("--lakehouse-prefix")

    args, unknown_args = parser.parse_known_args(argv)

    # AWS Glue supplies additional runtime arguments.
    # Permit only recognized Glue-managed options.
    allowed_glue_options = {
        "--JOB_NAME",
        "--JOB_RUN_ID",
        "--TempDir",
        "--job-bookmark-option",
        "--enable-metrics",
        "--enable-continuous-cloudwatch-log",
        "--enable-glue-datacatalog",
        "--enable-job-insights",
        "--enable-observability-metrics",
        "--enable-spark-ui",
        "--spark-event-logs-path",
        "--additional-python-modules",
        "--extra-py-files",
        "--datalake-formats",
        "--conf",
    }

    index = 0

    while index < len(unknown_args):
        argument = unknown_args[index]

        if not argument.startswith("--"):
            raise ValueError(
                f"Unexpected positional argument: {argument}"
            )

        option, separator, inline_value = argument.partition("=")

        if option not in allowed_glue_options:
            raise ValueError(
                f"Unsupported Glue argument: {option}"
            )

        if separator:
            if not inline_value:
                raise ValueError(
                    f"Missing value for Glue argument: {option}"
                )
            index += 1
            continue

        if (
            index + 1 >= len(unknown_args)
            or unknown_args[index + 1].startswith("--")
        ):
            raise ValueError(
                f"Missing value for Glue argument: {option}"
            )

        index += 2

    config_args = {
        "LAKEHOUSE_BUCKET": args.lakehouse_bucket,
        "LAKEHOUSE_REGION": args.lakehouse_region,
        "LAKEHOUSE_PREFIX": args.lakehouse_prefix,
    }

    for name, value in config_args.items():
        if value is not None:
            os.environ[name] = value

    if args.allow_writes:
        raise RuntimeError(
            "Write execution is disabled in this deployment entry point"
        )

    config = get_glue_config()

    allowed_companies = {"retailmax", "freshmart"}
    allowed_datasets = {
        "customers", "products", "stores",
        "sales", "inventory", "suppliers",
    }

    if args.company not in allowed_companies:
        raise ValueError("Unsupported company")

    if args.dataset not in allowed_datasets:
        raise ValueError("Unsupported dataset")

    if not args.batch_id.strip() or not args.version_id.strip():
        raise ValueError("Batch ID and S3 version ID are required")

    source_prefix = (
        f"{config['prefix']}/raw/"
        f"{args.company}/{args.dataset}/"
    )

    if (
        not args.source_key.startswith(source_prefix)
        or not args.source_key.endswith(".csv")
        or any(
            segment in ("", ".", "..")
            for segment in args.source_key.split("/")
        )
        or "\\\\" in args.source_key
        or "://" in args.source_key
    ):
        raise ValueError(
            "Source key must identify a CSV inside the approved "
            "company/dataset raw prefix"
        )

    batch_plan = plan_batch(args.source_key)

    if (
        batch_plan.company != args.company
        or batch_plan.dataset != args.dataset
    ):
        raise ValueError("Source identity does not match job arguments")

    expected_batch_id = build_object_batch_id(
        args.source_key,
        version_id=args.version_id,
    )

    if args.batch_id != expected_batch_id:
        raise ValueError(
            f"Batch ID mismatch: expected {expected_batch_id}"
        )

    paths = {
        "ledger": get_glue_s3_path("ledger"),
        "bronze": get_glue_s3_path(
            "bronze", batch_plan.company, batch_plan.dataset
        ),
        "silver": get_glue_s3_path("silver"),
        "gold": get_glue_s3_path("gold"),
    }

    plan = {
        "status": "dry_run",
        "batch_id": args.batch_id,
        "company": args.company,
        "dataset": args.dataset,
        "source_key": args.source_key,
        "version_id": args.version_id,
        "approved_bucket": config["bucket"],
        "approved_prefix": config["prefix"],
        "paths": paths,
        "writes_enabled": False,
    }

    print(json.dumps(plan, indent=2))
    return plan


def run_coordinator_dry_run(spark, plan):
    """Run the existing coordinator in read-only mode."""
    from src.glue.jobs.end_to_end_coordinator import (
        execute_end_to_end_batch,
    )

    if plan.get("writes_enabled") is not False:
        raise ValueError("Coordinator integration requires writes disabled")

    paths = plan["paths"]

    return execute_end_to_end_batch(
        spark,
        None,
        batch_id=plan["batch_id"],
        company=plan["company"],
        dataset=plan["dataset"],
        source_key=plan["source_key"],
        version_id=plan["version_id"],
        ledger_path=paths["ledger"],
        bronze_path=paths["bronze"],
        silver_base=paths["silver"],
        gold_base=paths["gold"],
        allow_writes=False,
    )


if __name__ == "__main__":
    main()
