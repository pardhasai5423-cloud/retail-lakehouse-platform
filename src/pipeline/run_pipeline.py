"""Command-line entry point for isolated retail lakehouse runs."""

import argparse
import json

from src.pipeline.master_orchestrator import build_pipeline_plan


def main():
    parser = argparse.ArgumentParser(
        description="Plan an isolated Retail Lakehouse pipeline run"
    )
    parser.add_argument(
        "--run-id",
        required=True,
        help="Unique isolated pipeline run identifier",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and display the pipeline plan without execution",
    )

    args = parser.parse_args()

    if not args.dry_run:
        parser.error(
            "Pipeline execution is not yet enabled. "
            "Use --dry-run."
        )

    plan = build_pipeline_plan(args.run_id)

    if plan["execution_enabled"] is not False:
        raise RuntimeError("Unexpected execution-enabled plan")

    print(json.dumps(plan, indent=2))


if __name__ == "__main__":
    main()
