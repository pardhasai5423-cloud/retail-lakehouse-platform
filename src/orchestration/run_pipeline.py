
import argparse
import logging
import os
import subprocess
import sys
import uuid
import json
import time

from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs" / "pipeline"

PIPELINE = {
    "bronze": [
        "src/bronze/ingest_bronze_s3.py",
    ],
    "silver": [
        "src/silver/write_silver_customers.py",
        "src/silver/write_silver_products.py",
        "src/silver/write_silver_suppliers.py",
        "src/silver/write_silver_stores.py",
        "src/silver/write_silver_inventory.py",
        "src/silver/write_silver_sales.py",
    ],
    "gold": [
        "src/gold/build_dim_customers.py",
        "src/gold/build_dim_products.py",
        "src/gold/build_dim_suppliers.py",
        "src/gold/build_dim_stores.py",
        "src/gold/build_dim_date.py",
        "src/gold/build_fact_sales.py",
        "src/gold/build_fact_inventory.py",
        "src/gold/build_agg_daily_sales.py",
        "src/gold/build_agg_store_performance.py",
        "src/gold/build_agg_category_sales.py",
        "src/gold/build_agg_inventory_status.py",
    ],
    "powerbi": [
        "src/powerbi/export_gold_to_csv.py",
    ],
}


def configure_logging(run_id):
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_path = LOG_DIR / f"{run_id}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    return log_path


def execute_script(script_path, run_id):
    absolute_path = PROJECT_ROOT / script_path

    if not absolute_path.is_file():
        raise FileNotFoundError(
            f"Pipeline script not found: {absolute_path}"
        )

    logging.info("STARTING: %s", script_path)

    start_time = datetime.now(timezone.utc)

    env = os.environ.copy()
    env.setdefault("AWS_PROFILE", "retail-lakehouse")
    env["PIPELINE_RUN_ID"] = run_id

    process = subprocess.Popen(
        [sys.executable, str(absolute_path)],
        cwd=str(PROJECT_ROOT),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    try:
        for line in process.stdout:
            logging.info("[%s] %s", script_path, line.rstrip())
    finally:
        if process.stdout:
            process.stdout.close()

    return_code = process.wait()

    duration = (
        datetime.now(timezone.utc) - start_time
    ).total_seconds()

    if return_code != 0:
        raise RuntimeError(
            f"{script_path} failed with exit code {return_code}"
        )

    logging.info(
        "COMPLETED: %s | Duration: %.2f seconds",
        script_path,
        duration,
    )



def run_pipeline(stages, dry_run):
    run_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + uuid.uuid4().hex[:8]
    )

    log_path = configure_logging(run_id)
    report_path = LOG_DIR / f"{run_id}.json"

    start_time = time.monotonic()

    total_steps = sum(len(PIPELINE[stage]) for stage in stages)

    report = {
        "run_id": run_id,
        "status": "RUNNING",
        "dry_run": dry_run,
        "stages": stages,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "finished_at": None,
        "duration_seconds": None,
        "total_steps": total_steps,
        "completed_steps": 0,
        "planned_steps": 0,
        "failed_step": None,
        "steps": [],
    }

    logging.info("RETAIL LAKEHOUSE PIPELINE")
    logging.info("Run ID: %s", run_id)
    logging.info("Stages: %s", ", ".join(stages))
    logging.info("Dry run: %s", dry_run)

    step_number = 0

    try:
        for stage in stages:
            logging.info("STAGE: %s", stage.upper())

            for script in PIPELINE[stage]:
                step_number += 1

                step = {
                    "step_number": step_number,
                    "stage": stage,
                    "script": script,
                    "status": "PENDING",
                    "duration_seconds": None,
                }

                report["steps"].append(step)

                logging.info(
                    "STEP %d/%d: %s",
                    step_number,
                    total_steps,
                    script,
                )

                if dry_run:
                    step["status"] = "PLANNED"
                    report["planned_steps"] += 1
                    logging.info("DRY RUN: Skipping execution")
                    continue

                step_start = time.monotonic()
                step["status"] = "RUNNING"

                try:
                    execute_script(script, run_id)

                    step["status"] = "SUCCESS"
                    report["completed_steps"] += 1

                except Exception as exc:
                    step["status"] = "FAILED"
                    step["error"] = str(exc)

                    report["failed_step"] = script
                    report["status"] = "FAILED"

                    logging.exception(
                        "PIPELINE FAILED at step %d: %s",
                        step_number,
                        script,
                    )

                    return 1

                finally:
                    step["duration_seconds"] = round(
                        time.monotonic() - step_start, 2
                    )

        report["status"] = (
            "DRY_RUN" if dry_run else "SUCCESS"
        )

        logging.info(
            "PIPELINE %s",
            "DRY RUN COMPLETED"
            if dry_run
            else "COMPLETED SUCCESSFULLY",
        )

        return 0

    finally:
        report["finished_at"] = (
            datetime.now(timezone.utc).isoformat()
        )

        report["duration_seconds"] = round(
            time.monotonic() - start_time, 2
        )

        with report_path.open("w", encoding="utf-8") as file:
            json.dump(report, file, indent=2)

        logging.info("Log file: %s", log_path)
        logging.info("JSON report: %s", report_path)



def main():
    parser = argparse.ArgumentParser(
        description="Retail Lakehouse Pipeline Orchestrator"
    )

    parser.add_argument(
        "--stages",
        nargs="+",
        choices=list(PIPELINE.keys()),
        default=list(PIPELINE.keys()),
        help="Pipeline stages to execute",
    )

    execution_mode = parser.add_mutually_exclusive_group()

    execution_mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Display execution plan without running scripts",
    )

    execution_mode.add_argument(
        "--execute",
        action="store_true",
        help="Execute the selected pipeline stages",
    )

    args = parser.parse_args()

    # Safe default: never execute data writes without --execute.
    dry_run = not args.execute

    return run_pipeline(args.stages, dry_run)


if __name__ == "__main__":
    sys.exit(main())
