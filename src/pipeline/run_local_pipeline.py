"""Isolated local Bronze -> Silver -> Gold pipeline runner.

All pipeline outputs must remain under a new local_runs/<run_id>
directory. This module must never write to S3.
"""

from pathlib import Path

from src.config.write_safety import require_new_local_target


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SOURCE_ROOT = PROJECT_ROOT / "data" / "source_dirty"
LOCAL_RUNS_ROOT = PROJECT_ROOT / "local_runs"

COMPANIES = ("retailmax", "freshmart")
DATASETS = (
    "customers",
    "products",
    "stores",
    "suppliers",
    "inventory",
    "sales",
)


def validate_run_directory(run_id):
    """Return a new, isolated local output directory."""
    if not isinstance(run_id, str):
        raise ValueError("Run ID must be a string")

    if not run_id.startswith("local-"):
        raise ValueError("Run ID must start with local-")

    suffix = run_id[len("local-"):]

    if not suffix or not all(
        character.isalnum() or character in "-_"
        for character in suffix
    ):
        raise ValueError("Invalid local run ID")

    target = LOCAL_RUNS_ROOT / run_id

    if target.resolve().parent != LOCAL_RUNS_ROOT.resolve():
        raise ValueError("Run directory escapes local_runs")

    require_new_local_target(target)

    return target


def execute_local_bronze(spark, run_root):
    """Ingest all 12 source CSVs into isolated local Bronze tables."""
    from src.glue.jobs.bronze_ingestion import (
        read_bronze_batch,
        prepare_bronze_batch,
    )

    results = []

    for dataset in DATASETS:
        for company in COMPANIES:
            source_path = SOURCE_ROOT / company / f"{dataset}.csv"
            source_key = f"incoming/{company}/{dataset}/{dataset}.csv"
            target = run_root / "bronze" / company / dataset

            if not source_path.is_file():
                raise FileNotFoundError(
                    f"Missing source CSV: {source_path}"
                )

            require_new_local_target(target)

            raw_df, plan = read_bronze_batch(
                spark,
                str(source_path),
                source_key,
            )

            bronze_df = prepare_bronze_batch(
                raw_df,
                plan,
                plan.batch_id,
                source_key,
            )

            (
                bronze_df.write
                .format("delta")
                .mode("errorifexists")
                .save(str(target))
            )

            results.append({
                "company": company,
                "dataset": dataset,
                "batch_id": plan.batch_id,
                "target": str(target),
            })

            print(f"[BRONZE] Completed: {company}/{dataset}")

    return results


def execute_local_silver(spark, run_root, bronze_results):
    """Transform isolated Bronze batches into consolidated Silver."""
    from src.glue.jobs.silver_execution import execute_silver_batch

    batch_lookup = {
        (item["company"], item["dataset"]): item
        for item in bronze_results
    }

    results = []

    for dataset in DATASETS:
        for company in COMPANIES:
            batch = batch_lookup[(company, dataset)]

            result = execute_silver_batch(
                spark,
                company=company,
                dataset=dataset,
                batch_id=batch["batch_id"],
                bronze_path=batch["target"],
                silver_base=str(run_root / "silver"),
                allow_writes=True,
            )

            results.append({
                "company": company,
                "dataset": dataset,
                "result": result,
            })

            print(f"[SILVER] Completed: {company}/{dataset}")

    return results


def execute_local_gold(spark, run_root):
    """Build all 11 Gold tables inside the isolated local run."""
    from src.glue.gold_planner import GOLD_ORDER
    from src.glue.jobs.gold_execution import execute_gold_table
    from src.glue.gold_refresh import (
        execute_date_refresh,
        execute_aggregate_refresh,
    )

    silver_base = str(run_root / "silver")
    gold_base = str(run_root / "gold")
    results = []

    for table in GOLD_ORDER:
        target = run_root / "gold" / table
        require_new_local_target(target)

        if table == "dim_date":
            result = execute_date_refresh(
                spark,
                silver_base,
                gold_base,
                allow_writes=True,
            )
        elif table.startswith("agg_"):
            result = execute_aggregate_refresh(
                spark,
                table,
                gold_base,
                allow_writes=True,
            )
        else:
            result = execute_gold_table(
                spark,
                table,
                silver_base,
                gold_base,
                allow_writes=True,
            )

        results.append({"table": table, "result": result})
        print(f"[GOLD] Completed: {table}")

    return results


def validate_local_pipeline_paths(run_root):
    """Validate every planned output against the isolated local run."""
    root = Path(run_root).resolve()
    approved_parent = LOCAL_RUNS_ROOT.resolve()

    if root.parent != approved_parent:
        raise ValueError("Pipeline output must be inside local_runs")

    if not root.name.startswith("local-"):
        raise ValueError("Invalid local pipeline run directory")

    require_new_local_target(root)

    targets = []

    for company in COMPANIES:
        for dataset in DATASETS:
            targets.append(root / "bronze" / company / dataset)

    for dataset in DATASETS:
        targets.append(root / "silver" / dataset)

    targets.append(root / "silver" / "sales_quarantine")

    from src.glue.gold_planner import GOLD_ORDER

    for table in GOLD_ORDER:
        targets.append(root / "gold" / table)

    for target in targets:
        resolved = target.resolve()

        if root not in resolved.parents:
            raise ValueError(
                f"Pipeline target escapes local run: {target}"
            )

        require_new_local_target(resolved)

    return root


def main():
    """Run validation or an explicitly approved isolated local pipeline."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Isolated local Retail Lakehouse pipeline"
    )
    parser.add_argument("--run-id", required=True)

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true")
    mode.add_argument("--execute-local", action="store_true")

    args = parser.parse_args()

    run_root = validate_run_directory(args.run_id)
    validate_local_pipeline_paths(run_root)

    for company in COMPANIES:
        for dataset in DATASETS:
            source = SOURCE_ROOT / company / f"{dataset}.csv"
            if not source.is_file():
                raise FileNotFoundError(
                    f"Missing source CSV: {source}"
                )

    print(f"[SOURCE] {SOURCE_ROOT}")
    print(f"[OUTPUT] {run_root}")

    if args.dry_run:
        print("[SAFETY] Local-only dry run")
        print("[BRONZE] 12 source datasets planned")
        print("[SILVER] 12 transformation batches planned")
        print("[GOLD] 11 analytical tables planned")
        print("[STATUS] Validation successful; no data written")
        return

    print("[SAFETY] Isolated local execution requested")

    from src.bronze.ingest_bronze import create_spark_session

    spark = create_spark_session()

    try:
        bronze_results = execute_local_bronze(spark, run_root)
        execute_local_silver(spark, run_root, bronze_results)
        execute_local_gold(spark, run_root)
        print("[SUCCESS] Local pipeline completed")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
