from src.pipeline.run_gold import plan_run_gold_refresh
"""Master orchestration for an isolated retail lakehouse run.

Planning is local and read-only. Execution is intentionally not
implemented until run-specific write safety is integrated.
"""

from src.pipeline.run_config import RunConfig

DATASETS = (
    "customers",
    "products",
    "stores",
    "suppliers",
    "inventory",
    "sales",
)

COMPANIES = ("retailmax", "freshmart")

STAGES = (
    "source_validation",
    "bronze_ingestion",
    "silver_transformation",
    "gold_refresh",
    "data_reconciliation",
    "powerbi_export",
)


def build_pipeline_plan(run_id):
    """Build a deterministic plan without accessing AWS or Spark."""
    config = RunConfig(run_id)

    batches = tuple(
        {
            "company": company,
            "dataset": dataset,
            "source_prefix": (
                f"{config.prefix}/raw/{company}/{dataset}/"
            ),
            "bronze_target": config.path(
                "bronze", company, dataset
            ),
        }
        for dataset in DATASETS
        for company in COMPANIES
    )

    gold_plan = plan_run_gold_refresh(DATASETS, config)
    silver_tables = DATASETS + ("sales_quarantine",)
    gold_tables = gold_plan["tables"]
    gold_targets = gold_plan["targets"]

    return {
        "run_id": config.run_id,
        "bucket": config.bucket,
        "region": config.region,
        "prefix": config.prefix,
        "stages": STAGES,
        "batches": batches,
        "silver_base": config.path("silver"),
        "gold_base": config.path("gold"),
        "execution_enabled": False,
        "silver_tables": silver_tables,
        "gold_tables": gold_tables,
        "gold_targets": gold_targets,
    }
