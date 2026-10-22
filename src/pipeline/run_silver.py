"""Run-specific Silver orchestration with strict destination validation."""

from src.glue.batch_planner import plan_run_batch
from src.glue.jobs.silver_execution import execute_silver_batch
from src.pipeline.run_config import RunConfig


def execute_run_silver_batch(
    spark,
    source_key,
    batch_id,
    run_config,
    *,
    allow_writes=False,
):
    """Plan Silver processing within a validated isolated run."""

    if not isinstance(run_config, RunConfig):
        raise TypeError("A validated RunConfig is required")

    plan = plan_run_batch(source_key, run_config)

    if batch_id != plan.batch_id:
        raise ValueError("Batch ID does not match source identity")

    bronze_path = run_config.path(
        "bronze", plan.company, plan.dataset
    )
    silver_base = run_config.path("silver")

    if allow_writes is not False:
        raise PermissionError(
            "Run-specific Silver writes are disabled until "
            "source selection and execution safety are verified"
        )

    return execute_silver_batch(
        spark,
        plan.company,
        plan.dataset,
        batch_id,
        bronze_path,
        silver_base,
        allow_writes=False,
    )
