"""Isolated run-specific Gold planning. No data writes."""

from src.glue.gold_planner import plan_gold_refresh
from src.pipeline.gold_safety import GOLD_TABLES
from src.pipeline.run_config import RunConfig


def plan_run_gold_refresh(changed_datasets, run_config):
    """Plan Gold refreshes strictly within one isolated run."""

    if not isinstance(run_config, RunConfig):
        raise TypeError("A validated RunConfig is required")

    tables = tuple(plan_gold_refresh(changed_datasets))

    approved_tables = frozenset(GOLD_TABLES)

    for table in tables:
        if table not in approved_tables:
            raise ValueError(f"Unapproved Gold table: {table}")

    silver_base = run_config.path("silver")
    gold_base = run_config.path("gold")

    targets = {
        table: run_config.path("gold", table)
        for table in tables
    }

    return {
        "status": "dry_run",
        "run_id": run_config.run_id,
        "tables": tables,
        "silver_base": silver_base,
        "gold_base": gold_base,
        "targets": targets,
        "execution_enabled": False,
    }
