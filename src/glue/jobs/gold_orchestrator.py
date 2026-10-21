"""Dependency-ordered Gold execution with dry-run protection."""

from src.glue.gold_planner import (
    GOLD_ORDER,
    plan_gold_refresh,
)
from src.glue.gold_refresh import (
    execute_date_refresh,
    execute_aggregate_refresh,
    validate_gold_target,
)
from src.glue.jobs.gold_execution import execute_gold_table


AGGREGATE_TABLES = frozenset({
    "agg_daily_sales",
    "agg_store_performance",
    "agg_category_sales",
    "agg_inventory_status",
})

DATE_TABLE = "dim_date"

DIRECT_TABLES = frozenset(GOLD_ORDER) - AGGREGATE_TABLES - {
    DATE_TABLE
}


def plan_gold_pipeline(changed_datasets, silver_base, gold_base):
    """Plan all affected Gold tables without reading or writing data."""
    if not isinstance(silver_base, str) or not silver_base.strip():
        raise ValueError("Silver base path is required")

    if not isinstance(gold_base, str) or not gold_base.strip():
        raise ValueError("Gold base path is required")

    tables = tuple(plan_gold_refresh(changed_datasets))

    for table in tables:
        validate_gold_target(
            f"{gold_base.rstrip('/')}/{table}"
        )

    return {
        "tables": tables,
        "silver_base": silver_base.rstrip("/"),
        "gold_base": gold_base.rstrip("/"),
    }


def execute_gold_pipeline(
    spark,
    changed_datasets,
    silver_base,
    gold_base,
    *,
    allow_writes=False,
):
    """Execute all impacted Gold tables in dependency order.

    No Spark reads or writes occur unless allow_writes=True.
    """
    plan = plan_gold_pipeline(
        changed_datasets, silver_base, gold_base
    )

    results = []

    for table in plan["tables"]:
        if table == DATE_TABLE:
            result = execute_date_refresh(
                spark,
                plan["silver_base"],
                plan["gold_base"],
                allow_writes=allow_writes,
            )

        elif table in AGGREGATE_TABLES:
            result = execute_aggregate_refresh(
                spark,
                table,
                plan["gold_base"],
                allow_writes=allow_writes,
            )

        elif table in DIRECT_TABLES:
            result = execute_gold_table(
                spark,
                table,
                plan["silver_base"],
                plan["gold_base"],
                allow_writes=allow_writes,
            )

        else:
            raise ValueError(
                f"Unrecognized Gold table: {table}"
            )

        results.append(result)

    return {
        "status": (
            "dry_run" if not allow_writes else "completed"
        ),
        "tables": plan["tables"],
        "results": results,
    }
