"""Incremental Gold dimension/fact execution.

Default behavior is read-only planning. Writes require explicit opt-in.
"""

from src.glue.gold_planner import (
    plan_gold_refresh,
    build_gold_table,
    get_gold_source,
    GOLD_SOURCES,
)
from src.glue.gold_writer import merge_gold_table
from src.glue.snapshot_contract import current_delta_version, read_pinned_snapshot, assert_snapshot_unchanged


def plan_gold_execution(changed_datasets, gold_base):
    if not isinstance(gold_base, str) or not gold_base.strip():
        raise ValueError("A Gold base path is required")

    tables = plan_gold_refresh(changed_datasets)

    return {
        "gold_base": gold_base.rstrip("/"),
        "tables": tables,
        "direct_tables": tuple(
            table for table in tables
            if table in GOLD_SOURCES
        ),
        "deferred_tables": tuple(
            table for table in tables
            if table not in GOLD_SOURCES
        ),
    }


def execute_gold_table(
    spark,
    table,
    silver_base,
    gold_base,
    *,
    allow_writes=False,
):
    """Transform one current Silver table and MERGE its Gold output."""
    if table not in GOLD_SOURCES:
        raise ValueError(
            f"Not a directly executable Gold table: {table}"
        )

    if not isinstance(silver_base, str) or not silver_base.strip():
        raise ValueError("A Silver base path is required")

    if not isinstance(gold_base, str) or not gold_base.strip():
        raise ValueError("A Gold base path is required")

    dataset = get_gold_source(table)
    silver_path = f"{silver_base.rstrip('/')}/{dataset}"
    gold_path = f"{gold_base.rstrip('/')}/{table}"

    if not allow_writes:
        return {
            "status": "dry_run",
            "table": table,
            "silver_path": silver_path,
            "gold_path": gold_path,
        }

    if table == "fact_sales":
        snapshot_version = current_delta_version(spark, silver_path)
        silver_df = read_pinned_snapshot(
            spark, silver_path, snapshot_version
        )
    else:
        silver_df = spark.read.format("delta").load(silver_path)

    gold_df = build_gold_table(table, silver_df)

    if table == "fact_sales":
        assert_snapshot_unchanged(
            spark, silver_path, snapshot_version
        )

    result = merge_gold_table(
        spark, gold_df, table, gold_path
    , full_snapshot=(table == "fact_sales"))

    return {
        **result,
        "silver_path": silver_path,
        "gold_path": gold_path,
    }
