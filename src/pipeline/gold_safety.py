"""Strict isolated-prefix safety for Gold Delta destinations."""

from src.pipeline.safety import pipeline_path

GOLD_TABLES = (
    "dim_customers",
    "dim_suppliers",
    "dim_products",
    "dim_stores",
    "dim_date",
    "fact_sales",
    "fact_inventory",
    "agg_daily_sales",
    "agg_store_performance",
    "agg_category_sales",
    "agg_inventory_status",
)

GOLD_BASE = pipeline_path("gold")

APPROVED_GOLD_TARGETS = frozenset(
    pipeline_path("gold", table)
    for table in GOLD_TABLES
)


def get_gold_target(table):
    """Return the exact isolated target for an approved Gold table."""
    if not isinstance(table, str) or table not in GOLD_TABLES:
        raise ValueError(f"Unsupported Gold table: {table!r}")

    return pipeline_path("gold", table)


def validate_isolated_gold_target(target):
    """Reject every destination outside the 11 exact Gold paths."""
    if not isinstance(target, str):
        raise ValueError("Gold target must be a string")

    if target not in APPROVED_GOLD_TARGETS:
        raise ValueError(
            f"Gold destination is not approved: {target!r}"
        )

    return target


def require_gold_write_approval(target, *, allow_writes=False):
    """Require explicit programmatic opt-in for a validated target."""
    validate_isolated_gold_target(target)

    if allow_writes is not True:
        raise PermissionError(
            "Gold writes are disabled without explicit approval"
        )

    return target
