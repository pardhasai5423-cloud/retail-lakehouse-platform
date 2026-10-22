"""Strict destination safety controls for incremental Silver writes."""

from src.pipeline.safety import (
    pipeline_path,
    validate_pipeline_path,
    require_write_approval,
)

SILVER_DATASETS = frozenset({
    "customers",
    "products",
    "stores",
    "suppliers",
    "inventory",
    "sales",
    "sales_quarantine",
})


def get_silver_target(dataset):
    """Build an exact, isolated Silver Delta table destination."""
    if dataset not in SILVER_DATASETS:
        raise ValueError(f"Unsupported Silver destination: {dataset!r}")

    return pipeline_path("silver", dataset)


def validate_silver_target(target_path):
    """Reject every destination outside the seven Silver tables."""
    validate_pipeline_path(target_path)

    allowed = {
        get_silver_target(dataset)
        for dataset in SILVER_DATASETS
    }

    if target_path not in allowed:
        raise ValueError(
            "Destination is not an approved Silver Delta table"
        )

    return target_path


def require_silver_write_approval(target_path, *, allow_writes=False):
    """Validate destination and require explicit write opt-in."""
    validate_silver_target(target_path)
    require_write_approval(allow_writes=allow_writes)

    return target_path
