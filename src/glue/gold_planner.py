"""Dependency-aware planning for incremental Gold processing.

Planning only: this module performs no reads or writes.
"""

from importlib import import_module

SILVER_DATASETS = (
    "customers", "products", "suppliers",
    "stores", "inventory", "sales",
)

GOLD_ORDER = (
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

# Dependencies describe which Gold outputs can change
# when a particular Silver dataset changes.
GOLD_IMPACTS = {
    "customers": {
        "dim_customers", "dim_date",
    },
    "products": {
        "dim_products", "agg_category_sales",
        "agg_inventory_status",
    },
    "suppliers": {
        "dim_suppliers",
    },
    "stores": {
        "dim_stores",
        "agg_store_performance",
    },
    "inventory": {
        "dim_date", "fact_inventory",
        "agg_inventory_status",
    },
    "sales": {
        "dim_date", "fact_sales",
        "agg_daily_sales",
        "agg_store_performance",
        "agg_category_sales",
    },
}

# Existing reusable transformation functions.
# Aggregations and dim_date require separate processing.
BUILDERS = {
    "dim_customers": (
        "src.gold.build_dim_customers",
        "build_customer_dimension",
    ),
    "dim_products": (
        "src.gold.build_dim_products",
        "build_product_dimension",
    ),
    "dim_suppliers": (
        "src.gold.build_dim_suppliers",
        "build_supplier_dimension",
    ),
    "dim_stores": (
        "src.gold.build_dim_stores",
        "build_store_dimension",
    ),
    "fact_sales": (
        "src.gold.build_fact_sales",
        "build_sales_fact",
    ),
    "fact_inventory": (
        "src.gold.build_fact_inventory",
        "build_inventory_fact",
    ),
}

GOLD_SOURCES = {
    "dim_customers": "customers",
    "dim_products": "products",
    "dim_suppliers": "suppliers",
    "dim_stores": "stores",
    "fact_sales": "sales",
    "fact_inventory": "inventory",
}


def plan_gold_refresh(changed_datasets):
    """Return ordered Gold outputs affected by changed Silver data."""
    if isinstance(changed_datasets, str):
        raise ValueError("Provide a collection of Silver datasets")

    try:
        changed = set(changed_datasets)
    except (TypeError, ValueError):
        raise ValueError("Invalid Silver dataset collection") from None

    if not changed:
        raise ValueError("At least one changed dataset is required")

    invalid = changed - set(SILVER_DATASETS)
    if invalid:
        raise ValueError(
            f"Unsupported Silver datasets: {sorted(map(str, invalid))}"
        )

    affected = set()
    for dataset in changed:
        affected.update(GOLD_IMPACTS[dataset])

    return tuple(
        table for table in GOLD_ORDER if table in affected
    )


def build_gold_table(table, silver_df):
    """Reuse an existing Gold dimension/fact transformation."""
    if table not in BUILDERS:
        raise ValueError(
            f"No reusable dimension/fact builder for: {table}"
        )

    module_name, function_name = BUILDERS[table]
    module = import_module(module_name)
    builder = getattr(module, function_name)
    return builder(silver_df)


def get_gold_source(table):
    """Return the Silver source for a Gold dimension or fact."""
    if table not in GOLD_SOURCES:
        raise ValueError(
            f"No direct Silver source for Gold table: {table}"
        )
    return GOLD_SOURCES[table]
