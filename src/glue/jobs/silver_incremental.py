"""Batch-scoped Silver transformations using existing business mappings."""

from pyspark.sql import functions as F

DATASETS = (
    "customers", "products", "stores",
    "sales", "inventory", "suppliers",
)


def select_bronze_batch(bronze_df, batch_id):
    """Restrict processing to one nonempty, explicitly identified batch."""
    if not isinstance(batch_id, str) or not batch_id.strip():
        raise ValueError("A nonempty batch_id is required")

    if "_batch_id" not in bronze_df.columns:
        raise ValueError("Bronze input is missing _batch_id")

    return bronze_df.filter(F.col("_batch_id") == F.lit(batch_id))


def standardize_silver_batch(bronze_df, company, dataset, batch_id):
    """Apply the existing RetailMax/FreshMart canonical mapping."""
    if company not in ("retailmax", "freshmart"):
        raise ValueError(f"Unsupported company: {company}")

    if dataset not in DATASETS:
        raise ValueError(f"Unsupported dataset: {dataset}")

    scoped = select_bronze_batch(bronze_df, batch_id)

    # Import lazily so selecting a batch does not initialize S3 settings.
    from importlib import import_module

    module = import_module(f"src.silver.transform_{dataset}")
    function = getattr(module, f"transform_{company}_{dataset}")

    return function(scoped)


def clean_silver_batch(standardized_df, dataset):
    """Apply existing dataset-specific cleaning and validation rules."""
    if dataset not in DATASETS:
        raise ValueError(f"Unsupported dataset: {dataset}")

    from importlib import import_module

    module = import_module(f"src.silver.clean_{dataset}")

    if dataset == "customers":
        return module.deduplicate_customers(
            module.clean_blank_values(standardized_df)
        )

    if dataset == "products":
        cleaned = module.clean_blank_values(standardized_df)
        validated = module.add_validation_status(
            module.add_price_quality_status(cleaned)
        )
        return module.deduplicate_products(
            validated.filter(F.col("validation_status") == "VALID")
        )

    if dataset == "stores":
        cleaned = module.clean_blank_values(standardized_df)
        validated = module.add_validation_status(
            module.add_opening_date_status(
                module.add_zip_quality_status(cleaned)
            )
        )
        return module.deduplicate_stores(
            validated.filter(F.col("validation_status") == "VALID")
        )

    if dataset == "inventory":
        cleaned = module.clean_identifiers(standardized_df)
        validated = module.add_validation_status(cleaned)
        classified = module.add_inventory_freshness_status(
            module.add_stock_status(validated)
        )
        valid = classified.filter(
            F.col("validation_status") == "VALID"
        )
        return module.deduplicate_inventory(valid)

    if dataset == "suppliers":
        cleaned = module.clean_blank_values(standardized_df)
        validated = module.add_validation_status(
            module.add_email_quality_status(cleaned)
        )
        return module.deduplicate_suppliers(
            validated.filter(F.col("validation_status") == "VALID")
        )

    if dataset == "sales":
        raise ValueError(
            "Sales requires customer, product, and store reference tables; "
            "use clean_incremental_sales_batch instead"
        )

    raise AssertionError("Unreachable dataset")



def clean_incremental_sales_batch(
    standardized_df,
    customers_df,
    products_df,
    stores_df,
):
    """Validate sales references and split valid/quarantined records."""
    from importlib import import_module

    module = import_module("src.silver.clean_sales")

    cleaned = module.clean_identifiers(standardized_df)
    deduplicated = module.deduplicate_sales(cleaned)

    referenced = module.add_reference_status(
        deduplicated,
        customers_df,
        "customer_key",
        "customer_reference_status",
    )
    referenced = module.add_reference_status(
        referenced,
        products_df,
        "product_key",
        "product_reference_status",
    )
    referenced = module.add_reference_status(
        referenced,
        stores_df,
        "store_key",
        "store_reference_status",
    )

    classified = module.add_quality_flags(referenced)

    valid = classified.filter(
        F.col("validation_status") == "VALID"
    )
    quarantine = classified.filter(
        F.col("validation_status") == "QUARANTINE"
    )

    return valid, quarantine
