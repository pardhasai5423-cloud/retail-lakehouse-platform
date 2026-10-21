"""Reusable Gold aggregation transformations.

These functions produce DataFrames only; they perform no writes.
"""

from pyspark.sql import functions as F


def _sales_metrics():
    return [
        F.count("*").alias("transaction_count"),
        F.sum("quantity").alias("units_sold"),
        F.sum(
            F.col("quantity") * F.col("unit_price")
        ).cast("decimal(18,2)").alias("gross_sales_amount"),
        F.sum("discount_amount").cast("decimal(18,2)").alias(
            "discount_amount"
        ),
        F.sum("total_amount").cast("decimal(18,2)").alias(
            "net_revenue"
        ),
    ]


def _validate_unique_keys(df, key, name):
    if df.filter(F.col(key).isNull()).limit(1).count():
        raise ValueError(f"{name} contains null {key}")

    if (
        df.groupBy(key).count()
        .filter(F.col("count") > 1)
        .limit(1).count()
    ):
        raise ValueError(f"{name} contains duplicate {key}")


def _join_product_dimension(facts, products, *, include_price=False):
    _validate_unique_keys(products, "product_key", "Products")

    product_columns = [
        "product_key",
        F.col("source_system").alias("product_source_system"),
        "category",
    ]

    if include_price:
        if "unit_price" in facts.columns:
            raise ValueError(
                "Inventory facts unexpectedly contain unit_price"
            )
        product_columns.append("unit_price")

    lookup = products.select(*product_columns)

    missing = (
        facts.select("product_key")
        .join(lookup.select("product_key"), "product_key", "left_anti")
        .limit(1).count()
    )
    if missing:
        raise ValueError("Facts reference missing products")

    joined = facts.join(lookup, "product_key", "inner")

    if joined.filter(
        ~F.col("source_system").eqNullSafe(
            F.col("product_source_system")
        )
    ).limit(1).count():
        raise ValueError("Product source system mismatch")

    if joined.filter(
        F.col("category").isNull()
        | (F.trim("category") == "")
    ).limit(1).count():
        raise ValueError("Missing product category")

    return joined


def build_daily_sales(sales):
    return (
        sales.groupBy(
            "date_key", "transaction_date", "source_system"
        )
        .agg(*_sales_metrics())
        .withColumn(
            "average_transaction_value",
            F.when(
                F.col("transaction_count") > 0,
                (
                    F.col("net_revenue")
                    / F.col("transaction_count")
                ).cast("decimal(18,2)")
            )
        )
    )


def build_store_performance(sales, stores):
    _validate_unique_keys(stores, "store_key", "Stores")

    if (
        sales.select("store_key")
        .join(stores.select("store_key"), "store_key", "left_anti")
        .limit(1).count()
    ):
        raise ValueError("Sales reference missing stores")

    store_sales = sales.groupBy("store_key").agg(
        *_sales_metrics(),
        F.countDistinct("customer_key").alias("distinct_customers"),
        F.countDistinct("product_key").alias(
            "distinct_products_sold"
        ),
    )

    performance = (
        stores.select(
            "store_key", "source_system", "source_store_id",
            "store_name", "city", "state"
        )
        .join(store_sales, "store_key", "left")
        .fillna(
            0,
            subset=[
                "transaction_count", "units_sold",
                "distinct_customers", "distinct_products_sold",
            ],
        )
    )

    for column in (
        "gross_sales_amount", "discount_amount", "net_revenue"
    ):
        performance = performance.withColumn(
            column,
            F.coalesce(
                F.col(column),
                F.lit(0).cast("decimal(18,2)"),
            ),
        )

    return performance.withColumn(
        "average_transaction_value",
        F.when(
            F.col("transaction_count") > 0,
            (
                F.col("net_revenue")
                / F.col("transaction_count")
            ).cast("decimal(18,2)")
        ).otherwise(F.lit(0).cast("decimal(18,2)")),
    )


def build_category_sales(sales, products):
    enriched = _join_product_dimension(sales, products)

    return (
        enriched.groupBy("source_system", "category")
        .agg(
            *_sales_metrics(),
            F.countDistinct("product_key").alias(
                "distinct_products_sold"
            ),
            F.countDistinct("customer_key").alias(
                "distinct_customers"
            ),
            F.countDistinct("store_key").alias("distinct_stores"),
        )
        .withColumn(
            "average_transaction_value",
            (
                F.col("net_revenue")
                / F.col("transaction_count")
            ).cast("decimal(18,2)"),
        )
    )


def build_inventory_status(inventory, products):
    joined = _join_product_dimension(
        inventory, products, include_price=True
    )

    if joined.filter(
        F.col("stock_status").isNull()
        | F.col("freshness_status").isNull()
    ).limit(1).count():
        raise ValueError("Missing inventory classification")

    enriched = (
        joined.withColumn(
            "reorder_shortfall",
            F.greatest(
                F.col("reorder_level") - F.col("stock_quantity"),
                F.lit(0),
            ),
        )
        .withColumn(
            "estimated_retail_value",
            (
                F.col("stock_quantity") * F.col("unit_price")
            ).cast("decimal(18,2)"),
        )
        .withColumn(
            "missing_price_flag",
            F.when(
                F.col("unit_price").isNull(), F.lit(1)
            ).otherwise(F.lit(0)),
        )
    )

    return enriched.groupBy(
        "source_system", "category",
        "stock_status", "freshness_status"
    ).agg(
        F.count("*").alias("inventory_record_count"),
        F.countDistinct("product_key").alias("distinct_products"),
        F.countDistinct("store_key").alias("distinct_stores"),
        F.sum("stock_quantity").alias("total_stock_quantity"),
        F.sum("reorder_level").alias("total_reorder_level"),
        F.sum("reorder_shortfall").alias("reorder_shortfall_units"),
        F.sum("estimated_retail_value").cast(
            "decimal(18,2)"
        ).alias("estimated_inventory_value"),
        F.sum("missing_price_flag").alias(
            "missing_price_record_count"
        ),
    )


AGGREGATION_BUILDERS = {
    "agg_daily_sales": build_daily_sales,
    "agg_store_performance": build_store_performance,
    "agg_category_sales": build_category_sales,
    "agg_inventory_status": build_inventory_status,
}
