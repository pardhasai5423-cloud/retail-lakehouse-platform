
import os
import sys

from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(__file__))

from transform_customers import (
    create_spark,
    BRONZE_BASE,
)


def parse_sales_date(column_name):
    """
    Parse ISO and US-style dates from Bronze.

    Invalid or missing dates may become null
    and will be investigated during profiling.
    """
    return F.coalesce(
        F.to_date(F.col(column_name), "yyyy-MM-dd"),
        F.to_date(F.col(column_name), "MM/dd/yyyy"),
    )


def standardized_sales(
    df,
    source_system,
    transaction_col,
    customer_col,
    product_col,
    store_col,
    date_col,
    quantity_col,
    unit_price_col,
    discount_col,
    total_col,
):
    """
    Map either company's Bronze Sales data
    into a unified canonical schema.

    No cleansing, deduplication, or filtering
    is performed at this stage.
    """

    source = F.lit(source_system)

    def source_key(column_name):
        return F.concat(
            source,
            F.lit(":"),
            F.trim(F.col(column_name))
        )

    return df.select(
        source.alias("source_system"),

        F.trim(F.col(transaction_col)).alias(
            "source_transaction_id"
        ),

        source_key(transaction_col).alias(
            "transaction_key"
        ),

        F.trim(F.col(customer_col)).alias(
            "source_customer_id"
        ),

        source_key(customer_col).alias(
            "customer_key"
        ),

        F.trim(F.col(product_col)).alias(
            "source_product_id"
        ),

        source_key(product_col).alias(
            "product_key"
        ),

        F.trim(F.col(store_col)).alias(
            "source_store_id"
        ),

        source_key(store_col).alias(
            "store_key"
        ),

        parse_sales_date(date_col).alias(
            "transaction_date"
        ),

        F.trim(F.col(quantity_col)).cast(
            "int"
        ).alias("quantity"),

        F.trim(F.col(unit_price_col)).cast(
            "decimal(12,2)"
        ).alias("unit_price"),

        F.trim(F.col(discount_col)).cast(
            "decimal(12,2)"
        ).alias("discount_amount"),

        F.trim(F.col(total_col)).cast(
            "decimal(14,2)"
        ).alias("total_amount"),

        F.col("_ingestion_timestamp").alias(
            "ingestion_timestamp"
        ),

        F.col("_batch_id").alias("batch_id"),

        F.col("_source_file").alias("source_file"),
    )


def transform_retailmax_sales(df):
    return standardized_sales(
        df=df,
        source_system="retailmax",
        transaction_col="transaction_id",
        customer_col="customer_id",
        product_col="product_id",
        store_col="store_id",
        date_col="transaction_date",
        quantity_col="quantity",
        unit_price_col="unit_price",
        discount_col="discount_amount",
        total_col="total_amount",
    )


def transform_freshmart_sales(df):
    return standardized_sales(
        df=df,
        source_system="freshmart",
        transaction_col="receipt_no",
        customer_col="cust_no",
        product_col="sku_code",
        store_col="location_code",
        date_col="sale_date",
        quantity_col="units_sold",
        unit_price_col="price_per_unit",
        discount_col="discount_value",
        total_col="net_sales",
    )


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("\n=== READING SALES BRONZE TABLES ===")

        retailmax_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/retailmax/sales")
        )

        freshmart_bronze = (
            spark.read.format("delta")
            .load(f"{BRONZE_BASE}/freshmart/sales")
        )

        print("\n=== STANDARDIZING SALES SCHEMAS ===")

        retailmax = transform_retailmax_sales(
            retailmax_bronze
        )

        freshmart = transform_freshmart_sales(
            freshmart_bronze
        )

        unified = retailmax.unionByName(freshmart)

        print("\n=== UNIFIED SALES SCHEMA ===")
        unified.printSchema()

        print("\n=== SALES COUNTS BY COMPANY ===")
        unified.groupBy(
            "source_system"
        ).count().show()

        print("\n=== SAMPLE STANDARDIZED SALES ===")

        unified.select(
            "source_system",
            "source_transaction_id",
            "transaction_key",
            "customer_key",
            "product_key",
            "store_key",
            "transaction_date",
            "quantity",
            "unit_price",
            "discount_amount",
            "total_amount",
        ).show(10, truncate=False)

        total = unified.count()

        print(f"\nTotal Sales records: {total:,}")

        assert total == 90000, (
            f"Expected 90,000 records, found {total}"
        )

        source_counts = {
            row["source_system"]: row["count"]
            for row in unified.groupBy(
                "source_system"
            ).count().collect()
        }

        assert source_counts == {
            "retailmax": 50000,
            "freshmart": 40000,
        }

        print(
            "\nSUCCESS: SALES SCHEMA "
            "STANDARDIZATION COMPLETED!"
        )

        print(
            "\nNOTE: No Silver Sales table "
            "has been written yet."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
