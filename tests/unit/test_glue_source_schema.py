"""Tests for the AWS Glue Bronze source schema loader."""

import pytest
from pyspark.sql.types import StringType, StructType

from src.glue.source_schema import load_source_schema


COMPANIES = ("retailmax", "freshmart")

DATASETS = (
    "customers",
    "products",
    "stores",
    "sales",
    "inventory",
    "suppliers",
)


@pytest.mark.parametrize(
    "company,dataset",
    [
        (company, dataset)
        for company in COMPANIES
        for dataset in DATASETS
    ],
)
def test_all_source_schemas_load(company, dataset):
    schema = load_source_schema(company, dataset)

    assert isinstance(schema, StructType)
    assert len(schema.fields) > 0

    # Bronze must preserve raw CSV values as strings.
    assert all(
        isinstance(field.dataType, StringType)
        for field in schema.fields
    )

    assert all(field.nullable for field in schema.fields)

    names = schema.fieldNames()
    assert len(names) == len(set(names))


def test_retailmax_sales_columns():
    schema = load_source_schema("retailmax", "sales")

    assert schema.fieldNames() == [
        "transaction_id",
        "customer_id",
        "product_id",
        "store_id",
        "transaction_date",
        "quantity",
        "unit_price",
        "discount_amount",
        "total_amount",
    ]


def test_freshmart_sales_columns():
    schema = load_source_schema("freshmart", "sales")

    assert schema.fieldNames() == [
        "receipt_no",
        "cust_no",
        "sku_code",
        "location_code",
        "sale_date",
        "units_sold",
        "price_per_unit",
        "discount_value",
        "net_sales",
    ]


@pytest.mark.parametrize(
    "company,dataset",
    [
        ("unknown", "sales"),
        ("retailmax", "unknown"),
        ("freshmart", "transactions"),
    ],
)
def test_unsupported_schemas_are_rejected(company, dataset):
    with pytest.raises(ValueError):
        load_source_schema(company, dataset)
