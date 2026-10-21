"""Unit tests for incremental Bronze batch planning."""

import pytest

from src.glue.batch_planner import plan_batch


@pytest.mark.parametrize(
    "company,dataset",
    [
        (company, dataset)
        for company in ("retailmax", "freshmart")
        for dataset in (
            "customers",
            "products",
            "stores",
            "sales",
            "inventory",
            "suppliers",
        )
    ],
)
def test_supported_company_dataset(company, dataset):
    source_key = f"incoming/{company}/{dataset}/batch-002.csv"

    plan = plan_batch(source_key)

    assert plan.source_key == source_key
    assert plan.company == company
    assert plan.dataset == dataset
    assert plan.filename == "batch-002.csv"
    assert plan.bronze_table == f"bronze/{company}/{dataset}"
    assert plan.batch_id.startswith("batch-")


def test_batch_id_is_deterministic():
    source_key = "incoming/retailmax/sales/batch-002.csv"

    first = plan_batch(source_key)
    second = plan_batch(source_key)

    assert first.batch_id == second.batch_id


def test_different_files_have_different_batch_ids():
    first = plan_batch("incoming/retailmax/sales/batch-002.csv")
    second = plan_batch("incoming/retailmax/sales/batch-003.csv")

    assert first.batch_id != second.batch_id


@pytest.mark.parametrize(
    "invalid_key",
    [
        "raw/retailmax/sales/batch-002.csv",
        "incoming/unknown/sales/batch-002.csv",
        "incoming/retailmax/unknown/batch-002.csv",
        "incoming/retailmax/sales/batch-002.parquet",
        "incoming/retailmax/sales/../batch-002.csv",
        "incoming/retailmax/sales/subfolder/batch-002.csv",
        "incoming/retailmax/sales/",
        "incoming/retailmax/sales/.csv",
        "",
        None,
    ],
)
def test_reject_invalid_source_keys(invalid_key):
    with pytest.raises(ValueError):
        plan_batch(invalid_key)


def test_batch_plan_is_immutable():
    plan = plan_batch("incoming/freshmart/sales/batch-002.csv")

    with pytest.raises(AttributeError):
        plan.company = "retailmax"
