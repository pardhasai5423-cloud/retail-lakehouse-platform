from unittest.mock import MagicMock, patch

import pytest

from src.glue.jobs.silver_incremental import (
    DATASETS,
    select_bronze_batch,
    standardize_silver_batch,
)


def test_missing_batch_id_rejected():
    df = MagicMock()
    df.columns = ["_batch_id"]

    with pytest.raises(ValueError, match="batch_id"):
        select_bronze_batch(df, "")


def test_missing_bronze_metadata_rejected():
    df = MagicMock()
    df.columns = ["customer_id"]

    with pytest.raises(ValueError, match="_batch_id"):
        select_bronze_batch(df, "batch-test")


@pytest.mark.parametrize("company", ["retailmax", "freshmart"])
@pytest.mark.parametrize("dataset", DATASETS)
def test_existing_mapping_dispatched(company, dataset):
    df = MagicMock()
    df.columns = ["_batch_id"]
    expected = MagicMock()

    module_name = f"src.silver.transform_{dataset}"
    function_name = f"transform_{company}_{dataset}"

    with patch(
        "src.glue.jobs.silver_incremental.select_bronze_batch",
        return_value=df,
    ), patch("importlib.import_module") as importer:
        module = MagicMock()
        getattr(module, function_name).return_value = expected
        importer.return_value = module

        result = standardize_silver_batch(
            df, company, dataset, "batch-test"
        )

    assert result is expected
    importer.assert_called_once_with(module_name)
    getattr(module, function_name).assert_called_once_with(df)


@pytest.mark.parametrize(
    "company,dataset",
    [
        ("unknown", "sales"),
        ("retailmax", "unknown"),
    ],
)
def test_invalid_source_rejected(company, dataset):
    with pytest.raises(ValueError):
        standardize_silver_batch(
            MagicMock(), company, dataset, "batch-test"
        )
