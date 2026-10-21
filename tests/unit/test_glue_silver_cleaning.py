"""Verify dispatch to existing Silver business rules."""

from unittest.mock import MagicMock, patch

import pytest

from src.glue.jobs.silver_incremental import (
    DATASETS,
    clean_silver_batch,
)


@pytest.mark.parametrize(
    "dataset",
    [dataset for dataset in DATASETS if dataset != "sales"],
)
def test_cleaning_returns_dataframe(dataset):
    df = MagicMock()

    with (
        patch("importlib.import_module") as importer,
        patch("src.glue.jobs.silver_incremental.F.col") as mock_col,
    ):
        module = MagicMock()
        importer.return_value = module

        result = clean_silver_batch(df, dataset)

        assert result is not None
        importer.assert_called_once_with(
            f"src.silver.clean_{dataset}"
        )

        if dataset in {"products", "stores", "suppliers", "inventory"}:
            mock_col.assert_called_once_with("validation_status")
        else:
            mock_col.assert_not_called()


def test_unsupported_dataset_rejected():
    with pytest.raises(ValueError, match="Unsupported dataset"):
        clean_silver_batch(MagicMock(), "payments")
