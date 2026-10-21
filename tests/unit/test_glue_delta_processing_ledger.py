"""Unit tests for durable processing ledger."""

from unittest.mock import MagicMock, patch

import pytest

from src.glue.delta_processing_ledger import (
    register_batch,
    update_batch_status,
)


def test_register_dry_run_does_not_write():
    spark = MagicMock()

    with patch(
        "src.glue.delta_processing_ledger.read_batch",
        return_value=None,
    ):
        result = register_batch(
            spark,
            "/tmp/ledger",
            "batch-001",
            ["sales"],
        )

    assert result["status"] == "PENDING"
    assert result["dry_run"] is True
    spark.createDataFrame.assert_not_called()


def test_registration_is_idempotent():
    existing = {
        "batch_id": "batch-001",
        "datasets": ("sales",),
        "source_versions": {"sales": "v1"},
        "status": "COMPLETED",
        "attempt": 1,
    }

    with patch(
        "src.glue.delta_processing_ledger.read_batch",
        return_value=existing,
    ):
        result = register_batch(
            MagicMock(),
            "/tmp/ledger",
            "batch-001",
            ["sales"],
            {"sales": "v1"},
            allow_writes=True,
        )

    assert result["status"] == "COMPLETED"


def test_batch_identity_cannot_change():
    existing = {
        "batch_id": "batch-001",
        "datasets": ("sales",),
        "source_versions": {"sales": "v1"},
    }

    with patch(
        "src.glue.delta_processing_ledger.read_batch",
        return_value=existing,
    ):
        with pytest.raises(ValueError, match="different source identity"):
            register_batch(
                MagicMock(),
                "/tmp/ledger",
                "batch-001",
                ["sales"],
                {"sales": "v2"},
                allow_writes=True,
            )


def test_unknown_batch_cannot_transition():
    with patch(
        "src.glue.delta_processing_ledger.read_batch",
        return_value=None,
    ):
        with pytest.raises(ValueError, match="Unknown batch_id"):
            update_batch_status(
                MagicMock(),
                "/tmp/ledger",
                "missing",
                "RUNNING",
                allow_writes=True,
            )


def test_transition_dry_run_does_not_write():
    from src.glue.processing_ledger import create_batch_record

    record = create_batch_record("batch-001", ["sales"])
    spark = MagicMock()

    with patch(
        "src.glue.delta_processing_ledger.read_batch",
        return_value=record,
    ):
        result = update_batch_status(
            spark,
            "/tmp/ledger",
            "batch-001",
            "RUNNING",
        )

    assert result["status"] == "RUNNING"
    assert result["dry_run"] is True
    spark.createDataFrame.assert_not_called()


def test_invalid_ledger_path_rejected():
    with pytest.raises(ValueError, match="ledger path"):
        register_batch(
            MagicMock(),
            "",
            "batch-001",
            ["sales"],
        )
