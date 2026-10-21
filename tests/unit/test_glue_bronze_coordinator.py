"""Tests for version-pinned Bronze execution planning."""

from unittest.mock import MagicMock

import pytest

from src.glue.jobs.bronze_coordinator import plan_bronze_execution


SOURCE_KEY = "incoming/retailmax/sales/batch-002.csv"
VERSION_ID = "test-version-001"


@pytest.fixture(autouse=True)
def approved_environment(monkeypatch):
    monkeypatch.setenv(
        "LAKEHOUSE_BUCKET", "retail-lakehouse-parsam-2026"
    )
    monkeypatch.setenv("LAKEHOUSE_REGION", "us-east-2")
    monkeypatch.setenv(
        "LAKEHOUSE_PREFIX", "test-runs/glue-incremental-v1"
    )


def mock_client():
    client = MagicMock()
    client.head_object.return_value = {
        "VersionId": VERSION_ID,
        "ContentLength": 1024,
        "ETag": '"example-etag"',
    }
    return client


def test_valid_execution_plan():
    client = mock_client()

    result = plan_bronze_execution(
        client, SOURCE_KEY, VERSION_ID
    )

    assert result["source_version_id"] == VERSION_ID
    assert result["source_size_bytes"] == 1024
    assert result["company"] == "retailmax"
    assert result["dataset"] == "sales"
    assert result["batch_id"].startswith("batch-")
    assert "/staging/retailmax/sales/" in result["staging_path"]
    assert "/bronze/retailmax/sales" in result["bronze_target"]
    assert result["dry_run"] is True
    assert result["execution_ready"] is False

    client.head_object.assert_called_once()
    client.get_object.assert_not_called()
    client.put_object.assert_not_called()


def test_source_version_mismatch_rejected():
    client = mock_client()
    client.head_object.return_value["VersionId"] = "wrong-version"

    with pytest.raises(RuntimeError):
        plan_bronze_execution(
            client, SOURCE_KEY, VERSION_ID
        )

    client.put_object.assert_not_called()


def test_empty_source_rejected():
    client = mock_client()
    client.head_object.return_value["ContentLength"] = 0

    with pytest.raises(ValueError):
        plan_bronze_execution(
            client, SOURCE_KEY, VERSION_ID
        )


def test_protected_prefix_rejected(monkeypatch):
    client = mock_client()

    monkeypatch.setenv(
        "LAKEHOUSE_PREFIX", "test-runs/run-001"
    )

    with pytest.raises(RuntimeError):
        plan_bronze_execution(
            client, SOURCE_KEY, VERSION_ID
        )

    client.head_object.assert_not_called()


def test_invalid_source_rejected():
    client = mock_client()

    with pytest.raises(ValueError):
        plan_bronze_execution(
            client,
            "incoming/unknown/sales/batch.csv",
            VERSION_ID,
        )

    client.head_object.assert_not_called()
