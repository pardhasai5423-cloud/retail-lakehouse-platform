"""Tests for staged S3 object verification."""

from unittest.mock import MagicMock

import pytest

from src.glue.staging_verifier import verify_staged_object


SOURCE = "incoming/retailmax/sales/batch-002.csv"
BATCH = "batch-" + "a" * 24
CHECKSUM = "b" * 64
SIZE = 1024
BUCKET = "retail-lakehouse-parsam-2026"
PREFIX = "test-runs/glue-incremental-v1"


@pytest.fixture(autouse=True)
def environment(monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_BUCKET", BUCKET)
    monkeypatch.setenv("LAKEHOUSE_REGION", "us-east-2")
    monkeypatch.setenv("LAKEHOUSE_PREFIX", PREFIX)


def client():
    mock = MagicMock()
    mock.head_object.return_value = {
        "ContentLength": SIZE,
        "VersionId": "staging-version-001",
        "Metadata": {
            "batch-id": BATCH,
            "sha256": CHECKSUM,
            "source-key": SOURCE,
        },
    }
    return mock


def test_valid_staging_object():
    s3 = client()

    result = verify_staged_object(
        s3, SOURCE, BATCH, CHECKSUM, SIZE
    )

    assert result["staging_version_id"] == "staging-version-001"
    assert result["sha256"] == CHECKSUM
    assert result["size_bytes"] == SIZE
    assert result["staging_path"].startswith(f"s3://{BUCKET}/")

    s3.head_object.assert_called_once()


@pytest.mark.parametrize(
    "field,value",
    [
        ("ContentLength", 999),
        ("VersionId", None),
        ("VersionId", ""),
    ],
)
def test_invalid_staging_properties(field, value):
    s3 = client()
    s3.head_object.return_value[field] = value

    with pytest.raises(RuntimeError):
        verify_staged_object(
            s3, SOURCE, BATCH, CHECKSUM, SIZE
        )


@pytest.mark.parametrize(
    "field,value",
    [
        ("batch-id", "wrong-batch"),
        ("sha256", "c" * 64),
        ("source-key", "wrong/source.csv"),
    ],
)
def test_invalid_metadata(field, value):
    s3 = client()
    s3.head_object.return_value["Metadata"][field] = value

    with pytest.raises(RuntimeError, match="mismatch"):
        verify_staged_object(
            s3, SOURCE, BATCH, CHECKSUM, SIZE
        )


def test_invalid_checksum_rejected():
    s3 = client()

    with pytest.raises(ValueError):
        verify_staged_object(
            s3, SOURCE, BATCH, "invalid", SIZE
        )

    s3.head_object.assert_not_called()


def test_invalid_size_rejected():
    s3 = client()

    with pytest.raises(ValueError):
        verify_staged_object(
            s3, SOURCE, BATCH, CHECKSUM, 0
        )

    s3.head_object.assert_not_called()


def test_protected_prefix_rejected(monkeypatch):
    s3 = client()
    monkeypatch.setenv("LAKEHOUSE_PREFIX", "test-runs/run-001")

    with pytest.raises(RuntimeError):
        verify_staged_object(
            s3, SOURCE, BATCH, CHECKSUM, SIZE
        )

    s3.head_object.assert_not_called()
