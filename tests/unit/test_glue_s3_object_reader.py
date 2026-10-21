"""Unit tests for read-only S3 object-version validation."""

from unittest.mock import Mock

import pytest

from src.glue.s3_object_reader import validate_s3_object
from src.glue.object_identity import build_object_batch_id


SOURCE_KEY = "incoming/retailmax/sales/batch-002.csv"
VERSION_ID = "version-001"
BUCKET = "retail-lakehouse-parsam-2026"


@pytest.fixture(autouse=True)
def approved_environment(monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_BUCKET", BUCKET)
    monkeypatch.setenv("LAKEHOUSE_REGION", "us-east-2")
    monkeypatch.setenv(
        "LAKEHOUSE_PREFIX",
        "test-runs/glue-incremental-v1",
    )


@pytest.fixture
def s3_client():
    client = Mock()

    client.head_object.return_value = {
        "VersionId": VERSION_ID,
        "ContentLength": 1024,
        "ETag": '"abc123"',
    }

    return client


def test_valid_object_version(s3_client):
    result = validate_s3_object(
        s3_client, SOURCE_KEY, VERSION_ID
    )

    assert result["bucket"] == BUCKET
    assert result["source_key"] == SOURCE_KEY
    assert result["version_id"] == VERSION_ID
    assert result["company"] == "retailmax"
    assert result["dataset"] == "sales"
    assert result["size_bytes"] == 1024
    assert result["etag"] == '"abc123"'

    assert result["batch_id"] == build_object_batch_id(
        SOURCE_KEY, version_id=VERSION_ID
    )

    s3_client.head_object.assert_called_once_with(
        Bucket=BUCKET,
        Key=SOURCE_KEY,
        VersionId=VERSION_ID,
    )


def test_version_mismatch_rejected(s3_client):
    s3_client.head_object.return_value["VersionId"] = (
        "version-002"
    )

    with pytest.raises(RuntimeError, match="version mismatch"):
        validate_s3_object(
            s3_client, SOURCE_KEY, VERSION_ID
        )


def test_missing_version_in_response_rejected(s3_client):
    del s3_client.head_object.return_value["VersionId"]

    with pytest.raises(RuntimeError, match="version mismatch"):
        validate_s3_object(
            s3_client, SOURCE_KEY, VERSION_ID
        )


@pytest.mark.parametrize("size", [0, -1, None, "1024"])
def test_invalid_object_size_rejected(s3_client, size):
    s3_client.head_object.return_value["ContentLength"] = size

    with pytest.raises(ValueError, match="non-empty"):
        validate_s3_object(
            s3_client, SOURCE_KEY, VERSION_ID
        )


@pytest.mark.parametrize("version", ["", "   ", None, 123])
def test_invalid_version_rejected_before_aws_call(
    s3_client, version
):
    with pytest.raises(ValueError):
        validate_s3_object(
            s3_client, SOURCE_KEY, version
        )

    s3_client.head_object.assert_not_called()


def test_invalid_source_path_rejected_before_aws_call(
    s3_client,
):
    with pytest.raises(ValueError):
        validate_s3_object(
            s3_client,
            "../protected/sales.csv",
            VERSION_ID,
        )

    s3_client.head_object.assert_not_called()


def test_unapproved_bucket_rejected_before_aws_call(
    s3_client, monkeypatch
):
    monkeypatch.setenv(
        "LAKEHOUSE_BUCKET", "unapproved-bucket"
    )

    with pytest.raises(RuntimeError):
        validate_s3_object(
            s3_client, SOURCE_KEY, VERSION_ID
        )

    s3_client.head_object.assert_not_called()


def test_s3_access_failure_propagates(s3_client):
    s3_client.head_object.side_effect = (
        PermissionError("Access denied")
    )

    with pytest.raises(PermissionError, match="Access denied"):
        validate_s3_object(
            s3_client, SOURCE_KEY, VERSION_ID
        )
