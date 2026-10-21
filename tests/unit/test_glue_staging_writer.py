"""Unit tests for create-only S3 staging uploads."""

import hashlib
from unittest.mock import MagicMock

import pytest

from src.glue.staging_writer import stage_verified_batch


SOURCE_KEY = "incoming/retailmax/sales/batch-002.csv"
BATCH_ID = "batch-" + "a" * 24
BUCKET = "retail-lakehouse-parsam-2026"
PREFIX = "test-runs/glue-incremental-v1"

EXPECTED_KEY = (
    f"{PREFIX}/staging/retailmax/sales/"
    f"{BATCH_ID}/batch-002.csv"
)


@pytest.fixture(autouse=True)
def approved_environment(monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_BUCKET", BUCKET)
    monkeypatch.setenv("LAKEHOUSE_REGION", "us-east-2")
    monkeypatch.setenv("LAKEHOUSE_PREFIX", PREFIX)


@pytest.fixture
def sample_csv(tmp_path):
    data = b"transaction_id,total_amount\nT001,25.00\n"
    path = tmp_path / "batch-002.csv"
    path.write_bytes(data)
    return path, hashlib.sha256(data).hexdigest()


def test_valid_csv_uploaded_create_only(sample_csv):
    path, checksum = sample_csv

    client = MagicMock()
    client.put_object.return_value = {"ETag": '"example-etag"'}

    result = stage_verified_batch(
        client, SOURCE_KEY, BATCH_ID, path, checksum
    )

    client.put_object.assert_called_once()

    kwargs = client.put_object.call_args.kwargs

    assert kwargs["Bucket"] == BUCKET
    assert kwargs["Key"] == EXPECTED_KEY
    assert kwargs["IfNoneMatch"] == "*"
    assert kwargs["Metadata"]["batch-id"] == BATCH_ID
    assert kwargs["Metadata"]["sha256"] == checksum
    assert kwargs["Metadata"]["source-key"] == SOURCE_KEY

    assert result["staging_path"] == f"s3://{BUCKET}/{EXPECTED_KEY}"
    assert result["sha256"] == checksum
    assert result["batch_id"] == BATCH_ID


def test_uppercase_checksum_accepted(sample_csv):
    path, checksum = sample_csv
    client = MagicMock()

    result = stage_verified_batch(
        client, SOURCE_KEY, BATCH_ID, path, checksum.upper()
    )

    assert result["sha256"] == checksum
    client.put_object.assert_called_once()


def test_incorrect_checksum_rejected(sample_csv):
    path, _ = sample_csv
    client = MagicMock()

    with pytest.raises(ValueError, match="checksum mismatch"):
        stage_verified_batch(
            client, SOURCE_KEY, BATCH_ID, path, "0" * 64
        )

    client.put_object.assert_not_called()


@pytest.mark.parametrize(
    "invalid_checksum",
    ["", "abc", "z" * 64, "a" * 63, None, 123],
)
def test_invalid_checksum_rejected(sample_csv, invalid_checksum):
    path, _ = sample_csv
    client = MagicMock()

    with pytest.raises(ValueError, match="SHA-256"):
        stage_verified_batch(
            client, SOURCE_KEY, BATCH_ID, path, invalid_checksum
        )

    client.put_object.assert_not_called()


def test_missing_local_file_rejected(tmp_path):
    client = MagicMock()

    with pytest.raises(FileNotFoundError):
        stage_verified_batch(
            client,
            SOURCE_KEY,
            BATCH_ID,
            tmp_path / "missing.csv",
            "a" * 64,
        )

    client.put_object.assert_not_called()


def test_invalid_batch_id_rejected(sample_csv):
    path, checksum = sample_csv
    client = MagicMock()

    with pytest.raises(ValueError):
        stage_verified_batch(
            client, SOURCE_KEY, "invalid-batch", path, checksum
        )

    client.put_object.assert_not_called()


def test_protected_prefix_rejected(sample_csv, monkeypatch):
    path, checksum = sample_csv
    client = MagicMock()

    monkeypatch.setenv("LAKEHOUSE_PREFIX", "test-runs/run-001")

    with pytest.raises(RuntimeError):
        stage_verified_batch(
            client, SOURCE_KEY, BATCH_ID, path, checksum
        )

    client.put_object.assert_not_called()


def test_unapproved_bucket_rejected(sample_csv, monkeypatch):
    path, checksum = sample_csv
    client = MagicMock()

    monkeypatch.setenv("LAKEHOUSE_BUCKET", "unapproved-bucket")

    with pytest.raises(RuntimeError):
        stage_verified_batch(
            client, SOURCE_KEY, BATCH_ID, path, checksum
        )

    client.put_object.assert_not_called()


def test_s3_conflict_propagated(sample_csv):
    path, checksum = sample_csv
    client = MagicMock()
    client.put_object.side_effect = RuntimeError(
        "PreconditionFailed: object already exists"
    )

    with pytest.raises(RuntimeError, match="PreconditionFailed"):
        stage_verified_batch(
            client, SOURCE_KEY, BATCH_ID, path, checksum
        )

    client.put_object.assert_called_once()


def test_invalid_source_key_rejected(sample_csv):
    path, checksum = sample_csv
    client = MagicMock()

    with pytest.raises(ValueError):
        stage_verified_batch(
            client,
            "incoming/unknown/sales/batch-002.csv",
            BATCH_ID,
            path,
            checksum,
        )

    client.put_object.assert_not_called()


def test_uploaded_bytes_match_verified_checksum(sample_csv):
    """Upload exactly the bytes that passed SHA-256 validation."""
    path, checksum = sample_csv
    client = MagicMock()

    stage_verified_batch(
        client, SOURCE_KEY, BATCH_ID, path, checksum
    )

    kwargs = client.put_object.call_args.kwargs
    uploaded = kwargs["Body"]

    assert isinstance(uploaded, bytes)
    assert hashlib.sha256(uploaded).hexdigest() == checksum
    assert kwargs["ContentLength"] == len(uploaded)
    assert kwargs["IfNoneMatch"] == "*"


def test_empty_csv_rejected(tmp_path):
    """Never stage an empty source object."""
    path = tmp_path / "empty.csv"
    path.write_bytes(b"")
    client = MagicMock()

    with pytest.raises(ValueError, match="empty"):
        stage_verified_batch(
            client,
            SOURCE_KEY,
            BATCH_ID,
            path,
            hashlib.sha256(b"").hexdigest(),
        )

    client.put_object.assert_not_called()


def test_oversized_csv_rejected(tmp_path):
    """Reject files larger than the configured 50 MiB limit."""
    path = tmp_path / "oversized.csv"

    with path.open("wb") as output:
        output.truncate(50 * 1024 * 1024 + 1)

    client = MagicMock()

    with pytest.raises(ValueError, match="50 MB"):
        stage_verified_batch(
            client,
            SOURCE_KEY,
            BATCH_ID,
            path,
            "a" * 64,
        )

    client.put_object.assert_not_called()


def test_modified_file_checksum_rejected(sample_csv):
    """Reject a file changed after its checksum was calculated."""
    path, original_checksum = sample_csv

    path.write_bytes(
        b"transaction_id,total_amount\nT001,999.00\n"
    )

    client = MagicMock()

    with pytest.raises(ValueError, match="checksum mismatch"):
        stage_verified_batch(
            client,
            SOURCE_KEY,
            BATCH_ID,
            path,
            original_checksum,
        )

    client.put_object.assert_not_called()


def test_invalid_source_cannot_trigger_upload(sample_csv):
    """Malformed source keys must never reach S3."""
    path, checksum = sample_csv
    client = MagicMock()

    with pytest.raises(ValueError):
        stage_verified_batch(
            client,
            "incoming/retailmax/sales/../../protected.csv",
            BATCH_ID,
            path,
            checksum,
        )

    client.put_object.assert_not_called()


def test_existing_identical_object_reused(sample_csv):
    """An identical staged object can be reused safely."""
    from botocore.exceptions import ClientError

    path, checksum = sample_csv
    data = path.read_bytes()
    client = MagicMock()

    client.put_object.side_effect = ClientError(
        {
            "Error": {
                "Code": "PreconditionFailed",
                "Message": "Object already exists",
            }
        },
        "PutObject",
    )

    body = MagicMock()
    body.read.return_value = data

    client.get_object.return_value = {
        "Body": body,
        "ETag": '"existing-etag"',
        "Metadata": {
            "batch-id": BATCH_ID,
            "sha256": checksum,
            "source-key": SOURCE_KEY,
        },
    }

    result = stage_verified_batch(
        client, SOURCE_KEY, BATCH_ID, path, checksum
    )

    assert result["status"] == "reused"
    assert result["sha256"] == checksum

    client.put_object.assert_called_once()
    client.get_object.assert_called_once()
    body.close.assert_called_once()


def test_existing_different_content_rejected(sample_csv):
    """A conflicting staged object must never be reused."""
    from botocore.exceptions import ClientError

    path, checksum = sample_csv
    client = MagicMock()

    client.put_object.side_effect = ClientError(
        {"Error": {"Code": "412", "Message": "Conflict"}},
        "PutObject",
    )

    body = MagicMock()
    body.read.return_value = b"different CSV content"

    client.get_object.return_value = {
        "Body": body,
        "Metadata": {
            "batch-id": BATCH_ID,
            "sha256": checksum,
            "source-key": SOURCE_KEY,
        },
    }

    with pytest.raises(RuntimeError, match="integrity"):
        stage_verified_batch(
            client, SOURCE_KEY, BATCH_ID, path, checksum
        )

    body.close.assert_called_once()


def test_existing_wrong_metadata_rejected(sample_csv):
    """Matching bytes are insufficient when metadata differs."""
    from botocore.exceptions import ClientError

    path, checksum = sample_csv
    client = MagicMock()

    client.put_object.side_effect = ClientError(
        {"Error": {"Code": "PreconditionFailed"}},
        "PutObject",
    )

    body = MagicMock()
    body.read.return_value = path.read_bytes()

    client.get_object.return_value = {
        "Body": body,
        "Metadata": {
            "batch-id": "wrong-batch",
            "sha256": checksum,
            "source-key": SOURCE_KEY,
        },
    }

    with pytest.raises(RuntimeError, match="integrity"):
        stage_verified_batch(
            client, SOURCE_KEY, BATCH_ID, path, checksum
        )


def test_unexpected_s3_error_propagated(sample_csv):
    """Permission and service errors must not be treated as retries."""
    from botocore.exceptions import ClientError

    path, checksum = sample_csv
    client = MagicMock()

    client.put_object.side_effect = ClientError(
        {
            "Error": {
                "Code": "AccessDenied",
                "Message": "Access denied",
            }
        },
        "PutObject",
    )

    with pytest.raises(ClientError):
        stage_verified_batch(
            client, SOURCE_KEY, BATCH_ID, path, checksum
        )

    client.get_object.assert_not_called()
