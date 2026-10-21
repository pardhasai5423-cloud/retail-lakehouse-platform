"""Tests for version-pinned S3 object downloads."""

from io import BytesIO
from unittest.mock import Mock

import pytest

from src.glue.s3_object_reader import download_s3_object_version


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


def mock_s3_response(data=b"header\nvalue\n", version=VERSION_ID):
    client = Mock()

    body = Mock()
    body.iter_chunks.return_value = iter([data])
    body.close = Mock()

    client.get_object.return_value = {
        "VersionId": version,
        "ContentLength": len(data),
        "Body": body,
    }

    return client, body


def test_download_exact_version(tmp_path):
    client, body = mock_s3_response()
    destination = tmp_path / "batch.csv"

    result = download_s3_object_version(
        client, SOURCE_KEY, VERSION_ID, destination
    )

    assert result == str(destination)
    assert destination.read_bytes() == b"header\nvalue\n"

    client.get_object.assert_called_once_with(
        Bucket=BUCKET,
        Key=SOURCE_KEY,
        VersionId=VERSION_ID,
    )

    body.close.assert_called_once()


def test_version_mismatch_rejected(tmp_path):
    client, body = mock_s3_response(version="version-002")
    destination = tmp_path / "batch.csv"

    with pytest.raises(RuntimeError, match="does not match"):
        download_s3_object_version(
            client, SOURCE_KEY, VERSION_ID, destination
        )

    assert not destination.exists()
    body.close.assert_called_once()


def test_existing_destination_rejected(tmp_path):
    client, _ = mock_s3_response()
    destination = tmp_path / "batch.csv"
    destination.write_text("existing data")

    with pytest.raises(FileExistsError):
        download_s3_object_version(
            client, SOURCE_KEY, VERSION_ID, destination
        )

    assert destination.read_text() == "existing data"
    client.get_object.assert_not_called()


def test_missing_destination_directory_rejected(tmp_path):
    client, _ = mock_s3_response()
    destination = tmp_path / "missing" / "batch.csv"

    with pytest.raises(FileNotFoundError):
        download_s3_object_version(
            client, SOURCE_KEY, VERSION_ID, destination
        )

    client.get_object.assert_not_called()


def test_partial_download_is_removed(tmp_path):
    client, body = mock_s3_response()
    destination = tmp_path / "batch.csv"

    def failing_chunks(chunk_size):
        yield b"partial data"
        raise OSError("Simulated download failure")

    body.iter_chunks.side_effect = failing_chunks

    with pytest.raises(OSError, match="download failure"):
        download_s3_object_version(
            client, SOURCE_KEY, VERSION_ID, destination
        )

    assert not destination.exists()
    body.close.assert_called_once()


@pytest.mark.parametrize("version", ["", "   ", None, 123])
def test_invalid_version_rejected(tmp_path, version):
    client, _ = mock_s3_response()

    with pytest.raises(ValueError):
        download_s3_object_version(
            client,
            SOURCE_KEY,
            version,
            tmp_path / "batch.csv",
        )

    client.get_object.assert_not_called()


def test_invalid_source_key_rejected(tmp_path):
    client, _ = mock_s3_response()

    with pytest.raises(ValueError):
        download_s3_object_version(
            client,
            "../protected/data.csv",
            VERSION_ID,
            tmp_path / "batch.csv",
        )

    client.get_object.assert_not_called()


def test_unapproved_bucket_rejected(tmp_path, monkeypatch):
    client, _ = mock_s3_response()

    monkeypatch.setenv("LAKEHOUSE_BUCKET", "wrong-bucket")

    with pytest.raises(RuntimeError):
        download_s3_object_version(
            client,
            SOURCE_KEY,
            VERSION_ID,
            tmp_path / "batch.csv",
        )

    client.get_object.assert_not_called()


def test_truncated_download_rejected(tmp_path):
    client, body = mock_s3_response(data=b"short")
    client.get_object.return_value["ContentLength"] = 100
    destination = tmp_path / "truncated.csv"

    with pytest.raises(IOError, match="size mismatch"):
        download_s3_object_version(
            client, SOURCE_KEY, VERSION_ID, destination
        )

    assert not destination.exists()
    body.close.assert_called_once()


def test_oversized_download_rejected(tmp_path):
    client, body = mock_s3_response(data=b"too much data")
    client.get_object.return_value["ContentLength"] = 3
    destination = tmp_path / "oversized.csv"

    with pytest.raises(IOError, match="exceeds expected"):
        download_s3_object_version(
            client, SOURCE_KEY, VERSION_ID, destination
        )

    assert not destination.exists()
    body.close.assert_called_once()


@pytest.mark.parametrize(
    "invalid_size",
    [None, 0, -1, "100"],
)
def test_invalid_content_length_rejected(
    tmp_path, invalid_size
):
    client, body = mock_s3_response()
    client.get_object.return_value[
        "ContentLength"
    ] = invalid_size

    destination = tmp_path / "invalid.csv"

    with pytest.raises(ValueError, match="content length"):
        download_s3_object_version(
            client, SOURCE_KEY, VERSION_ID, destination
        )

    assert not destination.exists()
    body.close.assert_called_once()


def test_valid_sha256_checksum(tmp_path):
    """A matching SHA-256 checksum must be accepted."""
    import hashlib

    data = b"retailmax,sales\nT001,25.00\n"
    expected = hashlib.sha256(data).hexdigest()

    client, body = mock_s3_response(data=data)
    destination = tmp_path / "valid_checksum.csv"

    result = download_s3_object_version(
        client,
        SOURCE_KEY,
        VERSION_ID,
        destination,
        expected_sha256=expected,
    )

    assert result == str(destination)
    assert destination.read_bytes() == data
    body.close.assert_called_once()


def test_invalid_sha256_checksum_rejected(tmp_path):
    """A checksum mismatch must delete the downloaded file."""
    data = b"retailmax,sales\nT001,25.00\n"

    client, body = mock_s3_response(data=data)
    destination = tmp_path / "invalid_checksum.csv"

    with pytest.raises(IOError, match="checksum mismatch"):
        download_s3_object_version(
            client,
            SOURCE_KEY,
            VERSION_ID,
            destination,
            expected_sha256="0" * 64,
        )

    assert not destination.exists()
    body.close.assert_called_once()


def test_uppercase_sha256_checksum_accepted(tmp_path):
    """SHA-256 hexadecimal comparisons are case-insensitive."""
    import hashlib

    data = b"test checksum data"
    expected = hashlib.sha256(data).hexdigest().upper()

    client, _ = mock_s3_response(data=data)
    destination = tmp_path / "uppercase_checksum.csv"

    download_s3_object_version(
        client,
        SOURCE_KEY,
        VERSION_ID,
        destination,
        expected_sha256=expected,
    )

    assert destination.read_bytes() == data


@pytest.mark.parametrize(
    "invalid_checksum",
    [
        "",
        "abc123",
        "z" * 64,
        "a" * 63,
        "a" * 65,
        123,
    ],
)
def test_invalid_sha256_format_rejected(
    tmp_path, invalid_checksum
):
    """Malformed checksums must fail before contacting S3."""

    client, _ = mock_s3_response()
    destination = tmp_path / "invalid_format.csv"

    with pytest.raises(ValueError, match="SHA-256"):
        download_s3_object_version(
            client,
            SOURCE_KEY,
            VERSION_ID,
            destination,
            expected_sha256=invalid_checksum,
        )

    client.get_object.assert_not_called()
    assert not destination.exists()


def test_sha256_verification_is_optional(tmp_path):
    """Existing downloads remain supported without a checksum."""

    data = b"sample csv content"
    client, _ = mock_s3_response(data=data)

    destination = tmp_path / "optional_checksum.csv"

    download_s3_object_version(
        client,
        SOURCE_KEY,
        VERSION_ID,
        destination,
    )

    assert destination.read_bytes() == data
