"""Unit tests for S3 object-based batch identity."""

import pytest

from src.glue.object_identity import build_object_batch_id


SOURCE_KEY = "incoming/retailmax/sales/batch-002.csv"


def test_same_version_produces_same_batch_id():
    first = build_object_batch_id(
        SOURCE_KEY, version_id="version-001"
    )
    second = build_object_batch_id(
        SOURCE_KEY, version_id="version-001"
    )

    assert first == second
    assert first.startswith("batch-")


def test_different_versions_produce_different_batch_ids():
    first = build_object_batch_id(
        SOURCE_KEY, version_id="version-001"
    )
    second = build_object_batch_id(
        SOURCE_KEY, version_id="version-002"
    )

    assert first != second


def test_different_files_produce_different_batch_ids():
    first = build_object_batch_id(
        SOURCE_KEY, version_id="version-001"
    )
    second = build_object_batch_id(
        "incoming/freshmart/sales/batch-002.csv",
        version_id="version-001",
    )

    assert first != second


def test_version_id_takes_priority_over_etag():
    first = build_object_batch_id(
        SOURCE_KEY,
        version_id="version-001",
        etag="etag-a",
    )
    second = build_object_batch_id(
        SOURCE_KEY,
        version_id="version-001",
        etag="etag-b",
    )

    assert first == second


def test_etag_fallback_is_deterministic():
    first = build_object_batch_id(
        SOURCE_KEY, etag='"abc123"'
    )
    second = build_object_batch_id(
        SOURCE_KEY, etag="abc123"
    )

    assert first == second


def test_different_etags_produce_different_batch_ids():
    first = build_object_batch_id(
        SOURCE_KEY, etag="abc123"
    )
    second = build_object_batch_id(
        SOURCE_KEY, etag="xyz789"
    )

    assert first != second


def test_missing_object_identity_is_rejected():
    with pytest.raises(ValueError):
        build_object_batch_id(SOURCE_KEY)


@pytest.mark.parametrize(
    "invalid_version",
    ["", "   ", 123],
)
def test_invalid_version_id_is_rejected(invalid_version):
    with pytest.raises(ValueError):
        build_object_batch_id(
            SOURCE_KEY, version_id=invalid_version
        )


@pytest.mark.parametrize(
    "invalid_etag",
    ["", "   ", 123],
)
def test_invalid_etag_is_rejected(invalid_etag):
    with pytest.raises(ValueError):
        build_object_batch_id(
            SOURCE_KEY, etag=invalid_etag
        )


def test_invalid_source_path_is_rejected():
    with pytest.raises(ValueError):
        build_object_batch_id(
            "incoming/unknown/sales/batch-002.csv",
            version_id="version-001",
        )
