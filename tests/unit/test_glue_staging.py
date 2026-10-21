"""Tests for deterministic and safe S3 staging destinations."""

import pytest

from src.glue.staging import get_staging_path


SOURCE_KEY = "incoming/retailmax/sales/batch-002.csv"
BATCH_A = "batch-" + "a" * 24
BATCH_B = "batch-" + "b" * 24

APPROVED_ROOT = (
    "s3://retail-lakehouse-parsam-2026/"
    "test-runs/glue-incremental-v1/"
)


@pytest.fixture(autouse=True)
def approved_environment(monkeypatch):
    monkeypatch.setenv(
        "LAKEHOUSE_BUCKET",
        "retail-lakehouse-parsam-2026",
    )
    monkeypatch.setenv(
        "LAKEHOUSE_REGION",
        "us-east-2",
    )
    monkeypatch.setenv(
        "LAKEHOUSE_PREFIX",
        "test-runs/glue-incremental-v1",
    )


def test_retailmax_staging_path():
    path = get_staging_path(SOURCE_KEY, BATCH_A)

    assert path == (
        APPROVED_ROOT
        + "staging/retailmax/sales/"
        + BATCH_A
        + "/batch-002.csv"
    )


def test_freshmart_staging_path():
    path = get_staging_path(
        "incoming/freshmart/customers/batch-003.csv",
        BATCH_A,
    )

    assert path.startswith(APPROVED_ROOT)
    assert path.endswith(
        "/staging/freshmart/customers/"
        + BATCH_A
        + "/batch-003.csv"
    )


def test_same_batch_has_stable_path():
    first = get_staging_path(SOURCE_KEY, BATCH_A)
    second = get_staging_path(SOURCE_KEY, BATCH_A)

    assert first == second


def test_different_batches_have_different_paths():
    first = get_staging_path(SOURCE_KEY, BATCH_A)
    second = get_staging_path(SOURCE_KEY, BATCH_B)

    assert first != second


@pytest.mark.parametrize(
    "invalid_batch",
    [
        "",
        None,
        "batch-123",
        "batch-" + "z" * 24,
        "batch-" + "A" * 24,
    ],
)
def test_invalid_batch_ids_rejected(invalid_batch):
    with pytest.raises(ValueError):
        get_staging_path(SOURCE_KEY, invalid_batch)


@pytest.mark.parametrize(
    "invalid_source",
    [
        "../protected/data.csv",
        "incoming/unknown/sales/batch-002.csv",
        "incoming/retailmax/sales/../../data.csv",
    ],
)
def test_invalid_source_paths_rejected(invalid_source):
    with pytest.raises(ValueError):
        get_staging_path(invalid_source, BATCH_A)


def test_protected_prefix_rejected(monkeypatch):
    monkeypatch.setenv(
        "LAKEHOUSE_PREFIX",
        "test-runs/run-001",
    )

    with pytest.raises(RuntimeError):
        get_staging_path(SOURCE_KEY, BATCH_A)


def test_missing_prefix_rejected(monkeypatch):
    monkeypatch.delenv(
        "LAKEHOUSE_PREFIX",
        raising=False,
    )

    with pytest.raises(RuntimeError):
        get_staging_path(SOURCE_KEY, BATCH_A)


def test_unapproved_bucket_rejected(monkeypatch):
    monkeypatch.setenv(
        "LAKEHOUSE_BUCKET",
        "another-bucket",
    )

    with pytest.raises(RuntimeError):
        get_staging_path(SOURCE_KEY, BATCH_A)


def test_unapproved_region_rejected(monkeypatch):
    monkeypatch.setenv(
        "LAKEHOUSE_REGION",
        "us-west-2",
    )

    with pytest.raises(RuntimeError):
        get_staging_path(SOURCE_KEY, BATCH_A)
