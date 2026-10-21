"""Unit tests for Bronze Delta destination and transaction safety."""

import pytest

from src.glue.bronze_writer import (
    get_bronze_target,
    get_transaction_identity,
    append_bronze_batch,
)


APPROVED_PREFIX = "test-runs/glue-incremental-v1"

SOURCE_KEY = "incoming/retailmax/sales/batch-002.csv"

BATCH_A = "batch-" + "a" * 24
BATCH_B = "batch-" + "b" * 24


@pytest.fixture
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
        APPROVED_PREFIX,
    )


def test_retailmax_bronze_target(approved_environment):
    target = get_bronze_target(SOURCE_KEY)

    assert target == (
        "s3://retail-lakehouse-parsam-2026/"
        "test-runs/glue-incremental-v1/"
        "bronze/retailmax/sales"
    )


def test_freshmart_bronze_target(approved_environment):
    target = get_bronze_target(
        "incoming/freshmart/customers/batch-003.csv"
    )

    assert target.endswith(
        "/bronze/freshmart/customers"
    )


def test_transaction_identity_is_stable():
    first = get_transaction_identity(SOURCE_KEY, BATCH_A)
    second = get_transaction_identity(SOURCE_KEY, BATCH_A)

    assert first == second
    assert first[1] == 1


def test_different_batches_have_different_identities():
    first = get_transaction_identity(SOURCE_KEY, BATCH_A)
    second = get_transaction_identity(SOURCE_KEY, BATCH_B)

    assert first != second


def test_different_datasets_have_different_identities():
    first = get_transaction_identity(SOURCE_KEY, BATCH_A)

    second = get_transaction_identity(
        "incoming/retailmax/customers/batch-002.csv",
        BATCH_A,
    )

    assert first != second


@pytest.mark.parametrize(
    "invalid_batch_id",
    [
        "",
        "batch-123",
        "batch-" + "z" * 24,
        "batch-" + "A" * 24,
        None,
    ],
)
def test_invalid_batch_id_is_rejected(invalid_batch_id):
    with pytest.raises(ValueError):
        get_transaction_identity(
            SOURCE_KEY, invalid_batch_id
        )


def test_invalid_source_key_is_rejected():
    with pytest.raises(ValueError):
        get_transaction_identity(
            "../protected/sales.csv", BATCH_A
        )


def test_unapproved_prefix_is_blocked(monkeypatch):
    monkeypatch.setenv(
        "LAKEHOUSE_PREFIX",
        "test-runs/run-001",
    )

    with pytest.raises(RuntimeError):
        get_bronze_target(SOURCE_KEY)


def test_missing_prefix_is_blocked(monkeypatch):
    monkeypatch.delenv(
        "LAKEHOUSE_PREFIX",
        raising=False,
    )

    with pytest.raises(RuntimeError):
        get_bronze_target(SOURCE_KEY)


def test_append_is_blocked_before_write(monkeypatch):
    """An unsafe environment must never invoke Spark's writer."""

    monkeypatch.setenv(
        "LAKEHOUSE_PREFIX",
        "test-runs/run-001",
    )

    class NoWriteAllowed:
        @property
        def write(self):
            raise AssertionError(
                "Spark write must not be accessed"
            )

    with pytest.raises(RuntimeError):
        append_bronze_batch(
            NoWriteAllowed(),
            SOURCE_KEY,
            BATCH_A,
        )


def test_delta_writer_uses_safe_append_options(
    approved_environment,
):
    """Verify Delta write configuration without writing data."""

    calls = []

    class FakeWriter:
        def format(self, value):
            calls.append(("format", value))
            return self

        def mode(self, value):
            calls.append(("mode", value))
            return self

        def option(self, name, value):
            calls.append(("option", name, value))
            return self

        def save(self, path):
            calls.append(("save", path))

    class FakeDataFrame:
        @property
        def write(self):
            return FakeWriter()

    target = append_bronze_batch(
        FakeDataFrame(),
        SOURCE_KEY,
        BATCH_A,
    )

    expected_app_id, expected_version = (
        get_transaction_identity(SOURCE_KEY, BATCH_A)
    )

    assert ("format", "delta") in calls
    assert ("mode", "append") in calls
    assert (
        "option",
        "txnAppId",
        expected_app_id,
    ) in calls
    assert (
        "option",
        "txnVersion",
        expected_version,
    ) in calls
    assert ("save", target) in calls

    assert target.startswith(
        "s3://retail-lakehouse-parsam-2026/"
        "test-runs/glue-incremental-v1/"
    )

    assert "test-runs/run-001" not in target


def test_invalid_batch_id_blocks_spark_write(
    approved_environment,
):
    """Reject invalid batch IDs before accessing Spark's writer."""

    class NoWriteAllowed:
        @property
        def write(self):
            raise AssertionError(
                "Spark writer must not be accessed"
            )

    with pytest.raises(ValueError):
        append_bronze_batch(
            NoWriteAllowed(),
            SOURCE_KEY,
            "invalid-batch",
        )
