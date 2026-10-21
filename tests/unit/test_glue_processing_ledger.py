"""Unit tests for processing ledger state transitions."""

import pytest

from src.glue.processing_ledger import (
    create_batch_record,
    transition_batch,
    should_process,
    is_completed,
)


def test_new_batch_starts_pending():
    record = create_batch_record(
        "batch-001",
        ["sales", "customers"],
        {"sales": "version-123"},
    )

    assert record["status"] == "PENDING"
    assert record["attempt"] == 0
    assert record["datasets"] == ("customers", "sales")
    assert record["source_versions"]["sales"] == "version-123"


def test_successful_batch_lifecycle():
    record = create_batch_record("batch-001", ["sales"])

    record = transition_batch(record, "RUNNING")
    assert record["attempt"] == 1

    record = transition_batch(record, "COMPLETED")

    assert is_completed(record)
    assert not should_process(record)


def test_failed_batch_can_retry():
    record = create_batch_record("batch-002", ["inventory"])

    record = transition_batch(record, "RUNNING")
    record = transition_batch(record, "FAILED", "Temporary failure")

    assert should_process(record)
    assert record["error"] == "Temporary failure"

    record = transition_batch(record, "RUNNING")

    assert record["attempt"] == 2
    assert record["error"] is None


def test_completed_batch_cannot_restart():
    record = create_batch_record("batch-003", ["products"])
    record = transition_batch(record, "RUNNING")
    record = transition_batch(record, "COMPLETED")

    with pytest.raises(ValueError, match="Invalid batch transition"):
        transition_batch(record, "RUNNING")


def test_running_batch_cannot_start_again():
    record = create_batch_record("batch-004", ["sales"])
    record = transition_batch(record, "RUNNING")

    with pytest.raises(ValueError, match="Invalid batch transition"):
        transition_batch(record, "RUNNING")


def test_failed_status_requires_error():
    record = create_batch_record("batch-005", ["stores"])
    record = transition_batch(record, "RUNNING")

    with pytest.raises(ValueError, match="requires an error"):
        transition_batch(record, "FAILED")


@pytest.mark.parametrize("batch_id", ["", "   ", None])
def test_invalid_batch_ids_rejected(batch_id):
    with pytest.raises(ValueError):
        create_batch_record(batch_id, ["sales"])


def test_empty_dataset_list_rejected():
    with pytest.raises(ValueError, match="At least one dataset"):
        create_batch_record("batch-006", [])


def test_unknown_status_rejected():
    record = create_batch_record("batch-007", ["sales"])

    with pytest.raises(ValueError, match="Invalid batch status"):
        transition_batch(record, "UNKNOWN")


def test_pending_batch_should_process():
    record = create_batch_record("batch-008", ["sales"])

    assert should_process(record)
    assert not is_completed(record)
