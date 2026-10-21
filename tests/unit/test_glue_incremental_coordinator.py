"""Unit tests for ledger-controlled Gold execution."""

from unittest.mock import MagicMock, patch

import pytest

from src.glue.jobs.incremental_coordinator import (
    execute_incremental_batch,
)


BASE = {
    "batch_id": "batch-001",
    "changed_datasets": ["sales"],
    "source_versions": {"sales": "v1"},
    "ledger_path": "/tmp/ledger",
    "silver_base": "/tmp/silver",
    "gold_base": "/tmp/gold",
}


def test_completed_batch_is_skipped():
    with (
        patch(
            "src.glue.jobs.incremental_coordinator.register_batch",
            return_value={"status": "COMPLETED"},
        ),
        patch(
            "src.glue.jobs.incremental_coordinator.execute_gold_pipeline"
        ) as pipeline,
    ):
        result = execute_incremental_batch(
            MagicMock(), **BASE, allow_writes=True
        )

    assert result["status"] == "skipped"
    pipeline.assert_not_called()


def test_running_batch_is_rejected():
    with patch(
        "src.glue.jobs.incremental_coordinator.register_batch",
        return_value={"status": "RUNNING"},
    ):
        with pytest.raises(RuntimeError, match="already RUNNING"):
            execute_incremental_batch(
                MagicMock(), **BASE, allow_writes=True
            )


def test_dry_run_does_not_transition_ledger():
    with (
        patch(
            "src.glue.jobs.incremental_coordinator.register_batch",
            return_value={"status": "PENDING"},
        ),
        patch(
            "src.glue.jobs.incremental_coordinator.update_batch_status"
        ) as transition,
        patch(
            "src.glue.jobs.incremental_coordinator.execute_gold_pipeline",
            return_value={"status": "dry_run"},
        ),
    ):
        result = execute_incremental_batch(
            MagicMock(), **BASE
        )

    assert result["status"] == "dry_run"
    transition.assert_not_called()


def test_successful_batch_completes():
    with (
        patch(
            "src.glue.jobs.incremental_coordinator.register_batch",
            return_value={"status": "PENDING"},
        ),
        patch(
            "src.glue.jobs.incremental_coordinator.update_batch_status"
        ) as transition,
        patch(
            "src.glue.jobs.incremental_coordinator.execute_gold_pipeline",
            return_value={"status": "completed"},
        ),
    ):
        result = execute_incremental_batch(
            MagicMock(), **BASE, allow_writes=True
        )

    assert result["status"] == "completed"
    assert [
        call.args[3] for call in transition.call_args_list
    ] == ["RUNNING", "COMPLETED"]


def test_failed_pipeline_marks_batch_failed():
    with (
        patch(
            "src.glue.jobs.incremental_coordinator.register_batch",
            return_value={"status": "PENDING"},
        ),
        patch(
            "src.glue.jobs.incremental_coordinator.update_batch_status"
        ) as transition,
        patch(
            "src.glue.jobs.incremental_coordinator.execute_gold_pipeline",
            side_effect=RuntimeError("Simulated Spark failure"),
        ),
    ):
        with pytest.raises(RuntimeError, match="Simulated Spark failure"):
            execute_incremental_batch(
                MagicMock(), **BASE, allow_writes=True
            )

    assert transition.call_args_list[-1].args[3] == "FAILED"
    assert (
        transition.call_args_list[-1].kwargs["error"]
        == "Simulated Spark failure"
    )


def test_failed_batch_can_retry():
    with (
        patch(
            "src.glue.jobs.incremental_coordinator.register_batch",
            return_value={"status": "FAILED"},
        ),
        patch(
            "src.glue.jobs.incremental_coordinator.update_batch_status"
        ),
        patch(
            "src.glue.jobs.incremental_coordinator.execute_gold_pipeline",
            return_value={"status": "completed"},
        ),
    ):
        result = execute_incremental_batch(
            MagicMock(), **BASE, allow_writes=True
        )

    assert result["status"] == "completed"
