"""Unit tests for unified Bronze/Silver/Gold orchestration."""

from unittest.mock import MagicMock, patch, call
import pytest

from src.glue.jobs.end_to_end_coordinator import (
    execute_end_to_end_batch,
)

ARGS = {
    "batch_id": "batch-101",
    "company": "retailmax",
    "dataset": "sales",
    "source_key": "incoming/retailmax/sales/part-001.csv",
    "version_id": "s3-version-001",
    "ledger_path": "/tmp/ledger",
    "bronze_path": "/tmp/bronze/sales",
    "silver_base": "/tmp/silver",
    "gold_base": "/tmp/gold",
}



@pytest.fixture(autouse=True)
def mock_bronze_preflight():
    """Keep coordinator unit tests isolated from real S3 access."""
    with (
        patch(
            "src.glue.jobs.end_to_end_coordinator.plan_bronze_execution",
            return_value={"batch_id": ARGS["batch_id"]},
        ) as plan,
        patch(
            "src.glue.jobs.end_to_end_coordinator.get_bronze_target",
            return_value=ARGS["bronze_path"],
        ) as target,
    ):
        yield plan, target


def invoke(**overrides):
    return execute_end_to_end_batch(
        MagicMock(),
        MagicMock(),
        **(ARGS | overrides),
    )


def test_version_id_required():
    with pytest.raises(ValueError, match="VersionId"):
        invoke(version_id="")


def test_completed_batch_skipped():
    with (
        patch(
            "src.glue.jobs.end_to_end_coordinator.register_batch",
            return_value={"status": "COMPLETED"},
        ),
        patch(
            "src.glue.jobs.end_to_end_coordinator.execute_bronze_batch"
        ) as bronze,
    ):
        result = invoke(allow_writes=True)

    assert result["status"] == "skipped"
    bronze.assert_not_called()


def test_running_batch_rejected():
    with patch(
        "src.glue.jobs.end_to_end_coordinator.register_batch",
        return_value={"status": "RUNNING"},
    ):
        with pytest.raises(RuntimeError, match="already RUNNING"):
            invoke(allow_writes=True)


def test_dry_run_skips_all_data_writes():
    with (
        patch(
            "src.glue.jobs.end_to_end_coordinator.register_batch",
            return_value={"status": "PENDING"},
        ),
        patch(
            "src.glue.jobs.end_to_end_coordinator.execute_bronze_batch"
        ) as bronze,
        patch(
            "src.glue.jobs.end_to_end_coordinator.execute_silver_batch"
        ) as silver,
        patch(
            "src.glue.jobs.end_to_end_coordinator.execute_gold_pipeline",
            return_value={"status": "dry_run"},
        ),
        patch(
            "src.glue.jobs.end_to_end_coordinator.update_batch_status"
        ) as ledger,
    ):
        result = invoke()

    assert result["status"] == "dry_run"
    bronze.assert_not_called()
    silver.assert_not_called()
    ledger.assert_not_called()


def test_all_layers_execute_in_order():
    events = []

    def bronze(*args, **kwargs):
        events.append("bronze")
        return {
            "execution_ready": True,
            "batch_id": ARGS["batch_id"],
            "bronze_target": ARGS["bronze_path"],
        }

    def silver(*args, **kwargs):
        events.append("silver")
        return {"status": "completed"}

    def gold(*args, **kwargs):
        events.append("gold")
        return {"status": "completed"}

    with (
        patch(
            "src.glue.jobs.end_to_end_coordinator.register_batch",
            return_value={"status": "PENDING"},
        ),
        patch(
            "src.glue.jobs.end_to_end_coordinator.update_batch_status"
        ) as ledger,
        patch(
            "src.glue.jobs.end_to_end_coordinator.execute_bronze_batch",
            side_effect=bronze,
        ),
        patch(
            "src.glue.jobs.end_to_end_coordinator.execute_silver_batch",
            side_effect=silver,
        ),
        patch(
            "src.glue.jobs.end_to_end_coordinator.execute_gold_pipeline",
            side_effect=gold,
        ),
    ):
        result = invoke(allow_writes=True)

    assert events == ["bronze", "silver", "gold"]
    assert result["status"] == "completed"
    assert [
        item.args[3] for item in ledger.call_args_list
    ] == ["RUNNING", "COMPLETED"]


@pytest.mark.parametrize("failing_stage", ["bronze", "silver", "gold"])
def test_failure_marks_ledger_failed(failing_stage):
    def stage(name):
        if name == failing_stage:
            raise RuntimeError(f"{name} failed")
        if name == "bronze":
            return {
                "execution_ready": True,
                "batch_id": ARGS["batch_id"],
                "bronze_target": ARGS["bronze_path"],
            }
        return {"status": "completed"}

    with (
        patch(
            "src.glue.jobs.end_to_end_coordinator.register_batch",
            return_value={"status": "PENDING"},
        ),
        patch(
            "src.glue.jobs.end_to_end_coordinator.update_batch_status"
        ) as ledger,
        patch(
            "src.glue.jobs.end_to_end_coordinator.execute_bronze_batch",
            side_effect=lambda *a, **k: stage("bronze"),
        ),
        patch(
            "src.glue.jobs.end_to_end_coordinator.execute_silver_batch",
            side_effect=lambda *a, **k: stage("silver"),
        ),
        patch(
            "src.glue.jobs.end_to_end_coordinator.execute_gold_pipeline",
            side_effect=lambda *a, **k: stage("gold"),
        ),
    ):
        with pytest.raises(RuntimeError, match="failed"):
            invoke(allow_writes=True)

    assert ledger.call_args_list[-1].args[3] == "FAILED"


def test_failed_batch_retries():
    with (
        patch(
            "src.glue.jobs.end_to_end_coordinator.register_batch",
            return_value={"status": "FAILED"},
        ),
        patch(
            "src.glue.jobs.end_to_end_coordinator.update_batch_status"
        ),
        patch(
            "src.glue.jobs.end_to_end_coordinator.execute_bronze_batch",
            return_value={
                "execution_ready": True,
                "batch_id": ARGS["batch_id"],
                "bronze_target": ARGS["bronze_path"],
            },
        ),
        patch(
            "src.glue.jobs.end_to_end_coordinator.execute_silver_batch",
            return_value={"status": "completed"},
        ),
        patch(
            "src.glue.jobs.end_to_end_coordinator.execute_gold_pipeline",
            return_value={"status": "completed"},
        ),
    ):
        result = invoke(allow_writes=True)

    assert result["status"] == "completed"


def test_preflight_rejects_mismatched_batch_id(mock_bronze_preflight):
    plan, _ = mock_bronze_preflight
    plan.return_value = {"batch_id": "batch-wrong"}

    with patch(
        "src.glue.jobs.end_to_end_coordinator.register_batch"
    ) as ledger:
        with pytest.raises(ValueError, match="batch ID"):
            invoke(allow_writes=True)

    ledger.assert_not_called()


def test_preflight_rejects_mismatched_bronze_path(
    mock_bronze_preflight,
):
    _, target = mock_bronze_preflight
    target.return_value = "/tmp/unapproved/bronze"

    with patch(
        "src.glue.jobs.end_to_end_coordinator.register_batch"
    ) as ledger:
        with pytest.raises(ValueError, match="Bronze path"):
            invoke(allow_writes=True)

    ledger.assert_not_called()
