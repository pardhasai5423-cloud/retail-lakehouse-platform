"""Local Delta ledger integration for end-to-end coordination."""

from unittest.mock import MagicMock, patch

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from src.glue.delta_processing_ledger import read_batch
from src.glue.jobs.end_to_end_coordinator import execute_end_to_end_batch


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("end-to-end-ledger-integration")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.mark.spark_delta_integration
def test_completed_batch_is_not_reprocessed(spark, tmp_path):
    ledger_path = str(tmp_path / "ledger")
    batch_id = "batch-local-integration"
    bronze_path = str(tmp_path / "bronze" / "sales")
    events = []

    args = {
        "batch_id": batch_id,
        "company": "retailmax",
        "dataset": "sales",
        "source_key": "incoming/retailmax/sales/local.csv",
        "version_id": "local-version-001",
        "ledger_path": ledger_path,
        "bronze_path": bronze_path,
        "silver_base": str(tmp_path / "silver"),
        "gold_base": str(tmp_path / "gold"),
        "expected_sha256": "a" * 64,
        "allow_writes": True,
    }

    def bronze(*a, **kw):
        events.append("bronze")
        return {
            "execution_ready": True,
            "batch_id": batch_id,
            "bronze_target": bronze_path,
        }

    def silver(*a, **kw):
        events.append("silver")
        return {"status": "completed"}

    def gold(*a, **kw):
        events.append("gold")
        return {"status": "completed"}

    with (
        patch(
            "src.glue.jobs.end_to_end_coordinator.plan_bronze_execution",
            return_value={"batch_id": batch_id},
        ),
        patch(
            "src.glue.jobs.end_to_end_coordinator.get_bronze_target",
            return_value=bronze_path,
        ),
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
        first = execute_end_to_end_batch(
            spark, MagicMock(), **args
        )

        assert first["status"] == "completed"
        assert events == ["bronze", "silver", "gold"]

        persisted = read_batch(spark, ledger_path, batch_id)
        assert persisted["status"] == "COMPLETED"
        assert persisted["attempt"] == 1

        second = execute_end_to_end_batch(
            spark, MagicMock(), **args
        )

        assert second["status"] == "skipped"
        assert events == ["bronze", "silver", "gold"]

    print("PASS: Durable ledger, execution order, and retry skip.")


@pytest.mark.spark_delta_integration
def test_failed_silver_batch_recovers_on_retry(spark, tmp_path):
    """A failed batch can recover using its durable Delta ledger."""
    ledger_path = str(tmp_path / "ledger")
    batch_id = "batch-local-recovery"
    bronze_path = str(tmp_path / "bronze" / "sales")
    events = []
    attempts = {"silver": 0}

    args = {
        "batch_id": batch_id,
        "company": "retailmax",
        "dataset": "sales",
        "source_key": "incoming/retailmax/sales/recovery.csv",
        "version_id": "local-version-recovery",
        "ledger_path": ledger_path,
        "bronze_path": bronze_path,
        "silver_base": str(tmp_path / "silver"),
        "gold_base": str(tmp_path / "gold"),
        "expected_sha256": "b" * 64,
        "allow_writes": True,
    }

    def bronze(*a, **kw):
        events.append("bronze")
        return {
            "execution_ready": True,
            "batch_id": batch_id,
            "bronze_target": bronze_path,
        }

    def silver(*a, **kw):
        events.append("silver")
        attempts["silver"] += 1

        if attempts["silver"] == 1:
            raise RuntimeError("simulated Silver failure")

        return {"status": "completed"}

    def gold(*a, **kw):
        events.append("gold")
        return {"status": "completed"}

    with (
        patch(
            "src.glue.jobs.end_to_end_coordinator.plan_bronze_execution",
            return_value={"batch_id": batch_id},
        ),
        patch(
            "src.glue.jobs.end_to_end_coordinator.get_bronze_target",
            return_value=bronze_path,
        ),
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
        with pytest.raises(
            RuntimeError, match="simulated Silver failure"
        ):
            execute_end_to_end_batch(
                spark, MagicMock(), **args
            )

        failed = read_batch(spark, ledger_path, batch_id)
        assert failed["status"] == "FAILED"
        assert failed["attempt"] == 1
        assert events == ["bronze", "silver"]

        result = execute_end_to_end_batch(
            spark, MagicMock(), **args
        )

        assert result["status"] == "completed"

        completed = read_batch(spark, ledger_path, batch_id)
        assert completed["status"] == "COMPLETED"
        assert completed["attempt"] == 2

        assert events == [
            "bronze", "silver",
            "bronze", "silver", "gold",
        ]

    print("PASS: Failed batch recovered on second attempt.")
