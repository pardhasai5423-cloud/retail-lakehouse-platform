"""Integrated tests for the Bronze execution coordinator."""

from unittest.mock import MagicMock, patch

import pytest

from src.glue.jobs.bronze_execution import execute_bronze_batch


SOURCE_KEY = "incoming/retailmax/sales/batch-002.csv"
VERSION_ID = "test-version-001"
CHECKSUM = "a" * 64

PLAN = {
    "batch_id": "batch-" + "b" * 24,
    "company": "retailmax",
    "dataset": "sales",
    "bronze_target": "s3://example/bronze/retailmax/sales",
    "staging_path": "s3://example/staging/source.csv",
    "dry_run": True,
    "execution_ready": False,
}


@pytest.fixture
def dependencies():
    with (
        patch(
            "src.glue.jobs.bronze_execution.plan_bronze_execution",
            return_value=PLAN.copy(),
        ) as planner,
        patch(
            "src.glue.jobs.bronze_execution.download_s3_object_version"
        ) as downloader,
        patch(
            "src.glue.jobs.bronze_execution.stage_verified_batch"
        ) as stager,
        patch(
            "src.glue.jobs.bronze_execution.verify_staged_object"
        ) as verifier,
        patch(
            "src.glue.jobs.bronze_execution.read_verified_staging_batch"
        ) as reader,
        patch(
            "src.glue.jobs.bronze_execution.prepare_bronze_batch"
        ) as preparer,
        patch(
            "src.glue.jobs.bronze_execution.append_bronze_batch"
        ) as writer,
    ):
        stager.return_value = {
            "staging_path": PLAN["staging_path"]
        }
        verifier.return_value = {
            "staging_path": PLAN["staging_path"],
            "staging_key": "test-runs/glue-incremental-v1/staging/test.csv",
            "staging_version_id": "staging-version-001",
        }
        reader.return_value = (MagicMock(), MagicMock())
        writer.return_value = PLAN["bronze_target"]

        yield {
            "planner": planner,
            "downloader": downloader,
            "stager": stager,
            "verifier": verifier,
            "reader": reader,
            "preparer": preparer,
            "writer": writer,
        }


def test_dry_run_does_not_write(dependencies):
    result = execute_bronze_batch(
        MagicMock(), MagicMock(), SOURCE_KEY, VERSION_ID
    )

    assert result["dry_run"] is True
    dependencies["downloader"].assert_not_called()
    dependencies["stager"].assert_not_called()
    dependencies["writer"].assert_not_called()


@pytest.mark.parametrize(
    "invalid_checksum",
    [None, "", "abc", "z" * 64, 123],
)
def test_execution_requires_checksum(
    dependencies, invalid_checksum
):
    with pytest.raises(ValueError, match="SHA-256"):
        execute_bronze_batch(
            MagicMock(),
            MagicMock(),
            SOURCE_KEY,
            VERSION_ID,
            allow_writes=True,
            expected_sha256=invalid_checksum,
        )

    dependencies["downloader"].assert_not_called()
    dependencies["stager"].assert_not_called()
    dependencies["writer"].assert_not_called()


def test_successful_execution(dependencies):
    data = b"transaction_id,total_amount\nT001,25.00\n"

    def fake_download(
        client, source_key, version_id, destination,
        expected_sha256=None,
    ):
        destination.write_bytes(data)
        return str(destination)

    dependencies["downloader"].side_effect = fake_download

    result = execute_bronze_batch(
        MagicMock(),
        MagicMock(),
        SOURCE_KEY,
        VERSION_ID,
        allow_writes=True,
        expected_sha256=CHECKSUM,
    )

    assert result["dry_run"] is False
    assert result["execution_ready"] is True
    assert result["bronze_target"] == PLAN["bronze_target"]

    dependencies["downloader"].assert_called_once()
    dependencies["stager"].assert_called_once()
    dependencies["reader"].assert_called_once()
    dependencies["preparer"].assert_called_once()
    dependencies["writer"].assert_called_once()


def test_download_failure_stops_pipeline(dependencies):
    dependencies["downloader"].side_effect = IOError(
        "Checksum mismatch"
    )

    with pytest.raises(IOError, match="Checksum mismatch"):
        execute_bronze_batch(
            MagicMock(),
            MagicMock(),
            SOURCE_KEY,
            VERSION_ID,
            allow_writes=True,
            expected_sha256=CHECKSUM,
        )

    dependencies["stager"].assert_not_called()
    dependencies["writer"].assert_not_called()


def test_staging_conflict_stops_pipeline(dependencies):
    dependencies["downloader"].side_effect = (
        lambda client, key, version, destination,
        expected_sha256=None:
        destination.write_bytes(b"valid")
    )

    dependencies["stager"].side_effect = RuntimeError(
        "PreconditionFailed"
    )

    with pytest.raises(RuntimeError, match="PreconditionFailed"):
        execute_bronze_batch(
            MagicMock(),
            MagicMock(),
            SOURCE_KEY,
            VERSION_ID,
            allow_writes=True,
            expected_sha256=CHECKSUM,
        )

    dependencies["reader"].assert_not_called()
    dependencies["writer"].assert_not_called()


def test_delta_failure_propagated(dependencies):
    dependencies["downloader"].side_effect = (
        lambda client, key, version, destination,
        expected_sha256=None:
        destination.write_bytes(b"valid")
    )

    dependencies["writer"].side_effect = RuntimeError(
        "Delta write failed"
    )

    with pytest.raises(RuntimeError, match="Delta write failed"):
        execute_bronze_batch(
            MagicMock(),
            MagicMock(),
            SOURCE_KEY,
            VERSION_ID,
            allow_writes=True,
            expected_sha256=CHECKSUM,
        )

    dependencies["stager"].assert_called_once()
    dependencies["writer"].assert_called_once()


def test_staging_verification_precedes_spark_read(dependencies):
    """Spark must only read a successfully verified staging path."""
    data = b"transaction_id,total_amount\nT001,25.00\n"

    dependencies["downloader"].side_effect = (
        lambda client, key, version, destination,
        expected_sha256=None: destination.write_bytes(data)
    )

    result = execute_bronze_batch(
        MagicMock(),
        MagicMock(),
        SOURCE_KEY,
        VERSION_ID,
        allow_writes=True,
        expected_sha256=CHECKSUM,
    )

    dependencies["verifier"].assert_called_once()
    dependencies["reader"].assert_called_once()

    reader_args = dependencies["reader"].call_args.args

    assert reader_args[2] == SOURCE_KEY
    assert reader_args[3] == PLAN["batch_id"]
    assert reader_args[4] == (
        "test-runs/glue-incremental-v1/staging/test.csv"
    )
    assert reader_args[5] == "staging-version-001"
    assert result["staging_version_id"] == "staging-version-001"


def test_staging_verification_failure_stops_delta_write(dependencies):
    """An invalid staged object must never reach Spark or Delta."""
    dependencies["downloader"].side_effect = (
        lambda client, key, version, destination,
        expected_sha256=None: destination.write_bytes(b"valid")
    )

    dependencies["verifier"].side_effect = RuntimeError(
        "Staged S3 object metadata or size mismatch"
    )

    with pytest.raises(RuntimeError, match="mismatch"):
        execute_bronze_batch(
            MagicMock(),
            MagicMock(),
            SOURCE_KEY,
            VERSION_ID,
            allow_writes=True,
            expected_sha256=CHECKSUM,
        )

    dependencies["reader"].assert_not_called()
    dependencies["writer"].assert_not_called()


def test_staging_path_mismatch_stops_delta_write(dependencies):
    """A different verified path must not be used for ingestion."""
    dependencies["downloader"].side_effect = (
        lambda client, key, version, destination,
        expected_sha256=None: destination.write_bytes(b"valid")
    )

    dependencies["verifier"].return_value = {
        "staging_path": "s3://unexpected/location.csv",
        "staging_version_id": "staging-version-001",
    }

    with pytest.raises(RuntimeError, match="staging path"):
        execute_bronze_batch(
            MagicMock(),
            MagicMock(),
            SOURCE_KEY,
            VERSION_ID,
            allow_writes=True,
            expected_sha256=CHECKSUM,
        )

    dependencies["reader"].assert_not_called()
    dependencies["writer"].assert_not_called()
