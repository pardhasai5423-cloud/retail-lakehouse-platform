from unittest.mock import MagicMock

import pytest

from src.config.settings import (
    get_s3_path,
    require_safe_write_environment,
)
from src.config.write_safety import (
    require_new_local_target,
    require_new_s3_target,
)


@pytest.fixture
def approved_environment(monkeypatch):
    monkeypatch.setenv(
        "LAKEHOUSE_PREFIX",
        "test-runs/run-001",
    )
    monkeypatch.setenv(
        "LAKEHOUSE_BUCKET",
        "retail-lakehouse-parsam-2026",
    )


def test_s3_path_generation(approved_environment):
    path = get_s3_path("silver", "customers")

    assert path == (
        "s3a://retail-lakehouse-parsam-2026/"
        "test-runs/run-001/silver/customers"
    )


@pytest.mark.parametrize(
    "prefix",
    [
        "",
        "production",
        "test-runs/run-002",
        "/test-runs/run-001",
        "test-runs/run-001/",
        "test-runs/run-001/extra",
    ],
)
def test_unsafe_prefix_rejected(monkeypatch, prefix):
    monkeypatch.setenv("LAKEHOUSE_PREFIX", prefix)

    with pytest.raises(RuntimeError, match="DATA WRITE BLOCKED"):
        require_safe_write_environment()


def test_approved_prefix_accepted(approved_environment):
    require_safe_write_environment()


def test_existing_local_target_rejected(tmp_path):
    target = tmp_path / "existing"
    target.mkdir()

    with pytest.raises(RuntimeError, match="LOCAL WRITE BLOCKED"):
        require_new_local_target(target)


def test_new_local_target_accepted(tmp_path):
    target = tmp_path / "new_output"

    result = require_new_local_target(target)

    assert result == target
    assert not target.exists()


def test_s3_wrong_bucket_rejected(approved_environment):
    spark = MagicMock()

    with pytest.raises(RuntimeError, match="S3 WRITE BLOCKED"):
        require_new_s3_target(
            spark,
            "s3a://wrong-bucket/test-runs/run-001/silver/customers",
        )

    spark._jvm.org.apache.hadoop.fs.Path.assert_not_called()


def test_s3_wrong_prefix_rejected(approved_environment):
    spark = MagicMock()

    with pytest.raises(RuntimeError, match="S3 WRITE BLOCKED"):
        require_new_s3_target(
            spark,
            "s3a://retail-lakehouse-parsam-2026/production/customers",
        )

    spark._jvm.org.apache.hadoop.fs.Path.assert_not_called()


def test_existing_s3_target_rejected(approved_environment):
    spark = MagicMock()

    filesystem = (
        spark._jvm.org.apache.hadoop.fs.Path.return_value
        .getFileSystem.return_value
    )

    filesystem.exists.return_value = True

    with pytest.raises(RuntimeError, match="S3 WRITE BLOCKED"):
        require_new_s3_target(
            spark,
            "s3a://retail-lakehouse-parsam-2026/"
            "test-runs/run-001/silver/customers",
        )


def test_new_s3_target_accepted(approved_environment):
    spark = MagicMock()

    filesystem = (
        spark._jvm.org.apache.hadoop.fs.Path.return_value
        .getFileSystem.return_value
    )

    filesystem.exists.return_value = False

    path = (
        "s3a://retail-lakehouse-parsam-2026/"
        "test-runs/run-001/silver/customers"
    )

    assert require_new_s3_target(spark, path) == path
