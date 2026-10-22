"""Unit tests for isolated pipeline run configuration."""

import pytest

from src.pipeline.run_config import RunConfig


def test_valid_run_config():
    config = RunConfig("jenkins-test-001")

    assert config.prefix == "test-runs/jenkins-test-001"
    assert config.bucket == "retail-lakehouse-parsam-2026"
    assert config.region == "us-east-2"


def test_s3_path_generation():
    config = RunConfig("jenkins-test-001")

    assert config.path("bronze", "sales") == (
        "s3a://retail-lakehouse-parsam-2026/"
        "test-runs/jenkins-test-001/bronze/sales"
    )


@pytest.mark.parametrize("run_id", [
    "run-001",
    "glue-incremental-v1",
    "../run-001",
    "test/run",
    "",
    "INVALID_RUN",
])
def test_invalid_or_protected_run_ids(run_id):
    with pytest.raises(ValueError):
        RunConfig(run_id)


@pytest.mark.parametrize("component", [
    "..",
    ".",
    "../gold",
    "gold/sales",
    "",
    r"gold\sales",
])
def test_unsafe_path_components(component):
    config = RunConfig("jenkins-test-001")

    with pytest.raises(ValueError):
        config.path(component)


def test_unapproved_bucket_rejected():
    with pytest.raises(ValueError):
        RunConfig(
            "jenkins-test-001",
            bucket="another-bucket",
        )


def test_unapproved_region_rejected():
    with pytest.raises(ValueError):
        RunConfig(
            "jenkins-test-001",
            region="us-west-2",
        )
