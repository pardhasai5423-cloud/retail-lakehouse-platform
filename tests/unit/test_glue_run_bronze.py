"""Tests for the incremental Bronze Glue job planner."""

import pytest

from src.glue.jobs.run_bronze import build_job_plan


SOURCE_KEY = "incoming/retailmax/sales/batch-002.csv"
APPROVED_PREFIX = "test-runs/glue-incremental-v1"


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
        APPROVED_PREFIX,
    )


def test_valid_versioned_object_plan():
    plan = build_job_plan(
        SOURCE_KEY,
        version_id="version-001",
    )

    assert plan["company"] == "retailmax"
    assert plan["dataset"] == "sales"
    assert plan["dry_run"] is True
    assert plan["transaction_version"] == 1
    assert plan["batch_id"].startswith("batch-")
    assert plan["bronze_target"].endswith(
        "/bronze/retailmax/sales"
    )


def test_same_object_generates_same_plan():
    first = build_job_plan(
        SOURCE_KEY, version_id="version-001"
    )
    second = build_job_plan(
        SOURCE_KEY, version_id="version-001"
    )

    assert first == second


def test_different_versions_generate_different_batches():
    first = build_job_plan(
        SOURCE_KEY, version_id="version-001"
    )
    second = build_job_plan(
        SOURCE_KEY, version_id="version-002"
    )

    assert first["batch_id"] != second["batch_id"]


def test_etag_fallback():
    plan = build_job_plan(
        SOURCE_KEY, etag='"abc123"'
    )

    assert plan["batch_id"].startswith("batch-")


def test_missing_identity_rejected():
    with pytest.raises(ValueError):
        build_job_plan(SOURCE_KEY)


def test_invalid_source_rejected():
    with pytest.raises(ValueError):
        build_job_plan(
            "incoming/unknown/sales/batch-002.csv",
            version_id="version-001",
        )


def test_unapproved_prefix_rejected(monkeypatch):
    monkeypatch.setenv(
        "LAKEHOUSE_PREFIX",
        "test-runs/run-001",
    )

    with pytest.raises(RuntimeError):
        build_job_plan(
            SOURCE_KEY,
            version_id="version-001",
        )


def test_unapproved_bucket_rejected(monkeypatch):
    monkeypatch.setenv(
        "LAKEHOUSE_BUCKET",
        "another-bucket",
    )

    with pytest.raises(RuntimeError):
        build_job_plan(
            SOURCE_KEY,
            version_id="version-001",
        )
