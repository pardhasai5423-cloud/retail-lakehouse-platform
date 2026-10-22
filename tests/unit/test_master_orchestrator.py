"""Tests for the read-only master pipeline planner."""

import pytest

from src.pipeline.master_orchestrator import (
    build_pipeline_plan,
    COMPANIES,
    DATASETS,
    STAGES,
)


def test_master_pipeline_plan():
    plan = build_pipeline_plan("jenkins-test-001")

    assert plan["run_id"] == "jenkins-test-001"
    assert plan["prefix"] == "test-runs/jenkins-test-001"
    assert plan["execution_enabled"] is False
    assert plan["stages"] == STAGES
    assert len(plan["stages"]) == 6
    assert len(plan["batches"]) == 12


def test_every_company_dataset_pair_is_planned():
    plan = build_pipeline_plan("jenkins-test-001")

    actual = {
        (batch["company"], batch["dataset"])
        for batch in plan["batches"]
    }

    expected = {
        (company, dataset)
        for company in COMPANIES
        for dataset in DATASETS
    }

    assert actual == expected


def test_all_destinations_use_new_run_prefix():
    plan = build_pipeline_plan("jenkins-test-001")
    approved = (
        "s3a://retail-lakehouse-parsam-2026/"
        "test-runs/jenkins-test-001/"
    )

    assert plan["silver_base"].startswith(approved)
    assert plan["gold_base"].startswith(approved)

    for batch in plan["batches"]:
        assert batch["bronze_target"].startswith(approved)
        assert batch["source_prefix"].startswith(
            "test-runs/jenkins-test-001/raw/"
        )


@pytest.mark.parametrize("run_id", [
    "run-001",
    "glue-incremental-v1",
])
def test_completed_runs_are_rejected(run_id):
    with pytest.raises(ValueError):
        build_pipeline_plan(run_id)
