"""Unit tests for AWS Glue configuration safety."""

import pytest

from src.glue.config import (
    get_glue_config,
    get_glue_s3_path,
    GLUE_ALLOWED_PREFIX,
)


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch):
    """Remove inherited settings so each test is isolated."""
    for name in (
        "LAKEHOUSE_BUCKET",
        "LAKEHOUSE_REGION",
        "LAKEHOUSE_PREFIX",
    ):
        monkeypatch.delenv(name, raising=False)


def test_approved_configuration(monkeypatch):
    monkeypatch.setenv(
        "LAKEHOUSE_PREFIX",
        GLUE_ALLOWED_PREFIX,
    )

    config = get_glue_config()

    assert config["bucket"] == "retail-lakehouse-parsam-2026"
    assert config["region"] == "us-east-2"
    assert config["prefix"] == GLUE_ALLOWED_PREFIX


def test_approved_s3_path(monkeypatch):
    monkeypatch.setenv(
        "LAKEHOUSE_PREFIX",
        GLUE_ALLOWED_PREFIX,
    )

    path = get_glue_s3_path(
        "bronze", "retailmax", "sales"
    )

    assert path == (
        "s3://retail-lakehouse-parsam-2026/"
        "test-runs/glue-incremental-v1/"
        "bronze/retailmax/sales"
    )


@pytest.mark.parametrize(
    "unsafe_prefix",
    [
        "test-runs/run-001",
        "test-runs/run-001/bronze",
        "production",
        "",
        "test-runs/glue-incremental-v1/",
    ],
)
def test_reject_unsafe_prefix(monkeypatch, unsafe_prefix):
    monkeypatch.setenv(
        "LAKEHOUSE_PREFIX",
        unsafe_prefix,
    )

    with pytest.raises(RuntimeError):
        get_glue_config()


def test_reject_unapproved_bucket(monkeypatch):
    monkeypatch.setenv(
        "LAKEHOUSE_PREFIX",
        GLUE_ALLOWED_PREFIX,
    )
    monkeypatch.setenv(
        "LAKEHOUSE_BUCKET",
        "unapproved-bucket",
    )

    with pytest.raises(RuntimeError):
        get_glue_config()


def test_reject_unapproved_region(monkeypatch):
    monkeypatch.setenv(
        "LAKEHOUSE_PREFIX",
        GLUE_ALLOWED_PREFIX,
    )
    monkeypatch.setenv(
        "LAKEHOUSE_REGION",
        "us-west-2",
    )

    with pytest.raises(RuntimeError):
        get_glue_config()


@pytest.mark.parametrize(
    "invalid_component",
    [
        "..",
        "../run-001",
        "bronze/../gold",
        "/bronze",
        "bronze/",
        "s3://another-bucket",
        "",
    ],
)
def test_reject_invalid_s3_path(
    monkeypatch, invalid_component
):
    monkeypatch.setenv(
        "LAKEHOUSE_PREFIX",
        GLUE_ALLOWED_PREFIX,
    )

    with pytest.raises(ValueError):
        get_glue_s3_path(invalid_component)
