"""Unit tests for pinned Silver Delta snapshots."""

from unittest.mock import MagicMock, patch

import pytest

from src.glue.snapshot_contract import (
    current_delta_version,
    read_pinned_snapshot,
    assert_snapshot_unchanged,
)


def test_reads_current_delta_version():
    spark = MagicMock()

    with patch(
        "src.glue.snapshot_contract.DeltaTable"
    ) as delta:
        delta.isDeltaTable.return_value = True
        delta.forPath.return_value.history.return_value \
            .select.return_value.collect.return_value = [
                {"version": 7}
            ]

        assert current_delta_version(spark, "/tmp/silver/sales") == 7


def test_pins_snapshot_version():
    spark = MagicMock()

    read_pinned_snapshot(spark, "/tmp/silver/sales", 5)

    spark.read.format.return_value.option.assert_called_once_with(
        "versionAsOf", 5
    )


def test_rejects_invalid_snapshot_version():
    with pytest.raises(ValueError, match="nonnegative"):
        read_pinned_snapshot(MagicMock(), "/tmp/sales", -1)


def test_rejects_non_delta_source():
    with patch(
        "src.glue.snapshot_contract.DeltaTable.isDeltaTable",
        return_value=False,
    ):
        with pytest.raises(ValueError, match="Not an existing Delta"):
            current_delta_version(MagicMock(), "/tmp/missing")


def test_detects_changed_snapshot():
    with patch(
        "src.glue.snapshot_contract.current_delta_version",
        return_value=8,
    ):
        with pytest.raises(RuntimeError, match="snapshot changed"):
            assert_snapshot_unchanged(
                MagicMock(), "/tmp/sales", 7
            )


def test_accepts_unchanged_snapshot():
    with patch(
        "src.glue.snapshot_contract.current_delta_version",
        return_value=7,
    ):
        assert assert_snapshot_unchanged(
            MagicMock(), "/tmp/sales", 7
        ) == 7
