"""Silver Delta snapshot-version validation for Gold reconciliation."""

from delta.tables import DeltaTable


def current_delta_version(spark, path):
    if not isinstance(path, str) or not path.strip():
        raise ValueError("A nonempty Delta path is required")

    if not DeltaTable.isDeltaTable(spark, path):
        raise ValueError(f"Not an existing Delta table: {path}")

    rows = (
        DeltaTable.forPath(spark, path)
        .history(1)
        .select("version")
        .collect()
    )

    if len(rows) != 1:
        raise RuntimeError("Unable to determine Delta table version")

    return int(rows[0]["version"])


def read_pinned_snapshot(spark, path, version):
    if not isinstance(version, int) or isinstance(version, bool) or version < 0:
        raise ValueError("Snapshot version must be a nonnegative integer")

    return (
        spark.read.format("delta")
        .option("versionAsOf", version)
        .load(path)
    )


def assert_snapshot_unchanged(spark, path, expected_version):
    actual = current_delta_version(spark, path)

    if actual != expected_version:
        raise RuntimeError(
            f"Silver snapshot changed during Gold preparation: "
            f"expected version {expected_version}, found {actual}"
        )

    return actual
