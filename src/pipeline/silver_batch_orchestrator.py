"""Ordered, fail-fast orchestration of remaining Silver batches."""

from src.pipeline.silver_coordinator import (
    execute_isolated_silver_batch,
)
from src.pipeline.silver_safety import (
    get_silver_target,
    require_silver_write_approval,
)

DATASET_ORDER = (
    "products",
    "stores",
    "suppliers",
    "inventory",
    "sales",
)

COMPANY_ORDER = ("retailmax", "freshmart")

EXPECTED_PAIRS = frozenset(
    (company, dataset)
    for dataset in DATASET_ORDER
    for company in COMPANY_ORDER
)

APPROVED_DESTINATIONS = frozenset(
    get_silver_target(dataset)
    for dataset in (*DATASET_ORDER, "sales_quarantine")
)


def build_remaining_silver_plan(batches):
    """Validate and order the exact ten remaining source batches."""
    if not isinstance(batches, (list, tuple)):
        raise ValueError("Expected a list or tuple of discovered batches")

    indexed = {}

    for batch in batches:
        if not isinstance(batch, dict):
            raise ValueError("Every source batch must be a dictionary")

        pair = (batch.get("company"), batch.get("dataset"))

        if pair not in EXPECTED_PAIRS:
            if pair[1] == "customers":
                continue
            raise ValueError(f"Unexpected Silver source: {pair!r}")

        if pair in indexed:
            raise ValueError(f"Duplicate source batch: {pair!r}")

        indexed[pair] = batch

    if set(indexed) != EXPECTED_PAIRS:
        missing = EXPECTED_PAIRS - set(indexed)
        raise ValueError(f"Missing Silver source batches: {sorted(missing)}")

    return [
        indexed[(company, dataset)]
        for dataset in DATASET_ORDER
        for company in COMPANY_ORDER
    ]


def execute_remaining_silver_batches(
    spark,
    batches,
    *,
    allow_writes=False,
):
    """Run batches sequentially; never retry automatically on failure."""
    plan = build_remaining_silver_plan(batches)

    if not isinstance(allow_writes, bool):
        raise ValueError("allow_writes must be boolean")

    if allow_writes:
        for destination in sorted(APPROVED_DESTINATIONS):
            require_silver_write_approval(
                destination,
                allow_writes=True,
            )

    results = []

    for index, batch in enumerate(plan, start=1):
        label = f"{batch['company']}/{batch['dataset']}"

        print(
            f"[{index}/{len(plan)}] START {label}",
            flush=True,
        )

        try:
            result = execute_isolated_silver_batch(
                spark,
                batch,
                allow_writes=allow_writes,
            )

            expected_status = (
                "completed" if allow_writes else "dry_run"
            )

            if result["execution"]["status"] != expected_status:
                raise RuntimeError(
                    f"Unexpected execution status for {label}"
                )

        except Exception:
            print(
                f"STOP: {label} failed. "
                "Do not automatically rerun the batch.",
                flush=True,
            )
            raise

        results.append(result)

        print(
            f"[{index}/{len(plan)}] "
            f"{result['execution']['status'].upper()} {label}",
            flush=True,
        )

    return results


def execute_first_silver_run(
    spark,
    s3_client,
    batches,
    *,
    allow_writes=False,
):
    """Validate the initial state before running remaining Silver batches.

    Intended only for the first execution, when all six destinations
    must be empty. Never automatically retry a partially completed run.
    """
    if allow_writes is not True:
        raise PermissionError(
            "STOP: Explicit allow_writes=True is required"
        )

    # Validate the complete source plan before accessing destination state.
    plan = build_remaining_silver_plan(batches)

    from src.pipeline.silver_source_manifest import (
        validate_pinned_source_plan,
    )
    from src.pipeline.safety import BUCKET
    from src.pipeline.silver_prewrite import (
        validate_first_silver_run_state,
    )

    validate_pinned_source_plan(plan)

    for batch in plan:
        source_key = batch["source_key"]

        metadata = s3_client.head_object(
            Bucket=BUCKET,
            Key=source_key,
        )

        actual_etag = metadata["ETag"].strip('"')
        expected_etag = batch["etag"].strip('"')

        if actual_etag != expected_etag:
            raise RuntimeError(
                f"STOP: Source ETag changed: {source_key}"
            )

        if metadata["ContentLength"] != batch["size_bytes"]:
            raise RuntimeError(
                f"STOP: Source size changed: {source_key}"
            )

    print(
        "PASS: All 10 pinned source identities and "
        "live S3 metadata verified",
        flush=True,
    )

    state = validate_first_silver_run_state(
        spark,
        s3_client,
    )

    if state["status"] != "ready":
        raise RuntimeError(
            "STOP: Silver pre-write state validation did not pass"
        )

    print(
        "PASS: First-run safety gate passed. "
        "Starting ordered Silver execution.",
        flush=True,
    )

    return execute_remaining_silver_batches(
        spark,
        plan,
        allow_writes=True,
    )
