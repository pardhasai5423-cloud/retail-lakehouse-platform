"""Batch processing ledger with explicit state transitions."""

from datetime import datetime, timezone

VALID_STATUSES = frozenset({
    "PENDING",
    "RUNNING",
    "COMPLETED",
    "FAILED",
})

ALLOWED_TRANSITIONS = {
    "PENDING": frozenset({"RUNNING"}),
    "RUNNING": frozenset({"COMPLETED", "FAILED"}),
    "FAILED": frozenset({"RUNNING"}),
    "COMPLETED": frozenset(),
}


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def validate_batch_id(batch_id):
    if not isinstance(batch_id, str) or not batch_id.strip():
        raise ValueError("A nonempty batch_id is required")
    return batch_id.strip()


def create_batch_record(batch_id, datasets, source_versions=None):
    batch_id = validate_batch_id(batch_id)

    if not datasets:
        raise ValueError("At least one dataset is required")

    datasets = tuple(sorted(set(datasets)))

    if any(not isinstance(d, str) or not d.strip() for d in datasets):
        raise ValueError("Dataset names must be nonempty strings")

    return {
        "batch_id": batch_id,
        "datasets": datasets,
        "source_versions": dict(source_versions or {}),
        "status": "PENDING",
        "attempt": 0,
        "created_at": utc_now(),
        "updated_at": utc_now(),
        "error": None,
    }


def transition_batch(record, new_status, error=None):
    if new_status not in VALID_STATUSES:
        raise ValueError(f"Invalid batch status: {new_status}")

    current = record["status"]

    if new_status not in ALLOWED_TRANSITIONS[current]:
        raise ValueError(
            f"Invalid batch transition: {current} -> {new_status}"
        )

    updated = dict(record)
    updated["status"] = new_status
    updated["updated_at"] = utc_now()

    if new_status == "RUNNING":
        updated["attempt"] = record["attempt"] + 1
        updated["error"] = None
    elif new_status == "FAILED":
        if not isinstance(error, str) or not error.strip():
            raise ValueError("FAILED requires an error message")
        updated["error"] = error.strip()
    elif new_status == "COMPLETED":
        updated["error"] = None

    return updated


def should_process(record):
    return record["status"] in {"PENDING", "FAILED"}


def is_completed(record):
    return record["status"] == "COMPLETED"
