"""Validate and plan incoming retail lakehouse batches."""

import hashlib
import re
from dataclasses import dataclass

COMPANIES = frozenset({"retailmax", "freshmart"})

DATASETS = frozenset({
    "customers",
    "products",
    "stores",
    "sales",
    "inventory",
    "suppliers",
})

FILE_PATTERN = re.compile(
    r"^incoming/"
    r"(retailmax|freshmart)/"
    r"(customers|products|stores|sales|inventory|suppliers)/"
    r"([A-Za-z0-9][A-Za-z0-9_-]*\.csv)$"
)


@dataclass(frozen=True)
class BatchPlan:
    source_key: str
    company: str
    dataset: str
    filename: str
    batch_id: str
    bronze_table: str


def plan_batch(source_key: str) -> BatchPlan:
    """Create a deterministic plan for one incoming CSV."""

    if not isinstance(source_key, str):
        raise ValueError("Source key must be a string")

    match = FILE_PATTERN.fullmatch(source_key)

    if match is None:
        raise ValueError(
            f"Unsupported incoming file path: {source_key!r}"
        )

    company, dataset, filename = match.groups()

    digest = hashlib.sha256(
        source_key.encode("utf-8")
    ).hexdigest()[:16]

    return BatchPlan(
        source_key=source_key,
        company=company,
        dataset=dataset,
        filename=filename,
        batch_id=f"batch-{digest}",
        bronze_table=f"bronze/{company}/{dataset}",
    )
