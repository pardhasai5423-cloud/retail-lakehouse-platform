"""Prepare a read-only manifest of initial CSV batches."""

import hashlib
from pathlib import Path

from src.glue.batch_planner import plan_batch
from src.pipeline.safety import ALLOWED_PREFIX

COMPANIES = ("retailmax", "freshmart")
DATASETS = (
    "customers",
    "products",
    "stores",
    "sales",
    "inventory",
    "suppliers",
)


def build_initial_manifest(source_root="data/source_dirty"):
    """Describe initial CSV inputs without uploading or modifying them."""
    root = Path(source_root)
    manifest = []

    for company in COMPANIES:
        for dataset in DATASETS:
            local_path = root / company / f"{dataset}.csv"

            if not local_path.is_file():
                raise FileNotFoundError(local_path)

            source_key = (
                f"{ALLOWED_PREFIX}/raw/"
                f"{company}/{dataset}/initial-001.csv"
            )

            plan_batch(source_key)

            digest = hashlib.sha256()
            size = 0

            with local_path.open("rb") as file:
                for chunk in iter(lambda: file.read(1024 * 1024), b""):
                    digest.update(chunk)
                    size += len(chunk)

            if size == 0:
                raise ValueError(f"Empty source file: {local_path}")

            manifest.append({
                "company": company,
                "dataset": dataset,
                "local_path": str(local_path),
                "source_key": source_key,
                "size_bytes": size,
                "sha256": digest.hexdigest(),
            })

    return manifest
