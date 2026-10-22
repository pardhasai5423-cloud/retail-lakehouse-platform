"""Validated configuration for future isolated lakehouse runs."""

from dataclasses import dataclass
import re

BUCKET = "retail-lakehouse-parsam-2026"
REGION = "us-east-2"

PROTECTED_PREFIXES = frozenset({
    "test-runs/run-001",
    "test-runs/glue-incremental-v1",
})

RUN_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{2,62}$")


@dataclass(frozen=True)
class RunConfig:
    run_id: str
    bucket: str = BUCKET
    region: str = REGION

    def __post_init__(self):
        if not isinstance(self.run_id, str) or not RUN_ID_PATTERN.fullmatch(self.run_id):
            raise ValueError("Invalid run ID")

        if self.bucket != BUCKET:
            raise ValueError("Unapproved S3 bucket")

        if self.region != REGION:
            raise ValueError("Unapproved AWS region")

        if self.prefix in PROTECTED_PREFIXES:
            raise ValueError("Protected run prefix")

    @property
    def prefix(self):
        return f"test-runs/{self.run_id}"

    def path(self, *parts):
        for part in parts:
            if (
                not isinstance(part, str)
                or not part
                or part in (".", "..")
                or "/" in part
                or "\\" in part
                or "://" in part
            ):
                raise ValueError(f"Invalid path component: {part!r}")

        suffix = "/".join(parts)
        base = f"s3a://{self.bucket}/{self.prefix}"
        return f"{base}/{suffix}" if suffix else base
