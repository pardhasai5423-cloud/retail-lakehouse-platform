"""Deterministic S3 staging destinations for verified CSV batches."""

import re

from src.glue.batch_planner import plan_batch
from src.glue.config import get_glue_s3_path


BATCH_ID_PATTERN = re.compile(r"^batch-[0-9a-f]{24}$")


def get_staging_path(source_key, batch_id):
    """
    Return the approved staging destination for one source object.

    The path is deterministic, allowing retries to locate the
    same staged batch without creating new destinations.
    """

    plan = plan_batch(source_key)

    if (
        not isinstance(batch_id, str)
        or BATCH_ID_PATTERN.fullmatch(batch_id) is None
    ):
        raise ValueError("Invalid S3 object batch ID")

    return get_glue_s3_path(
        "staging",
        plan.company,
        plan.dataset,
        batch_id,
        plan.filename,
    )
