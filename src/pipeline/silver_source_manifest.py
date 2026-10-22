"""Pinned source identities for the first remaining Silver run."""

from types import MappingProxyType

_SOURCE_RECORDS = (
    ('retailmax', 'products', 'batch-d4fc8d9edbecb06a4f368eb5', 'test-runs/glue-incremental-v1/raw/retailmax/products/initial-001.csv', '8f9485dc99a76fb5879ccdde49a37500', 69945),
    ('freshmart', 'products', 'batch-774b2ba1cc450f2c2dc1beae', 'test-runs/glue-incremental-v1/raw/freshmart/products/initial-001.csv', '3c42d8369edfa2381ac27ef31026e58d', 54514),
    ('retailmax', 'stores', 'batch-2d1b71080ce7b9ca0f8429ee', 'test-runs/glue-incremental-v1/raw/retailmax/stores/initial-001.csv', 'a845459e2e5a4cb5a60ee229d695622d', 3479),
    ('freshmart', 'stores', 'batch-5ad7207c9afad0377257770b', 'test-runs/glue-incremental-v1/raw/freshmart/stores/initial-001.csv', 'ea43fc0bec3eee1d183006b2234cf18d', 2852),
    ('retailmax', 'suppliers', 'batch-88e118fe024096b7b94e1c60', 'test-runs/glue-incremental-v1/raw/retailmax/suppliers/initial-001.csv', '3d679a61c9e492b364e4f07f388d2841', 8781),
    ('freshmart', 'suppliers', 'batch-93e6a50e59bcc307b8fe7829', 'test-runs/glue-incremental-v1/raw/freshmart/suppliers/initial-001.csv', '88481d9c1dd3df2687456a2a0c063475', 7007),
    ('retailmax', 'inventory', 'batch-88b176e668737f76a845071d', 'test-runs/glue-incremental-v1/raw/retailmax/inventory/initial-001.csv', 'c7b1119dc662293a140a5fd47babb029', 97662),
    ('freshmart', 'inventory', 'batch-4e5856a9400a962ca797f710', 'test-runs/glue-incremental-v1/raw/freshmart/inventory/initial-001.csv', '2dcefacd35c4e6d9cffbab9c792ce9aa', 78166),
    ('retailmax', 'sales', 'batch-efedecf4fd371464e6efdc79', 'test-runs/glue-incremental-v1/raw/retailmax/sales/initial-001.csv', '57565ea3f6677bcdad19184936a70034', 3664152),
    ('freshmart', 'sales', 'batch-9c7a25c815e01d80f2470644', 'test-runs/glue-incremental-v1/raw/freshmart/sales/initial-001.csv', '7b836a61f750d268505eb571612f2609', 2846876),
)

EXPECTED_SOURCE_MANIFEST = MappingProxyType({
    (company, dataset): (batch_id, source_key, etag, size_bytes)
    for company, dataset, batch_id, source_key, etag, size_bytes
    in _SOURCE_RECORDS
})


def validate_pinned_source_plan(plan):
    """Reject changed, missing, duplicated, or unexpected source identities."""
    if not isinstance(plan, (list, tuple)):
        raise ValueError("Source plan must be a list or tuple")

    if len(plan) != len(EXPECTED_SOURCE_MANIFEST):
        raise ValueError("STOP: Expected exactly 10 Silver source batches")

    seen = set()

    for batch in plan:
        if not isinstance(batch, dict):
            raise ValueError("STOP: Invalid source batch")

        pair = (batch.get("company"), batch.get("dataset"))

        if pair in seen:
            raise ValueError(f"STOP: Duplicate source batch: {pair}")

        seen.add(pair)

        expected = EXPECTED_SOURCE_MANIFEST.get(pair)

        if expected is None:
            raise ValueError(f"STOP: Unexpected source batch: {pair}")

        actual = (
            batch.get("batch_id"),
            batch.get("source_key"),
            str(batch.get("etag", "")).strip('"'),
            batch.get("size_bytes"),
        )

        if actual != expected:
            raise ValueError(
                f"STOP: Source identity mismatch for {pair}"
            )

    if seen != set(EXPECTED_SOURCE_MANIFEST):
        raise ValueError("STOP: Incomplete source manifest")

    return True
