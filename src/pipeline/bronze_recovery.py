"""Read-only inspection of Bronze Delta transaction commits."""

from src.glue.bronze_writer import get_transaction_identity
from src.pipeline.bronze_writer import get_bronze_target


def inspect_bronze_transaction(
    spark,
    source_key,
    batch_id,
    *,
    target=None,
):
    """
    Inspect the current Delta snapshot for a batch transaction.

    This function does not modify the target or processing ledger.

    A missing transaction marker is not proof that a transaction
    never committed, because Delta transaction retention applies.
    """
    expected_target = get_bronze_target(source_key)

    if target is not None and target != expected_target:
        raise ValueError("Recovery target does not match Bronze destination")

    target = expected_target
    app_id, txn_version = get_transaction_identity(
        source_key,
        batch_id,
    )

    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, target):
        return {
            "status": "TABLE_NOT_FOUND",
            "target": target,
            "batch_id": batch_id,
            "transaction_app_id": app_id,
            "transaction_version": txn_version,
        }

    # Read retained transaction markers from the Delta snapshot.
    jvm = spark._jvm

    delta_log = jvm.org.apache.spark.sql.delta.DeltaLog.forTable(
        spark._jsparkSession,
        target,
    )

    snapshot = delta_log.snapshot()
    transaction = snapshot.transactions().get(app_id)

    if transaction.isDefined():
        committed_version = int(transaction.get())

        return {
            "status": (
                "COMMITTED"
                if committed_version >= txn_version
                else "NOT_CONFIRMED"
            ),
            "target": target,
            "batch_id": batch_id,
            "transaction_app_id": app_id,
            "transaction_version": txn_version,
            "recorded_version": committed_version,
        }

    return {
        "status": "NOT_CONFIRMED",
        "target": target,
        "batch_id": batch_id,
        "transaction_app_id": app_id,
        "transaction_version": txn_version,
        "reason": "No retained transaction marker in current snapshot",
    }
