"""Version-pinned CSV ingestion tests using mocked S3."""

import hashlib
from unittest.mock import MagicMock

import pytest

from src.glue.versioned_spark_input import (
    read_verified_staging_batch,
)


BUCKET = "retail-lakehouse-parsam-2026"
PREFIX = "test-runs/glue-incremental-v1"
SOURCE = "incoming/retailmax/sales/batch-002.csv"
BATCH = "batch-" + "a" * 24
VERSION = "staging-version-001"
KEY = f"{PREFIX}/staging/retailmax/sales/{BATCH}/batch-002.csv"

HEADER = (
    "transaction_id,customer_id,product_id,store_id,"
    "transaction_date,quantity,unit_price,"
    "discount_amount,total_amount\n"
)

ROW = "T001,C001,P001,S001,2026-10-20,2,12.50,0,25.00\n"


@pytest.fixture(autouse=True)
def environment(monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_BUCKET", BUCKET)
    monkeypatch.setenv("LAKEHOUSE_REGION", "us-east-2")
    monkeypatch.setenv("LAKEHOUSE_PREFIX", PREFIX)


def make_client(data, version=VERSION):
    client = MagicMock()
    body = MagicMock()
    body.read.return_value = data

    client.get_object.return_value = {
        "VersionId": version,
        "Body": body,
    }

    return client, body


def execute(spark, client, checksum):
    return read_verified_staging_batch(
        spark,
        client,
        SOURCE,
        BATCH,
        KEY,
        VERSION,
        checksum,
    )


def test_valid_csv_parsed_into_spark_dataframe():
    data = (HEADER + ROW).encode()
    checksum = hashlib.sha256(data).hexdigest()

    client, body = make_client(data)
    spark = MagicMock()

    dataframe, plan = execute(spark, client, checksum)

    assert plan.company == "retailmax"
    assert plan.dataset == "sales"

    spark.createDataFrame.assert_called_once()
    assert dataframe is spark.createDataFrame.return_value

    client.get_object.assert_called_once_with(
        Bucket=BUCKET,
        Key=KEY,
        VersionId=VERSION,
    )
    body.close.assert_called_once()


def test_wrong_s3_version_rejected():
    data = (HEADER + ROW).encode()
    client, body = make_client(data, "wrong-version")

    with pytest.raises(RuntimeError, match="version mismatch"):
        execute(
            MagicMock(),
            client,
            hashlib.sha256(data).hexdigest(),
        )

    body.close.assert_called_once()


def test_checksum_mismatch_rejected():
    data = (HEADER + ROW).encode()
    client, body = make_client(data)

    with pytest.raises(RuntimeError, match="checksum mismatch"):
        execute(MagicMock(), client, "0" * 64)

    body.close.assert_called_once()


def test_extra_csv_column_rejected():
    data = (HEADER + ROW.rstrip("\n") + ",EXTRA\n").encode()
    client, _ = make_client(data)

    with pytest.raises(ValueError, match="extra columns"):
        execute(
            MagicMock(),
            client,
            hashlib.sha256(data).hexdigest(),
        )


def test_missing_csv_column_rejected():
    data = (
        HEADER + "T001,C001,P001,S001,2026-10-20,2,12.50,0\n"
    ).encode()

    client, _ = make_client(data)

    with pytest.raises(ValueError, match="missing columns"):
        execute(
            MagicMock(),
            client,
            hashlib.sha256(data).hexdigest(),
        )


def test_wrong_header_rejected():
    data = b"wrong,header\nvalue1,value2\n"
    client, _ = make_client(data)

    with pytest.raises(ValueError, match="header"):
        execute(
            MagicMock(),
            client,
            hashlib.sha256(data).hexdigest(),
        )


def test_empty_object_rejected():
    client, _ = make_client(b"")

    with pytest.raises(ValueError, match="empty"):
        execute(
            MagicMock(),
            client,
            hashlib.sha256(b"").hexdigest(),
        )


def test_unapproved_staging_key_rejected():
    client, _ = make_client((HEADER + ROW).encode())

    with pytest.raises(ValueError, match="Staging key"):
        read_verified_staging_batch(
            MagicMock(),
            client,
            SOURCE,
            BATCH,
            "test-runs/run-001/protected.csv",
            VERSION,
            "a" * 64,
        )

    client.get_object.assert_not_called()
