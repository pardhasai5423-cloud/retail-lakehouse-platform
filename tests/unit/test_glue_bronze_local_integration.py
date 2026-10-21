"""Real local Spark/Delta integration test for Bronze ingestion."""

import hashlib
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.glue.jobs.bronze_ingestion import prepare_bronze_batch
from src.glue.object_identity import build_object_batch_id
from src.glue.versioned_spark_input import read_verified_staging_batch


BUCKET = "retail-lakehouse-parsam-2026"
PREFIX = "test-runs/glue-incremental-v1"
SOURCE = "incoming/retailmax/sales/batch-002.csv"
VERSION = "staging-version-001"

BATCH = build_object_batch_id(
    SOURCE,
    version_id="source-version-001",
)

KEY = f"{PREFIX}/staging/retailmax/sales/{BATCH}/batch-002.csv"

HEADER = (
    "transaction_id,customer_id,product_id,store_id,"
    "transaction_date,quantity,unit_price,"
    "discount_amount,total_amount\n"
)

ROWS = (
    "T001,C001,P001,S001,2026-10-20,2,12.50,0,25.00\n"
    "T002,C002,P002,S002,2026-10-20,1,30.00,5.00,25.00\n"
)


@pytest.fixture(autouse=True)
def environment(monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_BUCKET", BUCKET)
    monkeypatch.setenv("LAKEHOUSE_REGION", "us-east-2")
    monkeypatch.setenv("LAKEHOUSE_PREFIX", PREFIX)


@pytest.fixture(scope="module")
def spark():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("BronzeLocalIntegration")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
    )

    session = configure_spark_with_delta_pip(
        builder
    ).getOrCreate()

    session.sparkContext.setLogLevel("ERROR")

    yield session

    session.stop()


@pytest.mark.spark_delta_integration
def test_versioned_csv_to_local_delta(spark, tmp_path):
    data = (HEADER + ROWS).encode("utf-8")
    checksum = hashlib.sha256(data).hexdigest()

    client = MagicMock()
    body = MagicMock()
    body.read.return_value = data

    client.get_object.return_value = {
        "VersionId": VERSION,
        "Body": body,
    }

    df, plan = read_verified_staging_batch(
        spark,
        client,
        SOURCE,
        BATCH,
        KEY,
        VERSION,
        checksum,
    )

    assert df.count() == 2
    assert plan.company == "retailmax"
    assert plan.dataset == "sales"

    bronze_df = prepare_bronze_batch(
        df,
        plan,
        BATCH, SOURCE)

    target = str(tmp_path / "bronze_delta")

    bronze_df.write.format("delta").mode("errorifexists").save(
        target
    )

    result = spark.read.format("delta").load(target)

    assert result.count() == 2
    assert result.select("_batch_id").distinct().count() == 1

    assert (
        result.select("_batch_id").first()[0]
        == BATCH
    )

    assert (
        result.select("_source_system").first()[0]
        == "retailmax"
    )

    assert (
        result.select("_source_dataset").first()[0]
        == "sales"
    )

    assert "_ingestion_timestamp" in result.columns
    assert "_source_file" in result.columns

    assert set(
        row.transaction_id
        for row in result.select("transaction_id").collect()
    ) == {"T001", "T002"}

    client.get_object.assert_called_once_with(
        Bucket=BUCKET,
        Key=KEY,
        VersionId=VERSION,
    )

    body.close.assert_called_once()
