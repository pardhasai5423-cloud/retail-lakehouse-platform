"""Shared Spark initialization for the test suite."""

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session", autouse=True)
def initialize_delta_spark():
    """Load Delta Lake JARs before individual Spark fixtures start."""

    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("retail-lakehouse-pytest")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")

    yield

    # Individual test fixtures manage their own Spark sessions.
    # Do not stop the shared JVM here.
