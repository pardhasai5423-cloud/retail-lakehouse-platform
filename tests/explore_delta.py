
from pathlib import Path

from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
TABLE_PATH = ROOT / "data/bronze/retailmax/customers"

builder = (
    SparkSession.builder
    .appName("ExploreDeltaLake")
    .master("local[2]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

try:
    delta_table = DeltaTable.forPath(spark, str(TABLE_PATH))

    print("\n=== DELTA TABLE HISTORY ===")
    delta_table.history().select(
        "version", "timestamp", "operation", "operationParameters"
    ).show(truncate=False)

    print("\n=== CURRENT DATA ===")
    df = spark.read.format("delta").load(str(TABLE_PATH))
    print("Current row count:", df.count())

    print("\n=== TABLE DETAILS ===")
    delta_table.detail().select(
        "format", "numFiles", "sizeInBytes", "location"
    ).show(truncate=False)

finally:
    spark.stop()
