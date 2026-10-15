from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName("RetailLakehouseDeltaTest")
    .master("local[*]")
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

try:
    path = "data/delta_smoke_test"

    df = spark.createDataFrame(
        [(1, "RetailMax"), (2, "FreshMart")],
        ["id", "company"]
    )

    df.write.format("delta").mode("overwrite").save(path)

    result = spark.read.format("delta").load(path)
    result.show()

    assert result.count() == 2
    print("SUCCESS: Delta write/read test passed!")

finally:
    spark.stop()
