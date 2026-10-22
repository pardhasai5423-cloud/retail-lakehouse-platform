"""Local Bronze-to-Silver integration using real Spark and Delta."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from src.glue.delta_processing_ledger import read_batch
from src.glue.jobs.end_to_end_coordinator import execute_end_to_end_batch

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, functions as F

from src.glue.jobs.silver_execution import execute_silver_batch
from src.glue.jobs.gold_orchestrator import execute_gold_pipeline


FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "local_e2e"

DATASETS = (
    "customers",
    "products",
    "stores",
    "inventory",
    "sales",
)



def execute_local_gold_pipeline(
    spark, changed_datasets, silver_base, gold_base, *, allow_writes=False
):
    """Execute real Gold transformations only in isolated pytest directories."""
    from src.glue.gold_planner import plan_gold_refresh
    from src.glue.gold_refresh import (
        execute_date_refresh,
        validate_gold_target,
    )
    from src.glue.jobs import gold_orchestrator
    from src.glue.jobs.gold_execution import execute_gold_table
    from src.glue.jobs.gold_orchestrator import (
        DATE_TABLE,
        AGGREGATE_TABLES,
        DIRECT_TABLES,
    )

    silver = Path(silver_base)
    gold = Path(gold_base)

    if (
        not silver.is_absolute()
        or not gold.is_absolute()
        or silver.is_symlink()
        or gold.is_symlink()
        or silver.name != "silver"
        or gold.name != "gold"
        or silver.parent.resolve() != gold.parent.resolve()
        or not silver.parent.resolve().is_relative_to(
            Path("/tmp").resolve()
        )
    ):
        raise ValueError("Local Gold execution requires isolated /tmp paths")

    tables = tuple(plan_gold_refresh(changed_datasets))

    for table in tables:
        validate_gold_target(str(gold / table))

    results = []

    for table in tables:
        if table == DATE_TABLE:
            result = execute_date_refresh(
                spark, str(silver), str(gold),
                allow_writes=allow_writes,
            )
        elif table in AGGREGATE_TABLES:
            result = gold_orchestrator.execute_aggregate_refresh(
                spark, table, str(gold),
                allow_writes=allow_writes,
            )
        elif table in DIRECT_TABLES:
            result = execute_gold_table(
                spark, table, str(silver), str(gold),
                allow_writes=allow_writes,
            )
        else:
            raise ValueError(f"Unsupported Gold table: {table}")

        results.append(result)

    return {
        "status": "completed" if allow_writes else "dry_run",
        "tables": tables,
        "results": results,
    }


@pytest.fixture(scope="module")
def spark():
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("RetailLakehouseLocalPipeline")
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

    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")

    yield session
    session.stop()


@pytest.mark.spark_delta_integration
def test_real_bronze_to_silver_pipeline(spark, tmp_path, monkeypatch):
    # Required by the existing transformation modules at import time.
    monkeypatch.setenv(
        "LAKEHOUSE_BUCKET", "retail-lakehouse-parsam-2026"
    )
    monkeypatch.setenv("LAKEHOUSE_REGION", "us-east-2")
    monkeypatch.setenv(
        "LAKEHOUSE_PREFIX", "test-runs/glue-incremental-v1"
    )

    bronze_base = tmp_path / "bronze"
    silver_base = tmp_path / "silver"

    expected = {
        "customers": 2,
        "products": 2,
        "stores": 2,
        "inventory": 2,
        "sales": 2,
    }

    for dataset in DATASETS:
        batch_id = f"local-e2e-{dataset}"
        source_file = FIXTURES / f"{dataset}.csv"

        assert source_file.is_file(), source_file

        raw = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(str(source_file))
        )

        bronze = (
            raw
            .withColumn("_batch_id", F.lit(batch_id))
            .withColumn("_source_system", F.lit("retailmax"))
            .withColumn("_source_dataset", F.lit(dataset))
            .withColumn(
                "_source_file",
                F.lit(f"incoming/retailmax/{dataset}/local.csv"),
            )
            .withColumn(
                "_ingestion_timestamp",
                F.current_timestamp(),
            )
        )

        bronze_path = str(bronze_base / dataset)

        bronze.write.format("delta").mode("errorifexists").save(
            bronze_path
        )

        assert (
            spark.read.format("delta").load(bronze_path).count()
            == expected[dataset]
        )

        result = execute_silver_batch(
            spark,
            "retailmax",
            dataset,
            batch_id,
            bronze_path,
            str(silver_base),
            allow_writes=True,
        )

        assert result["status"] == "completed"

        silver_path = str(silver_base / dataset)
        actual = spark.read.format("delta").load(silver_path).count()

        assert actual == expected[dataset], (
            f"{dataset}: expected {expected[dataset]}, got {actual}"
        )

        print(
            f"PASS: {dataset}: "
            f"{expected[dataset]} Bronze -> {actual} Silver"
        )

    sales = spark.read.format("delta").load(
        str(silver_base / "sales")
    )

    assert sales.select("transaction_key").distinct().count() == 2

    revenue = sales.agg(
        F.sum("total_amount").alias("revenue")
    ).first()["revenue"]

    assert float(revenue) == 50.0

    print("PASS: Sales references valid; revenue = $50.00")

    # Execute the real Gold refresh using local Silver Delta tables.
    gold_base = tmp_path / "gold"

    gold_result = execute_local_gold_pipeline(
        spark,
        DATASETS,
        str(silver_base),
        str(gold_base),
        allow_writes=True,
    )

    assert gold_result["status"] == "completed"

    expected_gold = (
        "dim_customers",
        "dim_products",
        "dim_stores",
        "dim_date",
        "fact_sales",
        "fact_inventory",
        "agg_daily_sales",
        "agg_store_performance",
        "agg_category_sales",
        "agg_inventory_status",
    )

    for table in expected_gold:
        path = str(gold_base / table)
        df = spark.read.format("delta").load(path)
        print(f"PASS: Gold {table}: {df.count()} records")

    fact_sales = spark.read.format("delta").load(
        str(gold_base / "fact_sales")
    )

    assert fact_sales.count() == 2

    daily = spark.read.format("delta").load(
        str(gold_base / "agg_daily_sales")
    )

    daily_revenue = daily.agg(
        F.sum("net_revenue").alias("revenue")
    ).first()["revenue"]

    assert float(daily_revenue) == 50.0

    print("PASS: Gold fact_sales = 2 records")
    print("PASS: Gold daily sales revenue = $50.00")

    # Replay the same sales batch through the real Silver execution.
    replay_result = execute_silver_batch(
        spark,
        "retailmax",
        "sales",
        "local-e2e-sales",
        str(bronze_base / "sales"),
        str(silver_base),
        allow_writes=True,
    )

    assert replay_result["status"] == "completed"

    silver_sales_after = spark.read.format("delta").load(
        str(silver_base / "sales")
    )

    assert silver_sales_after.count() == 2

    # Refresh Gold using the same unchanged Silver sales data.
    replay_gold = execute_local_gold_pipeline(
        spark,
        ["sales"],
        str(silver_base),
        str(gold_base),
        allow_writes=True,
    )

    assert replay_gold["status"] == "completed"

    gold_sales_after = spark.read.format("delta").load(
        str(gold_base / "fact_sales")
    )

    assert gold_sales_after.count() == 2

    daily_after = spark.read.format("delta").load(
        str(gold_base / "agg_daily_sales")
    )

    revenue_after = daily_after.agg(
        F.sum("net_revenue").alias("revenue")
    ).first()["revenue"]

    assert float(revenue_after) == 50.0

    print("PASS: Repeat processing is idempotent")
    print("PASS: Silver sales remains at 2 records")
    print("PASS: Gold fact_sales remains at 2 records")
    print("PASS: Gold revenue remains $50.00")


@pytest.mark.spark_delta_integration
def test_real_coordinator_with_local_delta(spark, tmp_path, monkeypatch):
    """Coordinator + real Silver/Gold + ledger, without AWS calls."""

    monkeypatch.setenv(
        "LAKEHOUSE_BUCKET", "retail-lakehouse-parsam-2026"
    )
    monkeypatch.setenv("LAKEHOUSE_REGION", "us-east-2")
    monkeypatch.setenv(
        "LAKEHOUSE_PREFIX", "test-runs/glue-incremental-v1"
    )

    bronze_path = str(tmp_path / "bronze" / "stores")
    silver_base = str(tmp_path / "silver")
    gold_base = str(tmp_path / "gold")
    ledger_path = str(tmp_path / "ledger")
    batch_id = "local-coordinator-stores-001"

    # Prepare local Bronze stores data.
    raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(str(FIXTURES / "stores.csv"))
    )

    bronze = (
        raw
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_source_system", F.lit("retailmax"))
        .withColumn("_source_dataset", F.lit("stores"))
        .withColumn(
            "_source_file",
            F.lit("incoming/retailmax/stores/local.csv"),
        )
        .withColumn(
            "_ingestion_timestamp",
            F.current_timestamp(),
        )
    )

    bronze.write.format("delta").save(bronze_path)

    # Seed Silver and Gold prerequisites using existing real logic.
    # The stores Gold impact includes agg_store_performance,
    # which requires a preexisting fact_sales table.
    sales_gold_path = str(Path(gold_base) / "fact_sales")

    from datetime import date
    from decimal import Decimal
    from pyspark.sql import types as T

    sales_schema = T.StructType([
        T.StructField("date_key", T.IntegerType()),
        T.StructField("transaction_date", T.DateType()),
        T.StructField("source_system", T.StringType()),
        T.StructField("store_key", T.StringType()),
        T.StructField("product_key", T.StringType()),
        T.StructField("customer_key", T.StringType()),
        T.StructField("quantity", T.IntegerType()),
        T.StructField("unit_price", T.DecimalType(18, 2)),
        T.StructField("discount_amount", T.DecimalType(18, 2)),
        T.StructField("total_amount", T.DecimalType(18, 2)),
    ])

    sales = spark.createDataFrame(
        [
            (
                20261020,
                date(2026, 10, 20),
                "retailmax",
                "retailmax:S001",
                "retailmax:P001",
                "retailmax:C001",
                2,
                Decimal("12.50"),
                Decimal("0.00"),
                Decimal("25.00"),
            ),
        ],
        sales_schema,
    )

    sales.write.format("delta").save(sales_gold_path)

    args = {
        "batch_id": batch_id,
        "company": "retailmax",
        "dataset": "stores",
        "source_key": "incoming/retailmax/stores/local.csv",
        "version_id": "local-version-stores-001",
        "ledger_path": ledger_path,
        "bronze_path": bronze_path,
        "silver_base": silver_base,
        "gold_base": gold_base,
        "allow_writes": True,
    }

    bronze_result = {
        "execution_ready": True,
        "batch_id": batch_id,
        "bronze_target": bronze_path,
    }

    with (
        patch(
            "src.glue.jobs.end_to_end_coordinator.plan_bronze_execution",
            return_value={"batch_id": batch_id},
        ),
        patch(
            "src.glue.jobs.end_to_end_coordinator.get_bronze_target",
            return_value=bronze_path,
        ),
        patch(
            "src.glue.jobs.end_to_end_coordinator.execute_bronze_batch",
            return_value=bronze_result,
        ) as bronze_mock,
    ):
        from src.glue.jobs.gold_orchestrator import (
            execute_gold_pipeline as real_gold_pipeline,
        )
        from src.glue.gold_refresh import (
            execute_aggregate_refresh as real_aggregate_refresh,
        )

        gold_attempts = {"count": 0}
        aggregate_attempts = {"count": 0}

        def aggregate_with_first_attempt_failure(*a, **kw):
            aggregate_attempts["count"] += 1

            if aggregate_attempts["count"] == 1:
                raise RuntimeError(
                    "simulated failure after dim_stores write"
                )

            return real_aggregate_refresh(*a, **kw)

        def gold_with_first_attempt_failure(*a, **kw):
            gold_attempts["count"] += 1
            return execute_local_gold_pipeline(*a, **kw)

        with (
            patch(
                "src.glue.jobs.end_to_end_coordinator.execute_gold_pipeline",
                side_effect=gold_with_first_attempt_failure,
            ),
            patch(
                "src.glue.jobs.gold_orchestrator.execute_aggregate_refresh",
                side_effect=aggregate_with_first_attempt_failure,
            ),
        ):
            with pytest.raises(
                RuntimeError,
                match="simulated failure after dim_stores write",
            ):
                execute_end_to_end_batch(
                    spark, MagicMock(), **args
                )

            failed = read_batch(spark, ledger_path, batch_id)
            assert failed["status"] == "FAILED"
            assert failed["attempt"] == 1

            # The dimension must have been committed before failure.
            partial_dimension = spark.read.format("delta").load(
                str(Path(gold_base) / "dim_stores")
            )

            assert partial_dimension.count() == 2

            # The aggregate must not have been written yet.
            from delta.tables import DeltaTable

            aggregate_path = str(
                Path(gold_base) / "agg_store_performance"
            )

            assert not DeltaTable.isDeltaTable(
                spark, aggregate_path
            )

            # Silver already succeeded before the simulated Gold failure.
            silver_before_retry = spark.read.format("delta").load(
                str(Path(silver_base) / "stores")
            )

            assert silver_before_retry.count() == 2

            # Retry the same batch using real Silver and real Gold.
            first = execute_end_to_end_batch(
                spark, MagicMock(), **args
            )

        assert first["status"] == "completed"

        ledger = read_batch(spark, ledger_path, batch_id)
        assert ledger["status"] == "COMPLETED"
        assert ledger["attempt"] == 2
        assert gold_attempts["count"] == 2
        assert aggregate_attempts["count"] == 2

        silver_stores = spark.read.format("delta").load(
            str(Path(silver_base) / "stores")
        )

        gold_stores = spark.read.format("delta").load(
            str(Path(gold_base) / "dim_stores")
        )

        assert silver_stores.count() == 2
        assert gold_stores.count() == 2

        store_performance = spark.read.format("delta").load(
            str(Path(gold_base) / "agg_store_performance")
        )

        assert store_performance.count() == 2

        second = execute_end_to_end_batch(
            spark, MagicMock(), **args
        )

        assert second["status"] == "skipped"
        assert bronze_mock.call_count == 2

    print("PASS: Coordinator executed real Silver and Gold")
    print("PASS: Silver stores = 2")
    print("PASS: Gold dim_stores = 2")
    print("PASS: Ledger recovered FAILED -> COMPLETED, attempt = 2")
    print("PASS: Completed batch skipped on replay")
    print("PASS: Silver stores remains at 2 records after retry")
    print("PASS: Partial Gold write recovered successfully")
    print("PASS: Store performance aggregate contains 2 records")
