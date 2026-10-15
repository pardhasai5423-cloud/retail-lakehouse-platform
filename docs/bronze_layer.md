
# Bronze Layer — Enterprise Retail Lakehouse Platform

## 1. Overview

The Bronze layer is the first ingestion layer of the Enterprise
Retail Lakehouse Platform.

The project simulates a post-merger data consolidation scenario
involving two fictional retail companies:

- RetailMax
- FreshMart

Both companies maintain independent datasets with different
schemas, naming conventions, and data-quality issues.

The Bronze layer ingests these datasets from Amazon S3 and
stores them as Delta Lake tables without applying business
transformations.

The objective is to preserve source data while introducing
the metadata required for traceability, auditing, and
downstream processing.

## 2. Technology Stack

| Component | Technology |
|-----------|------------|
| Cloud provider | Amazon Web Services |
| Object storage | Amazon S3 |
| Processing engine | Apache Spark 3.5.6 |
| Programming language | Python / PySpark |
| Storage format | Delta Lake 3.3.2 |
| Hadoop version | 3.3.4 |
| S3 connector | Hadoop S3A |
| AWS CLI | AWS CLI v2 |
| Development environment | Ubuntu 22.04 on WSL2 |
| Version control | Git and GitHub |

## 3. Architecture

The Bronze ingestion architecture is:

```text
RetailMax Source Data       FreshMart Source Data
         |                           |
         +-------------+-------------+
                       |
                       v
                Amazon S3 Raw
                       |
                       v
               PySpark Ingestion
                       |
            +----------+----------+
            |                     |
            v                     v
     Source Metadata        Batch Metadata
            |                     |
            +----------+----------+
                       |
                       v
              Amazon S3 Bronze
                       |
                       v
              Delta Lake Tables
                       |
                       v
               Silver Layer
```

The ingestion jobs currently run on a local Ubuntu WSL
environment and access S3 through Hadoop S3A.

## 4. Amazon S3 Configuration

### AWS Region

us-east-2 (US East — Ohio)

### S3 Bucket

retail-lakehouse-parsam-2026

### S3 Directory Structure

```text
s3://retail-lakehouse-parsam-2026/
|
+-- raw/
|   +-- retailmax/
|   |   +-- customers.csv
|   |   +-- products.csv
|   |   +-- stores.csv
|   |   +-- sales.csv
|   |   +-- inventory.csv
|   |   +-- suppliers.csv
|   |
|   +-- freshmart/
|       +-- customers.csv
|       +-- products.csv
|       +-- stores.csv
|       +-- sales.csv
|       +-- inventory.csv
|       +-- suppliers.csv
|
+-- bronze/
|   +-- retailmax/
|   |   +-- customers/
|   |   +-- products/
|   |   +-- stores/
|   |   +-- sales/
|   |   +-- inventory/
|   |   +-- suppliers/
|   |
|   +-- freshmart/
|       +-- customers/
|       +-- products/
|       +-- stores/
|       +-- sales/
|       +-- inventory/
|       +-- suppliers/
|
+-- silver/
+-- gold/
+-- logs/
+-- archive/
```

The raw zone stores the source CSV files.

The Bronze zone stores Delta Lake tables containing Parquet
data files and Delta transaction logs.

The Silver and Gold zones will be populated in later stages.

## 5. Source Datasets

Each retailer provides six datasets:

1. Customers
2. Products
3. Stores
4. Sales
5. Inventory
6. Suppliers

### Record Counts

| Dataset | RetailMax | FreshMart |
|---------|----------:|----------:|
| Customers | 5,050 | 4,040 |
| Products | 1,000 | 800 |
| Stores | 50 | 40 |
| Sales | 50,000 | 40,000 |
| Inventory | 2,000 | 1,600 |
| Suppliers | 100 | 80 |
| **Total** | **58,200** | **46,560** |

Total records: 104,760

Total source datasets: 12

The source datasets were generated using Python, Faker,
and pandas.

Controlled data-quality issues were introduced to simulate
real-world enterprise data ingestion challenges.

## 6. Data Quality Issues

The incoming source datasets intentionally contain:

- Duplicate customer records
- Missing customer email addresses
- Missing product prices
- Invalid product references in sales
- Negative sales quantities
- Inconsistent date formats
- Overlapping customer identities across retailers

### Preserved Data Quality Issues

| Issue | RetailMax | FreshMart |
|-------|----------:|----------:|
| Duplicate customer records | 50 | 40 |
| Negative sales quantities | 250 | 200 |
| Invalid sales product references | 250 | 200 |

The Bronze layer preserves these issues.

Data cleansing, standardization, deduplication, and
invalid-record handling will occur in the Silver layer.

## 7. Bronze Ingestion Pipeline

### Implementation Files

Local ingestion:

`src/bronze/ingest_bronze.py`

AWS S3 ingestion:

`src/bronze/ingest_bronze_s3.py`

### Ingestion Workflow

1. Initialize a SparkSession with Delta Lake support.
2. Configure Hadoop S3A for Amazon S3 connectivity.
3. Authenticate using the AWS CLI named profile.
4. Generate a unique ingestion batch ID.
5. Read source CSV files from the S3 raw zone.
6. Preserve source columns and their original names.
7. Read source data columns as strings.
8. Add technical ingestion metadata.
9. Write each dataset as a Delta Lake table in S3 Bronze.
10. Read each Delta table back and verify its row count.

### Source Path Example

```text
s3a://retail-lakehouse-parsam-2026/raw/retailmax/customers.csv
```

### Target Path Example

```text
s3a://retail-lakehouse-parsam-2026/bronze/retailmax/customers
```

### Ingestion Mode

The current implementation uses:

```python
df.write.format("delta").mode("overwrite").save(target_path)
```

This is a full-table overwrite strategy designed for the
current development environment.

Incremental ingestion will be introduced in a later phase.

## 8. Ingestion Metadata

The Bronze ingestion pipeline adds five technical metadata
columns to every dataset.

| Column | Description |
|--------|-------------|
| `_source_system` | Source company name |
| `_source_dataset` | Source dataset name |
| `_source_file` | Original S3 source file path |
| `_batch_id` | Unique ingestion execution identifier |
| `_ingestion_timestamp` | Timestamp of ingestion |

### Batch ID

Each pipeline execution generates a unique batch ID.

Example:

```text
20261015T183000Z-a1b2c3d4
```

All 12 tables processed during a successful full pipeline
execution share the same batch ID.

This enables batch-level traceability across datasets.

### Source Lineage

The `_source_file` column identifies the source file
from which a record was ingested.

This helps support:

- Source-to-target traceability
- Data auditing
- Pipeline troubleshooting
- Batch-level investigation

## 9. Delta Lake Implementation

Bronze datasets are stored using Delta Lake.

Delta Lake combines Parquet data files with a transaction
log that records changes to the table.

### Delta Table Structure

```text
bronze/retailmax/customers/
|
+-- _delta_log/
|   +-- 00000000000000000000.json
|   +-- 00000000000000000001.json
|
+-- part-00000-....snappy.parquet
+-- part-00001-....snappy.parquet
```

This structure is illustrative. The actual number of
Parquet files and transaction-log versions can vary.

### Delta Transaction Log

The `_delta_log` directory records committed transactions,
table metadata, and file additions or removals.

Delta Lake uses this transaction log to determine which
data files belong to a particular table version.

### ACID Properties

**Atomicity**

Transactions are committed as a single logical operation.
Readers do not see partially committed transactions.

**Consistency**

Delta Lake enforces table metadata and schema constraints.

Application-specific business validation is handled
separately by data-quality rules.

**Isolation**

Delta Lake uses consistent snapshots and transaction
conflict detection.

**Durability**

Committed transaction logs and data files are persisted
on the underlying storage system.

### Time Travel

Delta Lake supports querying historical table versions
while the required historical files remain available.

Example:

```python
df = (
    spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .load(table_path)
)
```

## 10. S3A and Delta Lake Configuration

The Spark environment uses Hadoop S3A to communicate
with Amazon S3.

### Hadoop S3A Dependency

```text
org.apache.hadoop:hadoop-aws:3.3.4
```

This matches the Hadoop 3.3.4 version used by the
installed Spark environment.

### AWS Credentials

AWS CLI profile:

```text
retail-lakehouse
```

IAM development user:

```text
retail-lakehouse-dev
```

Credentials are stored outside the Git repository.

The development IAM policy grants access only to the
project S3 bucket.

### Delta Log Store

The current implementation uses:

```text
org.apache.spark.sql.delta.storage.S3SingleDriverLogStore
```

This configuration supports the project's current
single-driver development setup.

It must not be treated as a general-purpose solution for
concurrent writes from independent Spark drivers.

A production deployment with multiple writers will
require an appropriate concurrency-safe configuration.

## 11. Automated Validation

### Local Validation

Script:

`tests/validate_bronze.py`

### AWS S3 Validation

Script:

`tests/validate_bronze_s3.py`

### Validation Checks

The S3 validation script verifies:

1. All 12 Delta tables are readable.
2. Each table contains the expected number of records.
3. Required ingestion metadata columns exist.
4. Metadata columns do not contain null values.
5. Source-system values match their expected retailer.
6. Source-dataset values match their expected dataset.
7. Each table contains one ingestion batch ID.
8. All 12 tables share the same batch ID.
9. Duplicate customer records are preserved.
10. Negative sales quantities are preserved.
11. Invalid product references are preserved.
12. The combined record count equals 104,760.

### Validation Results

All validation checks passed.

```text
PASS: retailmax.customers (5,050 rows)
PASS: retailmax.products (1,000 rows)
PASS: retailmax.stores (50 rows)
PASS: retailmax.sales (50,000 rows)
PASS: retailmax.inventory (2,000 rows)
PASS: retailmax.suppliers (100 rows)

PASS: freshmart.customers (4,040 rows)
PASS: freshmart.products (800 rows)
PASS: freshmart.stores (40 rows)
PASS: freshmart.sales (40,000 rows)
PASS: freshmart.inventory (1,600 rows)
PASS: freshmart.suppliers (80 rows)

PASS: Total Bronze records = 104,760
PASS: All tables share one ingestion batch ID

PASS: retailmax duplicates preserved: 50
PASS: freshmart duplicates preserved: 40

PASS: retailmax negatives=250, invalid products=250
PASS: freshmart negatives=200, invalid products=200

SUCCESS: ALL S3 BRONZE VALIDATIONS PASSED!
```

## 12. Running the Pipeline

### Activate the Python Environment

```bash
cd ~/retail-lakehouse-platform
source .venv/bin/activate
```

### Verify AWS Authentication

```bash
aws sts get-caller-identity --profile retail-lakehouse
```

### Test S3 Connectivity

```bash
python tests/test_s3_connection.py
```

### Run Local Bronze Ingestion

```bash
python src/bronze/ingest_bronze.py
```

### Run AWS S3 Bronze Ingestion

```bash
python src/bronze/ingest_bronze_s3.py
```

### Validate Local Bronze Tables

```bash
python tests/validate_bronze.py
```

### Validate AWS S3 Bronze Tables

```bash
python tests/validate_bronze_s3.py
```

### Inspect S3 Bronze Objects

```bash
aws s3 ls \
  s3://retail-lakehouse-parsam-2026/bronze/ \
  --recursive \
  --profile retail-lakehouse
```

## 13. Current Limitations

The current Bronze implementation is a development-stage
data engineering pipeline.

Known limitations include:

- Full-table overwrite instead of incremental ingestion
- Spark execution on local WSL rather than AWS Glue or EMR
- No automated orchestration or scheduling
- No centralized pipeline monitoring
- No automatic retry or recovery framework
- No production-grade concurrent-writer configuration
- No automated schema evolution strategy
- No source-file archival workflow
- No infrastructure-as-code deployment

These are planned improvements, not completed features.

## 14. Future Enhancements

Planned enhancements include:

- Incremental and idempotent ingestion
- Source-file archival
- Automated orchestration using AWS Step Functions
- AWS Glue or Amazon EMR execution
- CloudWatch monitoring and alerting
- Infrastructure as code
- CI/CD integration
- Improved schema management
- Batch audit and reconciliation tables

## 15. Next Phase — Silver Layer

The Silver layer will transform the Bronze datasets into
clean, standardized, and reliable enterprise data.

Planned transformations include:

- Standardizing RetailMax and FreshMart schemas
- Converting columns into appropriate data types
- Normalizing inconsistent date formats
- Deduplicating customer records
- Handling missing values
- Validating sales quantities
- Validating product references
- Quarantining invalid records
- Resolving overlapping customer identities
- Writing unified Silver Delta tables to Amazon S3

The Silver layer will retain appropriate source lineage
to support downstream auditing and analytics.

## 16. Conclusion

The Bronze layer successfully implements an AWS S3-based
data ingestion pipeline using PySpark and Delta Lake.

The pipeline ingests 12 datasets from two retail companies
and preserves 104,760 source records with technical
metadata for traceability.

Automated validation confirms the expected record counts,
consistent batch identification, and preservation of
intentionally introduced source-data issues.

This establishes the foundation for the Silver and Gold
layers of the Enterprise Retail Lakehouse Platform.

