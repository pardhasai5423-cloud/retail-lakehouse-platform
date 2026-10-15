
# Bronze Layer — Retail Lakehouse Platform

## Overview

The Bronze layer ingests raw data from two fictional retail
companies, RetailMax and FreshMart, using PySpark and Delta Lake.

## Source Datasets

Each retailer provides six datasets:

- Customers
- Products
- Stores
- Sales
- Inventory
- Suppliers

Total datasets: 12

## Ingestion Process

1. Read source CSV files using PySpark.
2. Preserve source-specific column names.
3. Read source columns as strings.
4. Add ingestion metadata.
5. Write datasets as Delta Lake tables.
6. Validate row counts and source-data preservation.

## Technical Metadata

Each Bronze record contains:

- `_source_system`
- `_source_dataset`
- `_source_file`
- `_ingestion_timestamp`

## Data Volume

| Company | Records |
|---------|--------:|
| RetailMax | 58,200 |
| FreshMart | 46,560 |
| Total | 104,760 |

## Data Quality

The Bronze layer intentionally retains source-data issues,
including:

- Duplicate customer records
- Missing customer emails
- Invalid product references
- Negative sales quantities
- Inconsistent date formats

These issues will be handled in the Silver layer.

## Delta Lake Implementation

Each Bronze dataset is stored as a Delta table containing
Parquet data files and a `_delta_log` transaction log.

Delta Lake supports ACID transactions, schema enforcement,
versioned snapshots, and Time Travel.

## Current Implementation

- Local Ubuntu WSL environment
- PySpark 3.5.6
- Delta Lake 3.3.2
- Full-table overwrite for development

## Future Improvements

- AWS S3 raw landing zone
- Incremental ingestion
- Idempotent batch processing
- Schema validation and evolution
- Automated orchestration
- Pipeline monitoring and alerting
