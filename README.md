# Enterprise Retail Lakehouse Platform

A data engineering portfolio project focused on post-merger data
consolidation for two fictional retailers: RetailMax and FreshMart.

## Technology Stack

- Python and Faker
- PySpark
- Delta Lake
- SQL
- AWS S3 and related AWS services
- Power BI
- Git and GitHub

## Architecture

The platform will implement a medallion architecture:

- Bronze: raw source data
- Silver: cleaned and standardized data
- Gold: business-ready analytical datasets

## Implementation Status

The platform includes a working Bronze, Silver, and Gold lakehouse pipeline using PySpark, Delta Lake, and Amazon S3.

### Bronze Layer

- Ingests separate RetailMax and FreshMart source datasets.
- Stores raw data in 12 Delta tables.
- Preserves 104,760 source records.

### Silver Layer

- Standardizes customers, products, suppliers, stores, inventory, and sales.
- Applies deduplication, validation, and referential integrity checks.
- Produces 6 cleaned Delta datasets and 1 sales quarantine dataset.
- Validated output includes 88,600 trusted sales and 1,400 quarantined sales.

### Gold Layer

- Builds 5 dimensions, 2 fact tables, and 4 business aggregations.
- Supports sales, store performance, product category, and inventory analysis.
- Produces 11 analytics-ready Delta tables.

### Orchestration and Data Quality

- Python-based orchestration supports individual processing stages.
- Delta transaction logs and committed Parquet files have been independently verified.
- S3 write safeguards prevent accidental overwriting of existing outputs.
- All 30 Bronze, Silver, Gold, and quarantine outputs have been verified.

## Analytics

Gold datasets are designed for downstream reporting and Power BI integration. The repository includes a CSV export component.

## Planned Enhancements

- Interactive BI dashboards
- Natural-language analytics
- Additional automated testing and monitoring
- CI/CD automation

CI trigger verification: Jenkins Poll SCM.
