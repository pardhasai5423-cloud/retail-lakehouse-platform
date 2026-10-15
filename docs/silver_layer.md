
# Silver Layer — Enterprise Retail Lakehouse

## 1. Overview

The Silver layer integrates and standardizes data from
RetailMax and FreshMart following their fictional merger.

It transforms raw Bronze Delta tables into validated,
analytics-ready datasets stored in Amazon S3 using Delta Lake.

The implementation uses:
- Apache Spark / PySpark 3.5.6
- Delta Lake
- Amazon S3
- Python-based data quality validation
- Source-qualified business keys
- Transaction-level data lineage

## 2. Architecture

RetailMax Bronze Delta Tables
                |
                v
      Schema Standardization
                |
                v
        Data Quality Checks
                |
                v
      Cleaning and Deduplication
                |
                v
       Reference Validation
                |
                v
       Silver Delta Tables
                |
                v
        Gold Analytics Layer

FreshMart Bronze tables follow the same processing flow.

Invalid Sales records are separated into a quarantine
Delta table rather than being silently discarded.

## 3. Silver Datasets

| Dataset | Record Count | Status |
|---------|-------------:|--------|
| Customers | 9,000 | Validated |
| Products | 1,800 | Validated |
| Suppliers | 180 | Validated |
| Stores | 90 | Validated |
| Inventory | 3,600 | Validated |
| Sales | 88,600 | Validated |
| Total | 103,270 | Validated |

An additional 1,400 Sales transactions are quarantined.

## 4. Source System Integration

RetailMax and FreshMart use different field names,
date formats, and identifier conventions.

PySpark transformation modules map these schemas into
a unified structure.

Examples:

RetailMax transaction_id -> source_transaction_id
FreshMart receipt_no -> source_transaction_id

RetailMax quantity -> quantity
FreshMart units_sold -> quantity

RetailMax total_amount -> total_amount
FreshMart net_sales -> total_amount

Source-qualified keys prevent collisions between
identifiers from different companies.

Example:

retailmax:RM-C00001
freshmart:FM-C00001

These keys are used to connect Sales and Inventory
to the corresponding Silver master datasets.

## 5. Customer Processing

RetailMax: 5,000 valid unique customers
FreshMart: 4,000 valid unique customers

Total: 9,000

Processing includes:
- Schema standardization
- Email normalization
- Date standardization
- Deterministic deduplication
- Source-qualified customer keys
- Ingestion lineage preservation

Missing emails are preserved rather than inventing
contact information.

Cross-company identity matches are identified as
candidates but are not automatically merged.

## 6. Product Processing

RetailMax: 1,000 products
FreshMart: 800 products

Total: 1,800

Product processing includes:
- Canonical product schema
- Decimal price types
- Source-qualified product keys
- Supplier reference preservation
- Product validation statuses

18 products have missing prices and are retained
with explicit quality flags.

## 7. Supplier Processing

RetailMax: 100 suppliers
FreshMart: 80 suppliers

Total: 180

Processing includes:
- Schema standardization
- Contact information normalization
- Supplier key generation
- Duplicate checks
- Supplier-product reference validation

Suppliers with similar names across companies
are not automatically merged.

## 8. Store Processing

RetailMax: 50 stores
FreshMart: 40 stores

Total: 90

Processing includes:
- Canonical store schema
- State and ZIP code normalization
- Opening date parsing
- Store key generation
- Duplicate checks
- Store reference validation

All store records passed validation.

## 9. Inventory Processing

RetailMax: 2,000 inventory records
FreshMart: 1,600 inventory records

Total: 3,600

Inventory records are validated against the
Silver Products and Stores tables.

Stock classifications:
- OUT_OF_STOCK: 3
- REORDER_REQUIRED: 436
- SUFFICIENT_STOCK: 3,161

Inventory freshness classifications:
- CURRENT: 889
- STALE: 2,711

The freshness classification uses a 365-day
threshold relative to the processing date.

All 3,600 records passed validation.

## 10. Sales Processing

Original Sales records:
- RetailMax: 50,000
- FreshMart: 40,000
- Total: 90,000

Trusted Silver Sales:
- RetailMax: 49,000
- FreshMart: 39,600
- Total: 88,600

Quarantined Sales:
- RetailMax: 1,000
- FreshMart: 400
- Total: 1,400

### Sales quality issues

- Negative quantities: 450
- Invalid product references: 450
- Missing transaction dates: 500
- Total amount mismatches: 450

Some quality flags overlap on the same transaction.

### Financial validation

Expected total:

(quantity * unit_price) - discount_amount

A tolerance of $0.01 is allowed for total
amount comparisons.

### Quarantine strategy

Transactions that violate the defined Sales
business rules are written to a separate Delta
table with:

- Original standardized transaction values
- Source identifiers
- Quality issue array
- Validation status
- Ingestion lineage

Quarantined records are excluded from trusted
Silver Sales analytics but remain available
for investigation and future reprocessing.

## 11. S3 Storage Layout

s3://retail-lakehouse-parsam-2026/
    bronze/
        retailmax/
        freshmart/
    silver/
        customers/
        products/
        suppliers/
        stores/
        inventory/
        sales/
    quarantine/
        sales/
    gold/

Each Silver dataset is stored in Delta Lake format.

## 12. Validation

Independent validation scripts verify:

- Record counts
- Schema and data types
- Required identifiers
- Duplicate business keys
- Data quality classifications
- Financial calculations
- Referential integrity
- Source lineage
- Delta transaction history

All six Silver datasets passed their respective
independent validation checks.

## 13. Current Implementation Limitations

The current pipelines process a development snapshot
and use Delta overwrite operations.

They do not yet implement:
- Incremental CDC processing
- Automated quarantine remediation
- Production job scheduling
- Cross-table atomic publication
- Automated schema evolution policies

These are potential future enhancements.

## 14. Next Phase — Gold Layer

The Gold layer will transform trusted Silver datasets
into business-focused analytical models.

Planned outputs include:
- Consolidated sales KPIs
- Revenue by company
- Revenue by store
- Product and category performance
- Customer purchasing behavior
- Inventory and replenishment analytics

Gold datasets will support Streamlit dashboards
and future natural-language analytics.
