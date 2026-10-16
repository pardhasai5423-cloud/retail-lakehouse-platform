
# Gold Layer — Enterprise Retail Lakehouse Platform

## 1. Overview

The Gold layer provides analytics-ready Delta Lake tables for
the post-merger consolidation of RetailMax and FreshMart.

It transforms validated Silver datasets into dimensional
models, transactional fact tables, and business aggregations.

All Gold tables are stored in Amazon S3 using Delta Lake
and built with Apache Spark (PySpark).

**Storage location:**

`s3a://retail-lakehouse-parsam-2026/gold/`

**Implementation status:** Complete — 11 tables built
and independently validated.

---

## 2. Architecture

```text
                       SILVER LAYER
          (Cleaned, validated, standardized data)
                              |
             +----------------+----------------+
             |                                 |
             v                                 v
      DIMENSION TABLES                     FACT TABLES
      ---------------                     -----------
      dim_customers                       fact_sales
      dim_products                        fact_inventory
      dim_suppliers
      dim_stores
      dim_date
             |                                 |
             +----------------+----------------+
                              |
                              v
                     GOLD AGGREGATIONS
                     -----------------
                     agg_daily_sales
                     agg_store_performance
                     agg_category_sales
                     agg_inventory_status
                              |
                              v
                    ANALYTICS / DASHBOARDS
                         (Planned)
```

The model uses source-qualified business keys to prevent
identifier collisions between the two acquired businesses.

Examples:

- `retailmax:CUST0001`
- `freshmart:CUST0001`

Identical identifiers from different source systems are
treated as distinct entities.

Cross-company identity resolution and category harmonization
are future enhancements.

---

## 3. Dimension Tables

### 3.1 dim_customers

**Grain:** One record per source-qualified customer.

**Records:** 9,000

- RetailMax: 5,000
- FreshMart: 4,000

Key attributes include:

- customer_key
- source_system
- source_customer_id
- customer_name
- email
- phone
- city
- state
- registration_date
- registration_year
- registration_month

Customer records are not automatically merged across
companies, even when matching email addresses exist.

### 3.2 dim_products

**Grain:** One record per source-qualified product.

**Records:** 1,800

- RetailMax: 1,000
- FreshMart: 800

Key attributes include:

- product_key
- product_name
- category
- brand
- unit_price
- supplier_key
- price_quality_status
- validation_status

18 products have missing prices.

These missing values are retained as NULL and flagged
rather than replaced with fabricated prices.

All Product-to-Supplier references have been validated.

### 3.3 dim_suppliers

**Grain:** One record per source-qualified supplier.

**Records:** 180

- RetailMax: 100
- FreshMart: 80

Supplier attributes include name, contact email, phone,
city, and state.

Suppliers with similar names across source systems are
not automatically merged.

### 3.4 dim_stores

**Grain:** One record per source-qualified store.

**Records:** 90

- RetailMax: 50
- FreshMart: 40

Attributes include:

- store_key
- store_name
- city
- state
- zip_code
- opened_date
- opened_year
- opened_month

ZIP codes remain strings to preserve leading zeros.

### 3.5 dim_date

**Grain:** One calendar date.

**Records:** 4,275

**Date range:** 2015-01-16 through 2026-09-29

The date dimension contains:

- date_key (YYYYMMDD)
- calendar_date
- year
- quarter
- month
- month_name
- day_of_month
- day_of_week
- day_name
- week_of_year
- is_weekend
- year_month

It supports date-based reporting and joins from
Sales and Inventory facts.

---

## 4. Fact Tables

### 4.1 fact_sales

**Grain:** One validated source Sales transaction record.

**Records:** 88,600

- RetailMax: 49,000
- FreshMart: 39,600

Core attributes:

- transaction_key
- source_system
- customer_key
- product_key
- store_key
- date_key
- transaction_date
- quantity
- unit_price
- discount_amount
- total_amount

**Business rule:**

`total_amount = quantity * unit_price - discount_amount`

The fact preserves original transaction lineage.

All Sales records have valid references to the
Customer, Product, Store, and Date dimensions.

#### Sales financial reconciliation

| Source | Transactions | Units Sold | Net Revenue |
|---|---:|---:|---:|
| RetailMax | 49,000 | 146,797 | $21,606,073.80 |
| FreshMart | 39,600 | 118,736 | $16,896,648.70 |
| **Total** | **88,600** | **265,533** | **$38,502,722.50** |

The Silver layer separately quarantined 1,400
invalid Sales records.

Quarantined transactions are not included in
Gold Sales reporting.

### 4.2 fact_inventory

**Grain:** One validated source inventory record.

**Records:** 3,600

- RetailMax: 2,000
- FreshMart: 1,600

Core attributes:

- inventory_key
- source_system
- product_key
- store_key
- last_updated_date_key
- stock_quantity
- reorder_level
- stock_status
- freshness_status
- last_updated

Inventory stock classifications:

| Status | Records |
|---|---:|
| OUT_OF_STOCK | 3 |
| REORDER_REQUIRED | 436 |
| SUFFICIENT_STOCK | 3,161 |
| **Total** | **3,600** |

Inventory freshness classifications:

| Status | Records |
|---|---:|
| CURRENT | 889 |
| STALE | 2,711 |
| **Total** | **3,600** |

The Inventory fact represents the available source
inventory records; it is not a historical snapshot fact.

---

## 5. Business Aggregations

### 5.1 agg_daily_sales

**Grain:** One company per transaction date.

**Records:** 2,736

Metrics:

- transaction_count
- units_sold
- gross_sales_amount
- discount_amount
- net_revenue
- average_transaction_value

All aggregated transactions and revenue reconcile
to fact_sales.

### 5.2 agg_store_performance

**Grain:** One source-qualified store.

**Records:** 90

Metrics:

- transaction_count
- units_sold
- gross_sales_amount
- discount_amount
- net_revenue
- distinct_customers
- distinct_products_sold
- average_transaction_value

The aggregation uses a left join from dim_stores
to ensure all stores are represented, including
stores without Sales transactions.

### 5.3 agg_category_sales

**Grain:** One company and product category.

**Records:** 14

- RetailMax: 6 categories
- FreshMart: 8 categories

Metrics:

- transaction_count
- units_sold
- gross_sales_amount
- discount_amount
- net_revenue
- distinct_products_sold
- distinct_customers
- distinct_stores
- average_transaction_value

The source category labels are preserved without
assuming equivalence across the two businesses.

### 5.4 agg_inventory_status

**Grain:** One company, category, stock status,
and freshness status combination.

**Records:** 59

Metrics:

- inventory_record_count
- distinct_products
- distinct_stores
- total_stock_quantity
- total_reorder_level
- reorder_shortfall_units
- estimated_inventory_value
- missing_price_record_count

**Reorder shortfall calculation:**

`max(reorder_level - stock_quantity, 0)`

**Total reorder shortfall:** 14,768 units.

Estimated inventory value uses:

`stock_quantity * product_unit_price`

This is an estimated retail selling value,
not an accounting inventory valuation.

Missing product prices are retained as NULL.
Unknown values are not represented as known zero values.

---

## 6. Data Quality and Validation

Every Gold table has an independent validation script
under the `tests/` directory.

Validation checks include:

1. Delta table format and transaction history.
2. Expected record counts.
3. Exact column names and data types.
4. Uniqueness of dimensional and aggregation keys.
5. Source-qualified identifier consistency.
6. Fact-to-dimension referential integrity.
7. Silver-to-Gold record preservation.
8. Financial equation verification.
9. Source-level reconciliation.
10. Independently reconstructed aggregation metrics.
11. Inventory classification reconciliation.
12. Missing-price and NULL handling.

**Result:** All 11 Gold tables passed their independent
validation suites.

---

## 7. Gold Table Inventory

| Table | Records |
|---|---:|
| dim_customers | 9,000 |
| dim_products | 1,800 |
| dim_suppliers | 180 |
| dim_stores | 90 |
| dim_date | 4,275 |
| fact_sales | 88,600 |
| fact_inventory | 3,600 |
| agg_daily_sales | 2,736 |
| agg_store_performance | 90 |
| agg_category_sales | 14 |
| agg_inventory_status | 59 |

---

## 8. Technology Stack

- Apache Spark / PySpark
- Delta Lake
- Amazon S3
- AWS IAM
- Python
- Git / GitHub
- WSL Ubuntu

Gold tables are currently built using full-refresh
batch processing.

Delta Lake provides transaction logging and
versioned table metadata.

---

## 9. Known Limitations

- The project uses synthetic business data.
- Customer identities are not automatically merged
  across the two companies.
- Product category harmonization is not implemented.
- Gold tables currently use full-refresh processing.
- Inventory records are not modeled as historical
  periodic snapshots.
- Inventory retail value is an estimate, not
  an accounting cost valuation.
- Missing product prices are preserved rather
  than imputed.
- The current S3 Delta configuration is designed
  for a single writer, not concurrent independent
  writers.

---

## 10. Planned Next Steps

1. Build Streamlit dashboards for Sales, Stores,
   Categories, and Inventory.
2. Add reusable pipeline orchestration.
3. Implement automated CI/CD checks.
4. Expand operational monitoring and logging.
5. Explore incremental processing.
6. Add natural-language analytics over curated data.

---

## 11. Completion Summary

The Gold layer contains 11 analytics-ready Delta tables:

- 5 dimensions
- 2 facts
- 4 business aggregations

All tables are stored in Amazon S3.

All tables passed independent data quality and
reconciliation validations.

The platform is ready for its next phase:
business intelligence and analytics.
