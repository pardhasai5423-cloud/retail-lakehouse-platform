
# Gold Layer Architecture
## Enterprise Retail Lakehouse Platform

### 1. Business Objective

Build a unified analytics layer for RetailMax and FreshMart
after their fictional merger.

The Gold layer consumes validated Silver Delta tables and
publishes dimensional models and business aggregates.

### 2. Technology Stack

- Apache Spark / PySpark
- Delta Lake
- Amazon S3
- Python
- Streamlit (planned dashboard)

### 3. Data Flow

Bronze -> Silver -> Gold -> Streamlit

Gold tables are created from trusted Silver data.

Quarantined Sales records are excluded from Gold revenue
calculations.

### 4. Dimensional Model

Dimensions:
- dim_customers
- dim_products
- dim_stores
- dim_suppliers
- dim_date

Facts:
- fact_sales
- fact_inventory

Planned aggregates:
- agg_daily_sales
- agg_store_performance
- agg_category_sales
- agg_inventory_status

### 5. Table Grain

dim_customers:
One row per source-qualified customer_key.

dim_products:
One row per source-qualified product_key.

dim_stores:
One row per source-qualified store_key.

dim_suppliers:
One row per source-qualified supplier_key.

dim_date:
One row per calendar date.

fact_sales:
One row per validated source Sales transaction_key.

fact_inventory:
One row per source inventory_key.

### 6. Key Relationships

fact_sales.customer_key -> dim_customers.customer_key

fact_sales.product_key -> dim_products.product_key

fact_sales.store_key -> dim_stores.store_key

fact_sales.transaction_date -> dim_date.calendar_date

fact_inventory.product_key -> dim_products.product_key

fact_inventory.store_key -> dim_stores.store_key

dim_products.supplier_key -> dim_suppliers.supplier_key

### 7. Sales Measures

- quantity
- unit_price
- discount_amount
- total_amount

Revenue is calculated from trusted total_amount values.

The current model assumes each source transaction_key
identifies one Sales record.

### 8. Inventory Measures

- stock_quantity
- reorder_level

Inventory analysis will also include stock status and
freshness classifications.

### 9. Data Quality Requirements

- No duplicate fact business keys
- No duplicate dimension business keys
- Valid fact-to-dimension references
- Financial calculations consistent with Silver
- Source lineage retained
- No quarantined Sales included in fact_sales
- Source-qualified identities retained

### 10. S3 Layout

s3://retail-lakehouse-parsam-2026/gold/
    dim_customers/
    dim_products/
    dim_stores/
    dim_suppliers/
    dim_date/
    fact_sales/
    fact_inventory/
    agg_daily_sales/
    agg_store_performance/
    agg_category_sales/
    agg_inventory_status/

### 11. Implementation Strategy

Phase 1: Build dimension tables.

Phase 2: Build fact tables.

Phase 3: Validate dimensional relationships and measures.

Phase 4: Build business aggregates.

Phase 5: Connect the Gold layer to Streamlit dashboards.

### 12. Current Scope

This initial Gold implementation uses full-refresh
processing for the development dataset.

Future improvements may include:
- Incremental processing
- MERGE-based upserts
- Slowly Changing Dimensions
- Automated orchestration
- Historical inventory snapshots
- Cross-company customer identity resolution
