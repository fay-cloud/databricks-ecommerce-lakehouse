# Production-Grade E-Commerce Lakehouse on Azure and Databricks

An end-to-end data engineering project built with **Databricks, PySpark, Delta Lake, Unity Catalog, Auto Loader, Lakeflow Jobs, and Databricks Asset Bundles**. The project transforms the public Olist Brazilian e-commerce dataset into governed, quality-checked, analytics-ready lakehouse tables using the Medallion Architecture.

## Project Overview

This project demonstrates how to build and operate a maintainable lakehouse pipeline with structured ingestion, data transformation, data quality validation, quarantine handling, dimensional modelling, orchestration, and execution auditing.

The pipeline processes historical e-commerce data covering customers, orders, products, sellers, payments, reviews, and geolocation. It separates raw ingestion from transformation and analytical modelling to improve traceability, reliability, and maintainability.

**Current implementation:** Bronze, Silver, and Gold pipelines are implemented in the development environment. Bronze audit logging has been introduced, and Silver audit integration is the next development step.

## Architecture

```text
Olist Public E-Commerce Dataset
              |
              v
       Raw CSV Files
              |
              v
     Bronze Ingestion
  Auto Loader + Checkpoints
  Review CSV Batch Rebuild
              |
              v
       Bronze Delta Tables
              |
              v
    Silver Transformations
 Cleaning + Standardisation
   Validation + Quarantine
              |
              v
       Silver Delta Tables
              |
              v
     Gold Transformations
 Dimensions + Fact Tables
     Daily Sales Mart
              |
              v
       Analytics and BI

 Cross-cutting capabilities:
 Unity Catalog | Lakeflow Jobs
 Data Quality | Pipeline Audit
 GitHub | Databricks Asset Bundles
```

### Medallion layers

| Layer | Purpose | Current implementation |
|---|---|---|
| Bronze | Preserve ingested source records and ingestion metadata | Implemented |
| Silver | Clean, standardise, validate, and quarantine records | Implemented |
| Gold | Create analytical dimensions, facts, and aggregated sales data | Implemented |
| Monitoring | Record pipeline execution and dataset audit metrics | Bronze audit logging implemented; Silver integration next |

## Technology Stack

- **Platform:** Azure Databricks
- **Processing:** Python, PySpark, Spark SQL
- **Storage format:** Delta Lake
- **Governance:** Unity Catalog
- **Ingestion:** Apache Spark Auto Loader for eight datasets; controlled batch ingestion for reviews
- **Orchestration:** Lakeflow Jobs
- **Deployment configuration:** Databricks Asset Bundles
- **Version control:** Git and GitHub
- **Data quality:** Validation rules, reconciliation checks, referential integrity checks, and quarantine tables

The current development environment uses Databricks-managed volumes. **ADLS Gen2 integration is a planned storage enhancement** and is not represented as an already configured connection.

## Data Source

The project uses the public Olist Brazilian E-Commerce dataset, containing historical marketplace transactions.

Nine source CSV files are processed:

1. `olist_customers_dataset.csv`
2. `olist_geolocation_dataset.csv`
3. `olist_order_items_dataset.csv`
4. `olist_order_payments_dataset.csv`
5. `olist_order_reviews_dataset.csv`
6. `olist_orders_dataset.csv`
7. `olist_products_dataset.csv`
8. `olist_sellers_dataset.csv`
9. `product_category_name_translation.csv`

The source is historical data, not a native streaming or change-data-capture feed. Auto Loader provides incremental file discovery and checkpoint-based processing, while the review dataset uses a separately validated batch rebuild because of CSV parsing issues encountered with its multiline comments.

## Bronze Layer

Bronze ingestion is implemented in `notebooks/development/bronze_ingestion.py`.

Implemented capabilities:

- Ingest eight datasets using Auto Loader.
- Maintain per-dataset schema locations and checkpoints.
- Preserve source file metadata and ingestion timestamps.
- Use rescued data handling for schema evolution.
- Validate the expected source files before ingestion.
- Rebuild the review dataset using explicit CSV parsing options, staging, validation, and controlled publication.
- Limit review publication to a validated snapshot with the expected row count and valid review identifiers.

Auto Loader checkpoints support incremental file processing. They do not, by themselves, guarantee record-level deduplication or full end-to-end idempotency across every failure scenario.

## Silver Layer

Silver transformations are implemented in `notebooks/development/silver_transformations.py`.

The pipeline applies cleaning, standardisation, and data quality checks before publishing curated tables. Invalid records are separated into quarantine tables, and critical quality checks can prevent invalid outputs from being accepted.

Implemented checks include:

- Required identifier and field validation.
- Business-key uniqueness checks where appropriate.
- Referential integrity between related datasets.
- Payment and review validation.
- Source-to-target reconciliation.
- Quarantine handling for invalid records.

Review records use the composite key `(review_id, order_id)` for uniqueness validation. A repeated `review_id` alone is monitored rather than automatically treated as a duplicate record, because review identifiers can occur across different orders in this source.

## Gold Layer

Gold transformations are implemented in `notebooks/development/gold_transformations.py`.

### Dimension tables

| Table | Grain |
|---|---|
| `de_dev.gold.dim_customer` | One row per customer identifier |
| `de_dev.gold.dim_product` | One row per product identifier |
| `de_dev.gold.dim_seller` | One row per seller identifier |

The product dimension incorporates the product category translation data.

### Fact tables and aggregate

| Table | Grain |
|---|---|
| `de_dev.gold.fact_order_items` | One row per order and order item |
| `de_dev.gold.fact_order_payments` | One row per order and payment sequence |
| `de_dev.gold.fact_order_reviews` | One row per source review/order record |
| `de_dev.gold.gold_daily_sales` | One row per purchase date and order status |

Keeping order items and payments in separate fact tables avoids multiplying item-level amounts when an order has multiple payment records.

The daily sales aggregate includes order counts, item counts, product revenue, freight, total item-plus-freight value, average item price, and a daily late-delivery metric.

## Current Validation Results

The following results were verified in the development environment after the review ingestion correction and pipeline reruns.

### Layer row counts

| Dataset | Bronze | Silver | Gold |
|---|---:|---:|---:|
| Order items | 112,650 | 112,650 | 112,650 |
| Payments | 103,886 | 103,886 | 103,886 |
| Reviews | 99,224 | 99,224 | 99,224 |

Additional verified Silver counts:

| Table | Rows |
|---|---:|
| Orders | 99,441 |
| Customers | 99,441 |
| Products | 32,951 |
| Sellers | 3,095 |
| Geolocation | 1,000,163 |
| Product category translations | 71 |

Verified Gold counts:

| Table | Rows |
|---|---:|
| `dim_customer` | 99,441 |
| `dim_product` | 32,951 |
| `dim_seller` | 3,095 |
| `fact_order_items` | 112,650 |
| `fact_order_payments` | 103,886 |
| `fact_order_reviews` | 99,224 |
| `gold_daily_sales` | 1,783 |

### Quality and reconciliation checks

- The review source was rebuilt and validated at 99,224 rows, with zero invalid rows and zero duplicate `(review_id, order_id)` pairs.
- Order-item, payment, and review counts reconcile between Silver and their corresponding Gold facts.
- The order-item fact has 112,650 distinct `(order_id, order_item_id)` keys.
- The payment fact has 103,886 distinct `(order_id, payment_sequential)` keys.
- The review fact has 99,224 distinct generated `review_row_key` values.
- Verified referential integrity checks returned zero violations for order items, orders, customers, products, sellers, payments, and reviews.
- Repeated pipeline executions maintained the verified order-item, payment, and review counts.

These checks demonstrate the verified scenarios above. Broader failure recovery, late-arriving data, schema-change replay, and production-scale performance still require additional testing.

## Pipeline Auditing

A Delta audit table has been created:

`de_dev.monitoring.pipeline_audit`

It records fields for pipeline and task status, environment, dataset and layer, execution timestamps, duration, source and output row counts, quarantined row counts, validation status, and error messages.

Current status:

- Bronze dataset and pipeline audit logging has been introduced.
- Completed executions can record success or failure.
- Auto Loader counts represent newly processed input rows when available; cumulative table counts are recorded separately.
- Review batch rebuild counts represent the validated snapshot.
- Silver-specific audit metrics and Gold audit integration remain to be completed.
- Historical `RUNNING` records from earlier executions are retained; they should not be interpreted as proof of a currently active run without correlating later records.

## Orchestration and Deployment

The pipeline is defined through Databricks Asset Bundles and a Lakeflow Job.

Configured tasks:

1. `bronze_ingestion`
2. `silver_transformations`
3. `gold_transformations`

Tasks run in dependency order, and `max_concurrent_runs: 1` prevents overlapping runs of this job. Each task is configured with one retry.

### Configured deployment targets

| Target | Catalog |
|---|---|
| `dev` | `de_dev` |
| `test` | `de_test` |
| `stage` | `de_stage` |
| `prod` | `de_prod` |

The targets are configured in `databricks.yml`. Successful development deployment does not by itself establish that every environment has been deployed and validated.

### Run the development pipeline

From the repository directory in Databricks Web Terminal:

```bash
cd /Workspace/Users/fayomik02@gmail.com/databricks-ecommerce-lakehouse

databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev ecommerce_pipeline
```

These commands validate the bundle, deploy the development configuration, and trigger the complete job.

## Repository Structure

```text
databricks-ecommerce-lakehouse/
├── databricks.yml
├── resources/
│   └── ecommerce_pipeline.yml
├── notebooks/
│   └── development/
│       ├── bronze_ingestion.py
│       ├── silver_transformations.py
│       ├── gold_transformations.py
│       └── pipeline_audit.py
└── README.md
```

## Development Roadmap

- [x] Configure Databricks Asset Bundles and environment targets.
- [x] Implement Bronze ingestion for the nine source datasets.
- [x] Correct and validate review CSV ingestion.
- [x] Implement Silver transformations and quarantine handling.
- [x] Build Gold dimensions, fact tables, and daily sales aggregation.
- [x] Validate key uniqueness, referential integrity, and selected layer reconciliations.
- [x] Create the pipeline audit Delta table and introduce Bronze audit logging.
- [ ] Integrate detailed source, output, quarantine, and validation metrics into Silver audit records.
- [ ] Extend audit logging to Gold and add operational monitoring queries.
- [ ] Test failure recovery, replay, late-arriving data, and schema evolution scenarios.
- [ ] Integrate ADLS Gen2 as the external raw-data storage target.
- [ ] Add automated CI/CD checks and validate deployments across test, stage, and production.
- [ ] Evaluate performance, access controls, and production operational readiness.

## Repository

GitHub: https://github.com/fay-cloud/databricks-ecommerce-lakehouse

---

**Project status:** The core Bronze-to-Silver-to-Gold pipeline is implemented and key data quality and reconciliation checks have passed in development. Monitoring and audit coverage, broader resilience testing, cloud storage integration, and production deployment hardening remain active work.
