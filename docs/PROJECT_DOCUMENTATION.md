# E-Commerce Lakehouse Data Platform
## Technical Project Documentation

**Platform:** Databricks Lakehouse  
**Architecture:** Medallion Architecture  
**Processing:** Python, PySpark, Delta Lake  
**Orchestration:** Databricks Lakeflow Jobs  
**Version Control and Deployment:** GitHub and Databricks Asset Bundles  
**Environment:** Development  
**Project Status:** Core pipeline implemented; additional production-hardening and cloud integration planned.

---

# 1. Executive Summary

The E-Commerce Lakehouse Data Platform is a data engineering project designed to ingest, validate, transform, and model e-commerce data into business-ready analytical datasets.

The platform uses Azure Databricks, Apache Spark, Delta Lake, and a Medallion Architecture to organise data processing into Bronze, Silver, and Gold layers. A separate Quarantine layer handles rejected records, while a central monitoring table records pipeline execution and validation outcomes.

The project uses the historical Olist Brazilian e-commerce dataset, which contains information about customers, orders, products, sellers, payments, reviews, and geographic attributes.

Databricks Lakeflow Jobs orchestrates the pipeline, executing Bronze ingestion, Silver transformations, and Gold transformations in dependency order. GitHub and Databricks Asset Bundles provide version control and deployment management.

The current implementation runs in a Databricks development environment. Azure Data Lake Storage Gen2 integration, incremental change processing, and additional operational hardening are identified as future enhancements rather than completed capabilities.

# 2. Business Context and Problem Statement

E-commerce data is generated across multiple business domains. Orders, customers, products, payments, reviews, and delivery information must be integrated before meaningful analysis can be performed.

Without a structured data platform, downstream consumers may encounter inconsistent data formats, duplicate records, incomplete attributes, invalid relationships, and incorrect aggregations.

The platform addresses these challenges by implementing a repeatable processing architecture that separates raw ingestion, data cleansing, analytical modelling, and business reporting.

The intended business outcomes include:

- Consistent analytical datasets.
- Improved visibility into data quality.
- Traceability of pipeline executions.
- Reliable modelling of order items, payments, and reviews.
- Support for sales, payment, customer, and delivery analysis.

# 3. Project Objectives

The project objectives are:

1. Implement a complete Bronze-to-Gold processing workflow.
2. Ingest nine historical source datasets.
3. Apply data standardisation, validation, and quarantine handling.
4. Build fact and dimension tables with explicit business grains.
5. Prevent incorrect monetary aggregation caused by fact-table join fan-out.
6. Introduce data-quality gates and audit logging.
7. Orchestrate tasks using Databricks Lakeflow Jobs.
8. Manage code and deployment configuration through GitHub and Databricks Asset Bundles.
9. Establish a foundation for incremental processing and production deployment.
10. Document the architecture, operational behaviour, and implementation status.

# 4. System Architecture

The system consists of the following logical components:

1. Source data and raw file storage.
2. Ingestion and file-processing mechanisms.
3. Bronze raw-data layer.
4. Silver cleansing and validation layer.
5. Quarantine layer.
6. Gold analytical layer.
7. Consumption and analytics.
8. Orchestration and monitoring.
9. Version control and deployment.
10. Governance and security.

## 4.1 Architecture diagram

The end-to-end architecture diagram is maintained in:

![E-Commerce Lakehouse System Architecture](docs/architecture.png)

The diagram illustrates the source-to-consumption flow and distinguishes the current development implementation from planned cloud integration and future enhancements.

## 4.2 End-to-end data flow

The implemented processing flow is:

**Historical source files → Bronze ingestion → Silver transformations → Gold transformations → Analytical consumption**

The Quarantine layer receives records rejected by applicable Silver validation rules. Audit records are written to a central monitoring table.

The current raw-file access approach uses Databricks volumes. ADLS Gen2 is the planned cloud storage integration.

# 5. Data Sources

The project uses nine datasets from the Olist Brazilian e-commerce dataset.

| Dataset | Description |
|---|---|
| `olist_customers_dataset.csv` | Customer records |
| `olist_geolocation_dataset.csv` | Geographic attributes |
| `olist_order_items_dataset.csv` | Items belonging to orders |
| `olist_order_payments_dataset.csv` | Payment records |
| `olist_order_reviews_dataset.csv` | Customer reviews |
| `olist_orders_dataset.csv` | Order status and lifecycle timestamps |
| `olist_products_dataset.csv` | Product attributes |
| `olist_sellers_dataset.csv` | Seller records |
| `product_category_name_translation.csv` | Product category translations |

These are historical source files rather than a live transactional CDC feed.

## 5.1 Ingestion strategy

Eight datasets use Databricks Auto Loader for file discovery and incremental ingestion.

Order reviews use a specialised batch-ingestion process because multiline review comments require specific CSV parsing behaviour.

The review process writes to a staging table, validates the data, and publishes the validated result to the Bronze review table.

# 6. Data Architecture and Medallion Layers

## 6.1 Bronze: Raw ingestion

**Schema:** `de_dev.bronze`

The Bronze layer preserves ingested source datasets in Delta tables.

Responsibilities include:

- Ingesting source data.
- Maintaining Auto Loader checkpoint state for applicable datasets.
- Retaining source metadata where configured.
- Preserving the original source representation as far as the ingestion process permits.
- Applying specialised staging and validation for reviews.

The nine Bronze tables are:

- `olist_customers`
- `olist_geolocation`
- `olist_order_items`
- `olist_order_payments`
- `olist_order_reviews`
- `olist_orders`
- `olist_products`
- `olist_sellers`
- `product_category_translation`

The Bronze layer is designed to provide a traceable starting point for downstream transformations.

## 6.2 Silver: Cleansing and validation

**Schema:** `de_dev.silver`

The Silver layer applies standardisation and data-quality rules to the Bronze datasets.

Responsibilities include:

- Cleansing and standardising records.
- Applying schema and business-rule validation.
- Checking business-key uniqueness.
- Validating relationships between datasets.
- Routing rejected records to quarantine tables.
- Applying quality gates before Gold processing.

The current implementation uses full-refresh overwrite operations for Silver outputs.

Incremental MERGE processing for changes to existing records has not yet been implemented as a complete end-to-end capability.

## 6.3 Quarantine: Rejected records

**Schema:** `de_dev.quarantine`

The Quarantine layer stores records rejected by the implemented validation rules.

It provides a place to investigate invalid records without silently treating them as valid analytical data.

The current implementation includes quarantine handling in Silver transformations. Automated remediation and corrected-record reprocessing require further development.

## 6.4 Gold: Analytical modelling

**Schema:** `de_dev.gold`

The Gold layer contains dimensional tables, fact tables, and aggregated datasets designed for analytical use.

### Dimensions

| Table | Purpose |
|---|---|
| `dim_customer` | Customer analysis |
| `dim_product` | Product analysis |
| `dim_seller` | Seller analysis |

### Fact tables

| Table | Grain |
|---|---|
| `fact_order_items` | One row per `(order_id, order_item_id)` |
| `fact_order_payments` | One row per `(order_id, payment_sequential)` |
| `fact_order_reviews` | One row per source review record, with a generated review-row key |

### Aggregated dataset

| Table | Purpose |
|---|---|
| `gold_daily_sales` | Daily metrics grouped by purchase date and order status |

Gold transformations currently use full-refresh overwrites.

# 7. Data Modelling Decisions

## 7.1 Separate order-item and payment facts

Order items and payments have different grains. An order can contain multiple items and multiple payment records.

Joining these datasets directly at order level can multiply rows and overstate revenue or payment values.

The platform therefore stores order items and payments in separate fact tables. This preserves their individual grains and supports more reliable aggregation.

## 7.2 Composite business keys

The principal keys include:

- Orders: `order_id`
- Order items: `(order_id, order_item_id)`
- Payments: `(order_id, payment_sequential)`
- Reviews: `(review_id, order_id)` for source-level uniqueness checks

Review IDs may occur across different orders in the source data, so `review_id` alone is not sufficient as the source uniqueness key.

## 7.3 Daily sales aggregation

`gold_daily_sales` aggregates order-item data by purchase date and order status.

The output contains measures such as:

- Order counts.
- Item counts.
- Product revenue.
- Freight value.
- Item-plus-freight value.
- Average item price.
- Average late-delivery indicator.

Because the dataset is aggregated, the number of Gold daily-sales rows is expected to be smaller than the number of source order-item records.

# 8. Data Quality and Validation

The pipeline includes quality checks intended to prevent invalid data from silently propagating into analytical outputs.

Implemented checks include:

- Business-key uniqueness.
- Schema and data validation.
- Referential-integrity validation.
- Quarantine handling.
- Review staging and validation before publishing.
- Source-to-output row-count reconciliation.
- Gold fact-grain validation.

## 8.1 Example: payment fact validation

A previously verified development result was:

| Metric | Result |
|---|---:|
| Payment records | 103,886 |
| Distinct composite payment keys | 103,886 |
| Duplicate payment keys | 0 |
| Zero-value payment records | 9 |
| Undefined payment types | 3 |

The zero-value payment records were retained for investigation rather than automatically deleted.

These results are evidence from a previous development validation run, not a permanent guarantee about future source data.

## 8.2 Idempotency and rerun behaviour

The current implementation includes several controls relevant to repeatable execution:

- Auto Loader checkpoints.
- Full-refresh overwrite operations in Silver and Gold.
- Business-key validation.
- Validated staging and publishing for reviews.
- Lakeflow Job retry configuration.

Normal reruns, duplicate-key checks, and a duplicate-file test were reported as successful during development.

Full recovery from partial failures has not been independently verified. File-level checkpointing and record-level deduplication are separate concerns, especially when identical source content arrives under a new filename.

# 9. Orchestration

The pipeline is orchestrated using Databricks Lakeflow Jobs.

The dependency graph is:

`bronze_ingestion → silver_transformations → gold_transformations`

## 9.1 Job configuration

| Property | Configuration |
|---|---|
| Job resource | `ecommerce_pipeline` |
| Job name pattern | `ecommerce_lakehouse_${bundle.target}` |
| Maximum concurrent runs | 1 |
| Retry configuration | One retry per task |
| Compute | Serverless |
| Environment version | 2 |
| Schedule | Daily at 06:00 Europe/Amsterdam |

The schedule is configured for the development job. A successful automatic scheduled execution should be verified from run history after the schedule triggers.

The retry configuration establishes the intended retry policy but does not independently prove recovery from all failure scenarios.

# 10. Monitoring and Audit Logging

The central audit table is:

`de_dev.monitoring.pipeline_audit`

It stores execution information including:

- Audit identifier.
- Pipeline name.
- Environment.
- Task name.
- Dataset name.
- Processing layer.
- Execution status.
- Start and end timestamps.
- Duration.
- Source and output row counts.
- Quarantined row counts.
- Validation status.
- Error details where supplied.

Successful audit records have been verified for Bronze, Silver, and Gold processing.

The current audit framework still requires further hardening for complete failure auditing, reliable job-run-ID correlation, per-table duration measurement, and automated notifications.

# 11. Version Control and Deployment

The source repository is:

`https://github.com/fay-cloud/databricks-ecommerce-lakehouse`

The `main` branch is used as the primary source-control branch.

The project uses Databricks Asset Bundles to manage job resources and deployment configuration.

## 11.1 Bundle validation

```bash
databricks bundle validate -t dev
```

## 11.2 Development deployment

```bash
databricks bundle deploy -t dev
```

## 11.3 Pipeline execution

```bash
databricks bundle run -t dev ecommerce_pipeline
```

These commands were successfully used to validate and deploy the development configuration and execute the pipeline.

Development deployment is verified. Full automated CI/CD and promotion across test, stage, and production remain further work.

# 12. Repository Structure

The core project structure is:

```text
databricks-ecommerce-lakehouse/
│
├── databricks.yml
├── README.md
│
├── resources/
│   └── ecommerce_pipeline.yml
│
├── notebooks/
│   └── development/
│       ├── bronze_ingestion.py
│       ├── silver_transformations.py
│       ├── gold_transformations.py
│       └── pipeline_audit.py
│
└── docs/
    ├── E-Commerce Lakehouse System Architecture.png
    └── PROJECT_DOCUMENTATION.md
```

The documentation directory contains the system architecture diagram and this technical document.

# 13. Security and Governance

Unity Catalog provides the intended governance foundation for organising and controlling access to data assets.

Relevant governance areas include:

- Catalog, schema, table, and volume organisation.
- Access permissions.
- Metadata and lineage.
- Secure credential management.
- Personally identifiable information handling.
- GDPR considerations.

The existence of Unity Catalog objects does not establish that every security policy or compliance control has been fully implemented or audited. Production security configuration requires separate verification.

# 14. Limitations and Future Development

The current project is a functioning development implementation, but several production-readiness areas remain.

## 14.1 Cloud storage

ADLS Gen2 integration is planned. The current raw-file workflow uses Databricks-managed volumes.

## 14.2 Incremental processing

Silver and Gold currently use full-refresh overwrites. Incremental Delta MERGE processing should be introduced selectively where supported by the source change patterns and business keys.

## 14.3 Failure recovery

Partial-failure recovery needs dedicated testing. The current job has retries configured, but safe recovery from every possible failure has not been established.

## 14.4 Monitoring and alerting

The central audit framework is implemented, while complete failure auditing and automated notifications require further development or verification.

## 14.5 Deployment automation

GitHub and Databricks Asset Bundles support version control and development deployment. Automated CI/CD and controlled promotion across all environments remain future work.

## 14.6 Business consumption

The Gold layer provides analytical datasets. Production dashboards and external consumer integrations should be documented as implemented only when they have actually been built and verified.

# 15. Implementation Summary

| Capability | Status |
|---|---|
| Bronze ingestion | Implemented |
| Silver cleansing and validation | Implemented |
| Quarantine handling | Implemented |
| Gold dimensional modelling | Implemented |
| Data-quality checks | Implemented and tested |
| Pipeline audit logging | Implemented; successful records verified |
| Lakeflow Job orchestration | Configured and executed |
| Development schedule | Configured |
| GitHub version control | Verified |
| Bundle validation and deployment | Verified in development |
| Partial-failure recovery | Not independently verified |
| Incremental change processing | Planned |
| ADLS Gen2 integration | Planned |
| Automated failure notifications | Planned or requires verification |
| Full production deployment | Not yet established |

# 16. Conclusion

This project establishes an end-to-end development lakehouse with ingestion, cleansing, data-quality validation, analytical modelling, orchestration, and audit logging.

Its key engineering decisions include using a Medallion Architecture, maintaining separate fact tables at defined grains, validating data before publishing critical datasets, and managing the pipeline through Databricks Lakeflow Jobs and Asset Bundles.

The implementation provides a foundation for further development in incremental processing, cloud storage integration, failure recovery, operational monitoring, governance, and automated environment promotion.

The project documentation distinguishes verified implementation from future work to provide an accurate account of the platform's current capabilities.

---

## Project Repository

GitHub: https://github.com/fay-cloud/databricks-ecommerce-lakehouse

System architecture: `docs/E-Commerce Lakehouse System Architecture.png`
