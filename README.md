#Project Summary: Building a Data Lakehouse with Databricks Declarative Pipelines (DLT)

This project demonstrates how to build an end-to-end data pipeline using Databricks Declarative Pipelines, following the best practices of a medallion architecture to process data in distinct layers.

* 1. Bronze Layer: Raw Data Ingestion
The Bronze layer is the entry point for all raw data. The goal is to ingest data efficiently while maintaining its original format and structure.

** Code Pattern: ** Uses dlt.create_streaming_table and @dlt.append_flow.

** Purpose: ** @dlt.append_flow declaratively appends data from multiple sources (e.g., sales_east, sales_west) into a single, unified streaming table (e.g., sales_stg). This simplifies the ingestion of multiple data streams into a single point, which is a common task in modern data architectures.

* 2. Silver Layer: Cleansing, Transformation, and CDC
The Silver layer takes the raw data from the Bronze layer, applies quality checks and transformations, and manages historical changes. This is where the data becomes a reliable "single source of truth."

** Code Pattern: ** Combines @dlt.view with dlt.create_auto_cdc_flow.

Purpose: The @dlt.view encapsulates all transformation logic (e.g., calculating total_amount, casting data types). This separates the business logic from the physical storage, which is a best practice for clean code and data lineage.

Change Data Capture (CDC): dlt.create_auto_cdc_flow is a powerful command that automates the complex MERGE logic for managing updates. The stored_as_scd_type parameter defines how changes are handled:

Type 1: Overwrites the existing record. Used for tables where only the latest state is needed (e.g., products_enr).

Type 2: Creates a new record to preserve a complete history of changes. This is essential for building dimension tables (e.g., dim_customers) used for historical analysis.

3. Gold Layer: Business Analytics & Aggregation
The Gold layer contains the final, aggregated, and business-ready data. It's optimized for fast querying and reporting, typically in the form of fact and dimension tables.

Code Pattern: Uses @dlt.table to create a materialized view.

Purpose: The code joins fact tables with dimension tables to create a comprehensive view for business analysis.

Critical Detail: When joining with SCD Type 2 dimension tables, a simple join on the primary key is incorrect. The join condition must include a date range to link each fact record to the correct historical version of the dimension record. This prevents data duplication and ensures the accuracy of aggregated results.
