# ETL_AI_Integration
AI-Driven ETL Pipeline with Automated CDC/SCD in Snowflake 🚀
Overview
This project builds a next-generation ETL pipeline in Snowflake, powered by AI integration (Facebook/BART-Large-MNLI model). The pipeline intelligently processes structured datasets, identifies changes, and automatically generates Change Data Capture (CDC) and Slowly Changing Dimensions (SCD) tables for any dataset present in the Snowflake schema.

Key Features
✅ AI-Powered Data Processing - Uses Facebook/BART-Large-MNLI for intelligent classification and processing.
✅ Automated CDC & SCD - No manual intervention is needed; the pipeline detects changes and applies Type-1 & Type-2 SCD rules.
✅ Snowflake Integration - Directly processes tables from Snowflake and creates CDC/SCD tables dynamically.
✅ Scalable ETL Architecture - Handles large datasets with automated schema adaptation.

ETL Process Flow
1️⃣ Extract: Data is ingested from various sources into Snowflake.
2️⃣ Transform: AI-powered classification using Facebook/BART-Large-MNLI categorizes changes.
3️⃣ Load: The pipeline creates CDC & SCD tables dynamically for all tables in the schema.

Dataset Example
The pipeline processes datasets with the following structure:

id	first_name	last_name	gender	City	JobTitle	Salary	Latitude	Longitude
1	John	Doe	Male	NY	Engineer	100000	40.7128	-74.0060
2	Alice	Smith	Female	LA	Manager	120000	34.0522	-118.2437
AI Integration: Facebook/BART-Large-MNLI
The pipeline leverages Facebook's BART-Large-MNLI model for:
🔹 Detecting Data Changes - Identifies inserts, updates, and deletions.
🔹 Classifying Business Rules - Automates decision-making for CDC/SCD.
🔹 Ensuring Data Integrity - Reduces human intervention for ETL automation.

Snowflake Tables Created
Original Tables: Tables in the existing Snowflake schema.
CDC Tables (table_cdc): Captures row-level changes with timestamps.
SCD-2 Tables (table_scd): Maintains history with effective_date and expiry_date.


Tech Stack Used
🚀 Snowflake - Cloud Data Warehouse
🤖 Facebook/BART-Large-MNLI - AI-based classification
🐍 Python - ETL scripting
📊 Pandas & SQLAlchemy - Data processing
⚡ Apache Airflow (optional) - Workflow orchestration
