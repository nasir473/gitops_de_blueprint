-- ==============================================================================
-- GitOps Data Platform: Snowflake Iceberg Table Creation & Verification
-- File: snowflake/tables/01_iceberg_tables.sql
-- Description: Provisions the Silver Iceberg Table in Snowflake synced with AWS Glue.
-- ==============================================================================

USE ROLE SYSADMIN;

-- ------------------------------------------------------------------------------
-- 1. Database & Schema Initialization
-- ------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS ANALYTICS_DEV
  COMMENT = 'Analytics serving layer for Modern Data Platform';

CREATE SCHEMA IF NOT EXISTS ANALYTICS_DEV.SILVER_ICEBERG
  COMMENT = 'Curated Apache Iceberg tables mapped to AWS S3 Silver warehouse';

USE SCHEMA ANALYTICS_DEV.SILVER_ICEBERG;

-- ------------------------------------------------------------------------------
-- 2. Create Snowflake Iceberg Table (Glue Catalog Synced)
-- ------------------------------------------------------------------------------
CREATE OR REPLACE ICEBERG TABLE employees_iceberg
  EXTERNAL_VOLUME = 'ext_vol_silver_iceberg'
  CATALOG = 'glue_iceberg_catalog_int'
  CATALOG_TABLE_NAME = 'employees_iceberg';

-- ------------------------------------------------------------------------------
-- 3. Verification & Metadata Exploration
-- ------------------------------------------------------------------------------
-- View discovered column types, partition keys, and metadata pointers
DESC ICEBERG TABLE employees_iceberg;

-- Query the table (Zero copy execution directly over S3 Silver Parquet files)
SELECT * FROM employees_iceberg LIMIT 10;

-- Aggregate metrics across departments
SELECT
    department,
    COUNT(*) AS total_employees,
    ROUND(AVG(salary), 2) AS avg_salary,
    MIN(salary) AS min_salary,
    MAX(salary) AS max_salary
FROM employees_iceberg
GROUP BY department
ORDER BY avg_salary DESC;

-- ------------------------------------------------------------------------------
-- 4. Operational Maintenance: Refresh Table Metadata
-- Note: Triggered manually or via Airflow DAG task after AWS Glue completes
-- ------------------------------------------------------------------------------
ALTER ICEBERG TABLE employees_iceberg REFRESH;
