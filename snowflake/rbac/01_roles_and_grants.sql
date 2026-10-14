-- ==============================================================================
-- GitOps Data Platform: Enterprise RBAC & Security for Snowflake Iceberg Tables
-- File: snowflake/rbac/01_roles_and_grants.sql
-- Description: Establishes zero-trust Role-Based Access Control (RBAC) hierarchy
--              for Data Engineering and Data Analytics personas.
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1. Provision Functional Roles
-- ------------------------------------------------------------------------------
USE ROLE USERADMIN;

CREATE ROLE IF NOT EXISTS DATA_ENGINEER_ROLE
  COMMENT = 'Full engineering access to Snowflake Iceberg tables, stages, and refresh operations';

CREATE ROLE IF NOT EXISTS DATA_ANALYST_ROLE
  COMMENT = 'Read-only querying access to curated Iceberg dimensional and fact tables';

-- ------------------------------------------------------------------------------
-- 2. Build Role Hierarchy (Inheritance to SYSADMIN)
-- ------------------------------------------------------------------------------
USE ROLE SECURITYADMIN;

GRANT ROLE DATA_ENGINEER_ROLE TO ROLE SYSADMIN;
GRANT ROLE DATA_ANALYST_ROLE TO ROLE SYSADMIN;

-- ------------------------------------------------------------------------------
-- 3. Virtual Warehouse Usage Grants
-- ------------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

GRANT USAGE, OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE DATA_ENGINEER_ROLE;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE DATA_ANALYST_ROLE;

-- ------------------------------------------------------------------------------
-- 4. Database & Schema Grants
-- ------------------------------------------------------------------------------
GRANT USAGE ON DATABASE ANALYTICS_DEV TO ROLE DATA_ENGINEER_ROLE;
GRANT USAGE ON DATABASE ANALYTICS_DEV TO ROLE DATA_ANALYST_ROLE;

GRANT USAGE ON SCHEMA ANALYTICS_DEV.SILVER_ICEBERG TO ROLE DATA_ENGINEER_ROLE;
GRANT USAGE ON SCHEMA ANALYTICS_DEV.SILVER_ICEBERG TO ROLE DATA_ANALYST_ROLE;

-- ------------------------------------------------------------------------------
-- 5. Iceberg Table Level Privileges
-- ------------------------------------------------------------------------------
-- Data Engineer: Full privileges including ALTER/REFRESH
GRANT ALL PRIVILEGES ON TABLE ANALYTICS_DEV.SILVER_ICEBERG.employees_iceberg TO ROLE DATA_ENGINEER_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA ANALYTICS_DEV.SILVER_ICEBERG TO ROLE DATA_ENGINEER_ROLE;

-- Data Analyst: Read-only SELECT access
GRANT SELECT ON TABLE ANALYTICS_DEV.SILVER_ICEBERG.employees_iceberg TO ROLE DATA_ANALYST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ANALYTICS_DEV.SILVER_ICEBERG TO ROLE DATA_ANALYST_ROLE;

-- ------------------------------------------------------------------------------
-- 6. Integration and Volume Grants for Data Engineers
-- ------------------------------------------------------------------------------
GRANT USAGE ON INTEGRATION glue_iceberg_catalog_int TO ROLE DATA_ENGINEER_ROLE;
GRANT USAGE ON EXTERNAL VOLUME ext_vol_silver_iceberg TO ROLE DATA_ENGINEER_ROLE;
