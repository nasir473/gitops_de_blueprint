-- ==============================================================================
-- GitOps Data Platform: Snowflake Catalog Integration for AWS Glue Data Catalog
-- File: snowflake/stages/02_catalog_integration.sql
-- Description: Connects Snowflake to the AWS Glue Data Catalog for automatic
--              Iceberg schema, partition, and metadata synchronization.
-- Prerequisites:
--   1. Run with ACCOUNTADMIN role.
--   2. AWS Glue Catalog Database (gitops_dev_db) created.
-- ==============================================================================

USE ROLE ACCOUNTADMIN;

-- ------------------------------------------------------------------------------
-- 1. Create Catalog Integration pointing to AWS Glue
-- ------------------------------------------------------------------------------
CREATE OR REPLACE CATALOG INTEGRATION glue_iceberg_catalog_int
  CATALOG_SOURCE = GLUE
  CATALOG_NAMESPACE = 'gitops_dev_db'
  TABLE_FORMAT = ICEBERG
  GLUE_AWS_ROLE_ARN = 'arn:aws:iam::790347819012:role/gitops-snowflake-iceberg-role-dev'
  GLUE_CATALOG_ID = '790347819012'
  GLUE_REGION = 'ap-south-1'
  ENABLED = TRUE;

-- ------------------------------------------------------------------------------
-- 2. Inspect Catalog Integration
-- ------------------------------------------------------------------------------
DESC CATALOG INTEGRATION glue_iceberg_catalog_int;

-- ------------------------------------------------------------------------------
-- 3. Grant Usage Privileges to SYSADMIN
-- ------------------------------------------------------------------------------
GRANT USAGE ON INTEGRATION glue_iceberg_catalog_int TO ROLE SYSADMIN;
