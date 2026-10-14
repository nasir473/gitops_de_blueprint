-- ==============================================================================
-- GitOps Data Platform: Snowflake External Volume for Apache Iceberg
-- File: snowflake/stages/01_external_volume.sql
-- Description: Establishes secure storage connection to the Silver S3 Iceberg warehouse.
-- Prerequisites:
--   1. Run with ACCOUNTADMIN role.
--   2. IAM Role deployed in AWS via terraform/environments/dev:
--      arn:aws:iam::<AWS_ACCOUNT_ID>:role/gitops-snowflake-iceberg-role-dev
-- ==============================================================================

USE ROLE ACCOUNTADMIN;

-- ------------------------------------------------------------------------------
-- 1. Create External Volume for Silver S3 Iceberg Warehouse
-- Note: Replace <AWS_ACCOUNT_ID> and <SILVER_BUCKET_NAME> with your environment values.
-- Default Bucket: gitops-silver-bkt-dev-790347819012
-- Default Role ARN: arn:aws:iam::790347819012:role/gitops-snowflake-iceberg-role-dev
-- ------------------------------------------------------------------------------
CREATE OR REPLACE EXTERNAL VOLUME ext_vol_silver_iceberg
   STORAGE_LOCATIONS =
      (
         (
            NAME = 's3-silver-iceberg-dev'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://gitops-silver-bkt-dev-790347819012/iceberg/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::790347819012:role/gitops-snowflake-iceberg-role-dev'
         )
      );

-- ------------------------------------------------------------------------------
-- 2. Inspect the External Volume to Retrieve Handshake Tokens
-- ------------------------------------------------------------------------------
DESC EXTERNAL VOLUME ext_vol_silver_iceberg;

/*
ACTION REQUIRED FOR AWS IAM TRUST HANDSHAKE:
-------------------------------------------
From the output table of `DESC EXTERNAL VOLUME ext_vol_silver_iceberg`:
1. Copy the value of property: STORAGE_AWS_IAM_USER_ARN (e.g. arn:aws:iam::123456789012:user/xyz)
2. Copy the value of property: STORAGE_AWS_EXTERNAL_ID  (e.g. ABC1234_SFCRole=...)

Provide these values to your Terraform Dev variables or update `terraform.tfvars`:
  snowflake_storage_aws_iam_user_arn = "<STORAGE_AWS_IAM_USER_ARN>"
  snowflake_storage_aws_external_id  = "<STORAGE_AWS_EXTERNAL_ID>"

Then run:
  terraform apply -target=module.snowflake_iam
*/

-- ------------------------------------------------------------------------------
-- 3. Grant Usage Privileges to SYSADMIN
-- ------------------------------------------------------------------------------
GRANT USAGE ON EXTERNAL VOLUME ext_vol_silver_iceberg TO ROLE SYSADMIN;
