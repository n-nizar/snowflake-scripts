-- Initial setup with ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Create warehouse and database
CREATE WAREHOUSE IF NOT EXISTS load_wh
WITH warehouse_size = 'x-small';

CREATE DATABASE IF NOT EXISTS data_ingestion;

-- Create roles
-- Optional: the "nnsuperadmin" role is used to manage all of my key data projects.
CREATE ROLE IF NOT EXISTS nnsuperadmin;
CREATE ROLE IF NOT EXISTS bulk_load_s3;

-- Grant roles to user
-- Replace "nnizar" with your specific Snowflake username.
GRANT ROLE nnsuperadmin TO USER nnizar;
GRANT ROLE bulk_load_s3 TO USER nnizar;

-- Grant roles to nnsuperadmin role
GRANT ROLE bulk_load_s3 TO ROLE nnsuperadmin;
GRANT ROLE bulk_load_int TO ROLE nnsuperadmin;

-- Grant permissions to roles
GRANT USAGE ON WAREHOUSE load_wh TO ROLE bulk_load_s3;
GRANT USAGE, CREATE SCHEMA ON DATABASE data_ingestion TO ROLE bulk_load_s3;

-- Set up storage integration for S3
-- Replace "STORAGE_AWS_ROLE_ARN" with the ARN of the role that has access to the S3 bucket.
CREATE STORAGE INTEGRATION IF NOT EXISTS S3_role_integration_bulkloading
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<aws_account_number>:role/snowflake_bulkloading_access'
  STORAGE_ALLOWED_LOCATIONS = ('s3://nnizar-snowflake-stage/bulk-loading/');

-- Update the IAM role trust relationship with values: STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
DESCRIBE INTEGRATION S3_role_integration_bulkloading;

GRANT USAGE ON INTEGRATION S3_role_integration_bulkloading TO ROLE bulk_load_s3;