-- Initial setup using ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Create warehouse and database if they don't exist
CREATE WAREHOUSE IF NOT EXISTS load_wh WITH warehouse_size = 'x-small';
CREATE DATABASE IF NOT EXISTS data_ingestion;

-- Create roles and grant them to the user
-- Optional: the "nnsuperadmin" role is used to manage all of my key data projects.
-- Replace "nnizar" with your specific Snowflake username.
CREATE ROLE IF NOT EXISTS nnsuperadmin;
CREATE ROLE IF NOT EXISTS snowpipe_s3;

GRANT ROLE nnsuperadmin TO USER nnizar;
GRANT ROLE snowpipe_s3 TO ROLE nnsuperadmin;
GRANT ROLE snowpipe_s3 TO USER nnizar;

-- Grant necessary permissions to roles
GRANT USAGE ON WAREHOUSE load_wh TO ROLE snowpipe_s3;
GRANT USAGE, CREATE SCHEMA ON DATABASE data_ingestion TO ROLE snowpipe_s3;

-- Set up storage integration for S3
-- Replace "STORAGE_AWS_ROLE_ARN" with the ARN of the role that has access to the S3 bucket.
CREATE STORAGE INTEGRATION IF NOT EXISTS S3_role_integration_snowpipe
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<aws_account_number>:role/snowpipe_access'
  STORAGE_ALLOWED_LOCATIONS = ('s3://nnizar-snowflake-stage/snowpipe/');

-- Update the IAM role trust relationship with values: STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
DESCRIBE INTEGRATION S3_role_integration_snowpipe;

-- Grant usage on the integration to the snowpipe role
GRANT USAGE ON INTEGRATION S3_role_integration_snowpipe TO ROLE snowpipe_s3;