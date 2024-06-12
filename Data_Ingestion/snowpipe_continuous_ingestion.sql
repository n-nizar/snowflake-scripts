-- Step 1: Initial setup using ACCOUNTADMIN role
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

-- Step 2: Use more specific roles for regular operations
-- Use the warehouse and snowpipe role
USE ROLE snowpipe_s3;
USE WAREHOUSE load_wh;

-- Create schema for snowpipe
CREATE SCHEMA IF NOT EXISTS data_ingestion.snowpipe;

-- Create an external stage using the storage integration
-- Replace S3 URL with your specific bucket info.
CREATE STAGE IF NOT EXISTS data_ingestion.snowpipe.S3stage_snowpipe
  URL = 's3://nnizar-snowflake-stage/snowpipe/'
  STORAGE_INTEGRATION = S3_role_integration_snowpipe;

-- Describe the stage to verify its creation
DESCRIBE STAGE data_ingestion.snowpipe.S3stage_snowpipe;

-- Create a table to load data from Snowpipe
CREATE TABLE IF NOT EXISTS data_ingestion.snowpipe.demo_sales (
    ProductID NUMBER,
    Date DATE,
    CustomerID NUMBER,
    CampaignID NUMBER,
    Units NUMBER,
    Product VARCHAR,
    Category VARCHAR,
    Segment VARCHAR,
    ManufacturerID NUMBER,
    Manufacturer VARCHAR,
    "Unit Cost" FLOAT,
    "Unit Price" FLOAT,
    ZipCode NUMBER,
    "Email Name" VARCHAR,
    City VARCHAR,
    State VARCHAR,
    Region VARCHAR,
    District VARCHAR,
    Country VARCHAR
);

-- Define file format for CSV
CREATE FILE FORMAT IF NOT EXISTS data_ingestion.snowpipe.csv 
  TYPE = 'csv' 
  COMPRESSION = 'auto'
  FIELD_DELIMITER = ',' 
  RECORD_DELIMITER = '\n' 
  SKIP_HEADER = 1 
  FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
  TRIM_SPACE = FALSE 
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE 
  ESCAPE = 'none' 
  ESCAPE_UNENCLOSED_FIELD = '\134' 
  DATE_FORMAT = 'auto' 
  TIMESTAMP_FORMAT = 'auto' 
  NULL_IF = ('') 
  COMMENT = 'file format for ingesting csv file to Snowflake';

-- Create Snowpipe for automated data loading
CREATE PIPE IF NOT EXISTS data_ingestion.snowpipe.demo_S3_pipe AUTO_INGEST = TRUE AS
  COPY INTO data_ingestion.snowpipe.demo_sales
  FROM @data_ingestion.snowpipe.S3stage_snowpipe/demo/
  FILE_FORMAT = (FORMAT_NAME = data_ingestion.snowpipe.csv)
  PATTERN = '.*csv.*';

-- Show pipes to verify creation
SHOW PIPES;

-- Describe the pipe to get SQS ARN for event notification setup
DESCRIBE PIPE data_ingestion.snowpipe.demo_S3_pipe;

-- Example commands to alter or drop the pipe
-- Pause the pipe
ALTER PIPE data_ingestion.snowpipe.demo_S3_pipe SET PIPE_EXECUTION_PAUSED = TRUE;

-- Drop the pipe
DROP PIPE data_ingestion.snowpipe.demo_S3_pipe;
