USE ROLE accountadmin;

CREATE WAREHOUSE IF NOT EXISTS load_wh WITH warehouse_size='x-small';
CREATE DATABASE IF NOT EXISTS data_ingestion;

-- Replace "nnizar" with your specific Snowflake username.
-- Optional: the "nnsuperadmin" role is used to manage all of my key data projects.
CREATE ROLE IF NOT EXISTS nnsuperadmin;
GRANT ROLE nnsuperadmin TO USER nnizar;
CREATE ROLE IF NOT EXISTS snowpipe_s3;
GRANT ROLE snowpipe_s3 TO ROLE nnsuperadmin;
GRANT ROLE snowpipe_s3 TO USER nnizar;

GRANT USAGE ON WAREHOUSE load_wh TO ROLE snowpipe_s3;
GRANT USAGE, CREATE SCHEMA ON DATABASE data_ingestion TO ROLE snowpipe_s3;

-- Integrate IAM user with Snowflake storage. Replace "storeage_aws_role_arn" with the ARN of the role that has access to the S3 bucket.
CREATE STORAGE INTEGRATION IF NOT EXISTS S3_role_integration_snowpipe
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = "arn:aws:iam::<aws_account_number>:role/snowpipe_access"
  STORAGE_ALLOWED_LOCATIONS = ("s3://nnizar-snowflake-stage/snowpipe/");

-- Update the IAM role trust relationship with values: STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
DESCRIBE INTEGRATION S3_role_integration_snowpipe;

GRANT USAGE ON INTEGRATION S3_role_integration_snowpipe TO ROLE snowpipe_s3;

USE WAREHOUSE load_wh;
USE ROLE snowpipe_s3;

CREATE SCHEMA IF NOT EXISTS data_ingestion.snowpipe;

-- Create an external stage using the storage integration object created during the previous step.
-- Replace S3 URL with your specific bucket info.
CREATE STAGE IF NOT EXISTS data_ingestion.snowpipe.S3stage_snowpipe
URL='s3://nnizar-snowflake-stage/snowpipe/'
storage_integration = S3_role_integration_snowpipe;

DESCRIBE STAGE data_ingestion.snowpipe.S3stage_snowpipe;

-- Table to load data from Snowpipe
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

CREATE FILE FORMAT IF NOT EXISTS data_ingestion.snowpipe.csv type='csv' compression = 'auto' field_delimiter = ',' record_delimiter = '\n' skip_header = 1 field_optionally_enclosed_by = '\042' trim_space = false error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134' date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting csv file to snowflake';

CREATE PIPE IF NOT EXISTS data_ingestion.snowpipe.demo_S3_pipe auto_ingest=true as
  copy into data_ingestion.snowpipe.demo_sales
  from @data_ingestion.snowpipe.S3stage_snowpipe/demo/
  FILE_FORMAT=csv PATTERN = '.*csv.*';

SHOW PIPES;

-- Create a new event notification for the S3 bucket and provide the SQS ARN value (Notification Channel) from Snowpipe.
DESCRIBE PIPE data_ingestion.snowpipe.demo_S3_pipe;

-- Alter or Drop snowpipe
ALTER PIPE data_ingestion.snowpipe.demo_S3_pipe SET PIPE_EXECUTION_PAUSED = true;

DROP PIPE data_ingestion.snowpipe.demo_S3_pipe;