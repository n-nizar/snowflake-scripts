-- Step 1: Initial setup with ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Create warehouse and database
CREATE WAREHOUSE IF NOT EXISTS load_wh
WITH warehouse_size = 'x-small';

CREATE DATABASE IF NOT EXISTS data_ingestion;

-- Create roles
-- Optional: the "nnsuperadmin" role is used to manage all of my key data projects.
CREATE ROLE IF NOT EXISTS nnsuperadmin;
CREATE ROLE IF NOT EXISTS bulk_load_s3;
CREATE ROLE IF NOT EXISTS bulk_load_int;

-- Grant roles to user
-- Replace "nnizar" with your specific Snowflake username.
GRANT ROLE nnsuperadmin TO USER nnizar;
GRANT ROLE bulk_load_s3 TO USER nnizar;
GRANT ROLE bulk_load_int TO USER nnizar;

-- Grant roles to nnsuperadmin role
GRANT ROLE bulk_load_s3 TO ROLE nnsuperadmin;
GRANT ROLE bulk_load_int TO ROLE nnsuperadmin;

-- Grant permissions to roles
GRANT USAGE ON WAREHOUSE load_wh TO ROLE bulk_load_s3;
GRANT USAGE, CREATE SCHEMA ON DATABASE data_ingestion TO ROLE bulk_load_s3;
GRANT USAGE ON WAREHOUSE load_wh TO ROLE bulk_load_int;
GRANT USAGE, CREATE SCHEMA ON DATABASE data_ingestion TO ROLE bulk_load_int;

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

-- Step 2: Use more specific roles for regular operations
-- Use warehouse and bulk load role
USE ROLE bulk_load_s3;
USE WAREHOUSE load_wh;

-- Create schema for bulk load
CREATE SCHEMA IF NOT EXISTS data_ingestion.bulk_load;

-- Create an external stage using the storage integration
-- Replace S3 URL with your specific bucket info.
CREATE STAGE IF NOT EXISTS data_ingestion.bulk_load.S3stage_bulkload
  URL = 's3://nnizar-snowflake-stage/bulk-loading/'
  STORAGE_INTEGRATION = S3_role_integration_bulkloading;

DESCRIBE STAGE data_ingestion.bulk_load.S3stage_bulkload;

-- Define file format for CSV
CREATE FILE FORMAT IF NOT EXISTS data_ingestion.bulk_load.csv 
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

GRANT USAGE ON FILE FORMAT data_ingestion.bulk_load.csv TO ROLE bulk_load_int;

-- Create and load tables
CREATE TABLE IF NOT EXISTS data_ingestion.bulk_load.sales_s3 (
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

COPY INTO data_ingestion.bulk_load.sales_s3
FROM @data_ingestion.bulk_load.S3stage_bulkload/demo/
FILE_FORMAT = (FORMAT_NAME = data_ingestion.bulk_load.csv)
PATTERN = '.*csv.*'
ON_ERROR = 'skip_file';

-- Create and load a simplified table
CREATE TABLE IF NOT EXISTS data_ingestion.bulk_load.sales_s3_v2 (
    ProductID NUMBER,
    CampaignID NUMBER,
    ManufacturerID NUMBER
);

COPY INTO data_ingestion.bulk_load.sales_s3_v2 (ProductID, CampaignID, ManufacturerID)
FROM (SELECT DISTINCT t.$1, t.$4, t.$9 FROM @data_ingestion.bulk_load.S3stage_bulkload/demo/ t)
FILE_FORMAT = (FORMAT_NAME = data_ingestion.bulk_load.csv)
PATTERN = '.*csv.*'
ON_ERROR = 'skip_file';

-- Switch to internal bulk load role
USE ROLE bulk_load_int;

-- Create and manage internal stage
CREATE STAGE IF NOT EXISTS data_ingestion.bulk_load.intstage_bulkload;
DESCRIBE STAGE data_ingestion.bulk_load.intstage_bulkload;

-- Enable directory listing and refresh stage
ALTER STAGE data_ingestion.bulk_load.intstage_bulkload SET directory = (enable = TRUE);
ALTER STAGE data_ingestion.bulk_load.intstage_bulkload REFRESH;

-- Create table for internal bulk load
CREATE TABLE IF NOT EXISTS data_ingestion.bulk_load.sales_int (
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

-- Show grants on internal stage to verify permissions
SHOW GRANTS ON STAGE data_ingestion.bulk_load.intstage_bulkload;

-- Grant ownership of internal stage to internal bulk load role
GRANT OWNERSHIP ON STAGE data_ingestion.bulk_load.intstage_bulkload TO ROLE bulk_load_int;

-- Load local files to internal stage using SnowSQL
-- Uncomment and modify the file path according to your local setup
-- PUT file:///Users/helloworld/Downloads/Downloads-Workspace/Data/ @data_ingestion.bulk_load.intstage_bulkload/csv/sample_date/;

-- Load files from internal stage to Snowflake table
COPY INTO data_ingestion.bulk_load.sales_int
FROM @data_ingestion.bulk_load.intstage_bulkload
FILE_FORMAT = (FORMAT_NAME = data_ingestion.bulk_load.csv)
PATTERN = '.*csv.*'
ON_ERROR = 'skip_file';

-- Remove files from internal stage after loading
REMOVE @data_ingestion.bulk_load.intstage_bulkload/csv/sample_date/;

-- Confirm removal of files from internal stage
LIST @data_ingestion.bulk_load.intstage_bulkload;