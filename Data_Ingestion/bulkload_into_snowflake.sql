USE ROLE accountadmin;

CREATE WAREHOUSE IF NOT EXISTS load_wh WITH warehouse_size='x-small';
CREATE DATABASE IF NOT EXISTS data_ingestion;

-- Replace "nnizar" with your specific Snowflake username.
-- Optional: the "nnsuperadmin" role is used to manage all of my key data projects.
CREATE ROLE IF NOT EXISTS nnsuperadmin;
GRANT ROLE nnsuperadmin TO USER nnizar;
CREATE ROLE IF NOT EXISTS bulk_load_s3;
CREATE ROLE IF NOT EXISTS bulk_load_int;
GRANT ROLE bulk_load_s3 TO ROLE nnsuperadmin;
GRANT ROLE bulk_load_s3 TO ROLE nnsuperadmin;

GRANT ROLE bulk_load_s3 TO USER nnizar;
GRANT ROLE bulk_load_int TO USER nnizar;

GRANT USAGE ON WAREHOUSE load_wh TO ROLE bulk_load_s3;
GRANT USAGE, CREATE SCHEMA ON DATABASE data_ingestion TO ROLE bulk_load_s3;
GRANT USAGE ON WAREHOUSE load_wh TO ROLE bulk_load_int;
GRANT USAGE, CREATE SCHEMA ON DATABASE data_ingestion TO ROLE bulk_load_int;

-- Integrate IAM user with Snowflake storage. Replace "storeage_aws_role_arn" with the ARN of the role that has access to the S3 bucket.
CREATE STORAGE INTEGRATION IF NOT EXISTS S3_role_integration_bulkloading
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = "arn:aws:iam::<aws_account_number>:role/snowflake_bulkloading_access"
  STORAGE_ALLOWED_LOCATIONS = ("s3://nnizar-snowflake-stage/bulk-loading/");

-- Update the IAM role trust relationship with values: STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
DESCRIBE INTEGRATION S3_role_integration_bulkloading;

GRANT USAGE ON INTEGRATION S3_role_integration_bulkloading TO ROLE bulk_load_s3;

USE WAREHOUSE load_wh;
USE ROLE bulk_load_s3;

CREATE SCHEMA  IF NOT EXISTS data_ingestion.bulk_load;

-- Create an external stage using the storage integration object created during the previous step.
-- Replace S3 URL with your specific bucket info.
CREATE STAGE IF NOT EXISTS data_ingestion.bulk_load.S3stage_bulkload 
URL='s3://nnizar-snowflake-stage/bulk-loading/'
storage_integration = S3_role_integration_bulkloading;

DESCRIBE STAGE data_ingestion.bulk_load.S3stage_bulkload;

CREATE FILE FORMAT IF NOT EXISTS data_ingestion.bulk_load.csv type='csv' compression = 'auto' field_delimiter = ',' record_delimiter = '\n' skip_header = 1 field_optionally_enclosed_by = '\042' trim_space = false error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134' date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting csv file to snowflake';

GRANT USAGE ON FILE FORMAT DATA_INGESTION.BULK_LOAD.CSV TO ROLE bulk_load_int;

-- Bulk load All columns from a file in a stage
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
FILE_FORMAT=ma_db_raw.source.csv PATTERN = '.*csv.*'
ON_ERROR = 'skip_file';

-- Bulk load selected columns from a file in a stage
CREATE TABLE IF NOT EXISTS data_ingestion.bulk_load.sales_s3_v2 (
    ProductID NUMBER,
    CampaignID NUMBER,
    ManufacturerID NUMBER
);

COPY INTO data_ingestion.bulk_load.sales_s3_v2(ProductID, CampaignID, ManufacturerID)
FROM (SELECT DISTINCT t.$1, t.$4, t.$9 FROM @data_ingestion.bulk_load.S3stage_bulkload/demo/ t)
FILE_FORMAT=ma_db_raw.source.csv PATTERN = '.*csv.*'
ON_ERROR = 'skip_file';

USE ROLE bulk_load_int;

-- Create an internal stage
CREATE STAGE IF NOT EXISTS data_ingestion.bulk_load.intstage_bulkload;

DESCRIBE STAGE data_ingestion.bulk_load.intstage_bulkload;

ALTER STAGE
data_ingestion.bulk_load.intstage_bulkload
SET directory = (enable = TRUE);

ALTER STAGE
data_ingestion.bulk_load.intstage_bulkload refresh;

-- Bulk load All columns from a file in a stage
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

SHOW GRANTS ON STAGE DATA_INGESTION.BULK_LOAD.INTSTAGE_BULKLOAD;

GRANT OWNERSHIP ON STAGE data_ingestion.bulk_load.intstage_bulkload TO ROLE bulk_load_int;

-- Load local files to an internal stage using SnowSQL. Modify the file path according to where you have saved the files.
/*
PUT file:///Users/helloworld/Downloads/Downloads-Workspace/Data/ @data_ingestion.bulk_load.intstage_bulkload/csv/sample_date/;
*/

-- Load files from an internal stage to a Snowflake table.
COPY INTO data_ingestion.bulk_load.sales_int
FROM @data_ingestion.bulk_load.intstage_bulkload
FILE_FORMAT=DATA_INGESTION.BULK_LOAD.CSV PATTERN = '.*csv.*'
ON_ERROR = 'skip_file';

-- After loading data from stage to Snowflake table, remove it from the Stage
REMOVE @data_ingestion.bulk_load.intstage_bulkload/csv/sample_date/;

-- Confirm that the file has been removed
LIST @data_ingestion.bulk_load.intstage_bulkload;