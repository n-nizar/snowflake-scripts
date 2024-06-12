-- Refer to Security_and_Access_Control/bulkload_s3.sql for the initial setup.
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
CREATE FILE FORMAT IF NOT EXISTS data_ingestion.bulk_load.csv_s3
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
FILE_FORMAT = (FORMAT_NAME = data_ingestion.bulk_load.csv_s3)
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
FILE_FORMAT = (FORMAT_NAME = data_ingestion.bulk_load.csv_s3)
PATTERN = '.*csv.*'
ON_ERROR = 'skip_file';