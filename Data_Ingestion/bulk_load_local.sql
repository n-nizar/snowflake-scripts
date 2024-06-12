-- Refer to Security_and_Access_Control/setup_bulk_load_local.sql for the initial setup.
-- Use warehouse and bulk load role
USE ROLE bulk_load_int;
USE WAREHOUSE load_wh;

-- Create and manage internal stage
CREATE STAGE IF NOT EXISTS data_ingestion.bulk_load.intstage_bulkload;
DESCRIBE STAGE data_ingestion.bulk_load.intstage_bulkload;

-- Enable directory listing and refresh stage
ALTER STAGE data_ingestion.bulk_load.intstage_bulkload SET directory = (enable = TRUE);
ALTER STAGE data_ingestion.bulk_load.intstage_bulkload REFRESH;

-- Define file format for CSV
CREATE FILE FORMAT IF NOT EXISTS data_ingestion.bulk_load.csv_int
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
FILE_FORMAT = (FORMAT_NAME = data_ingestion.bulk_load.csv_int)
PATTERN = '.*csv.*'
ON_ERROR = 'skip_file';

-- Remove files from internal stage after loading
REMOVE @data_ingestion.bulk_load.intstage_bulkload/csv/sample_date/;

-- Confirm removal of files from internal stage
LIST @data_ingestion.bulk_load.intstage_bulkload;