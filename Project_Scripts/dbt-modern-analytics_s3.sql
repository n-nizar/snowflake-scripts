USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS ma_wh WITH warehouse_size='x-small';
CREATE DATABASE IF NOT EXISTS ma_db_raw;
CREATE DATABASE IF NOT EXISTS ma_db_dev;
CREATE DATABASE IF NOT EXISTS ma_db_prod;

-- Replace "nnizar" with your specific Snowflake username.
-- Optional: the "nnsuperadmin" role is used to manage all of my key data projects.
CREATE ROLE IF NOT EXISTS nnsuperadmin;
GRANT ROLE nnsuperadmin TO USER nnizar;
CREATE ROLE IF NOT EXISTS ma_dbt_admin;
GRANT ROLE ma_dbt_admin TO USER nnizar;
GRANT ROLE ma_dbt_admin TO ROLE nnsuperadmin;

CREATE ROLE IF NOT EXISTS ma_dbt_dev;
GRANT ROLE ma_dbt_dev TO ROLE nnsuperadmin;
GRANT ROLE ma_dbt_dev TO ROLE ma_dbt_admin;
GRANT ROLE ma_dbt_dev TO USER nnizar;

GRANT USAGE ON WAREHOUSE ma_wh TO ROLE ma_dbt_dev;
GRANT USAGE, CREATE SCHEMA ON DATABASE ma_db_raw TO ROLE ma_dbt_dev;
GRANT USAGE, CREATE SCHEMA ON DATABASE ma_db_dev TO ROLE ma_dbt_dev;
GRANT USAGE, CREATE SCHEMA ON DATABASE ma_db_prod TO ROLE ma_dbt_admin;

GRANT USAGE ON INTEGRATION S3_role_integration_bulkloading TO ROLE ma_dbt_dev;

USE ROLE ma_dbt_admin;

-- Create the dbt_audit schema for production environment
CREATE SCHEMA IF NOT EXISTS ma_db_prod.dbt_audit;

-- Create the dbt_run_audit table to store run details
CREATE TABLE IF NOT EXISTS ma_db_prod.dbt_audit.dbt_run (
    invocation_id STRING,
    object_type STRING,
    object STRING,
    status STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    PRIMARY KEY (invocation_id, object)
);

USE ROLE ma_dbt_dev;
USE WAREHOUSE ma_wh;

-- Create the dbt_audit schema for development environment.
CREATE SCHEMA IF NOT EXISTS ma_db_dev.dbt_audit;

-- Create the dbt_run_audit table to store run details
CREATE TABLE IF NOT EXISTS ma_db_dev.dbt_audit.dbt_run (
    invocation_id STRING,
    object_type STRING,
    object STRING,
    status STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    PRIMARY KEY (invocation_id, object)
);

CREATE SCHEMA IF NOT EXISTS ma_db_raw.source;

-- Create an external stage. Ensure that the storage integration has the privilege to access the S3 bucket.
-- Replace S3 URL with your specific bucket info.
CREATE STAGE IF NOT EXISTS ma_db_raw.source.S3_stage
URL='s3://nnizar-snowflake-stage/bulk-loading/'
storage_integration = S3_role_integration_bulkloading;

CREATE FILE FORMAT IF NOT EXISTS ma_db_raw.source.csv type='csv' compression = 'auto' field_delimiter = ',' record_delimiter = '\n' skip_header = 1 field_optionally_enclosed_by = '\042' trim_space = false error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134' date_format = 'auto' timestamp_format = 'auto' null_if = ('') comment = 'file format for ingesting csv file to snowflake';

-- Bulk load All columns from a file in a stage
CREATE TABLE IF NOT EXISTS ma_db_raw.source.sales (
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
    Country VARCHAR,
    LoadDate DATE
);

COPY INTO ma_db_raw.source.sales
FROM @ma_db_raw.source.S3_stage/modern-analytics/
FILE_FORMAT=ma_db_raw.source.csv PATTERN = '.*csv.*'
ON_ERROR = 'skip_file';