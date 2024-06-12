-- Initial setup with ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Create warehouse and database
CREATE WAREHOUSE IF NOT EXISTS load_wh
WITH warehouse_size = 'x-small';

CREATE DATABASE IF NOT EXISTS data_ingestion;

-- Create roles
-- Optional: the "nnsuperadmin" role is used to manage all of my key data projects.
CREATE ROLE IF NOT EXISTS nnsuperadmin;
CREATE ROLE IF NOT EXISTS bulk_load_int;

-- Grant roles to user
-- Replace "nnizar" with your specific Snowflake username.
GRANT ROLE nnsuperadmin TO USER nnizar;
GRANT ROLE bulk_load_int TO USER nnizar;

-- Grant roles to nnsuperadmin role
GRANT ROLE bulk_load_int TO ROLE nnsuperadmin;

-- Grant permissions to roles
GRANT USAGE ON WAREHOUSE load_wh TO ROLE bulk_load_int;
GRANT USAGE, CREATE SCHEMA ON DATABASE data_ingestion TO ROLE bulk_load_int;