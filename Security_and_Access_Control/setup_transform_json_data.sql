-- Initial setup using ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Create warehouse and database if they don't exist
CREATE WAREHOUSE IF NOT EXISTS transform_wh WITH warehouse_size = 'x-small';
CREATE DATABASE IF NOT EXISTS data_transformation;

-- Create roles and grant them to the user
-- Replace "nnizar" with your specific Snowflake username.
-- Optional: the "nnsuperadmin" role is used to manage all of my key data projects.
CREATE ROLE IF NOT EXISTS nnsuperadmin;
GRANT ROLE nnsuperadmin TO USER nnizar;

CREATE ROLE IF NOT EXISTS dengineer;
GRANT ROLE dengineer TO ROLE nnsuperadmin;
GRANT ROLE dengineer TO USER nnizar;

-- Grant necessary permissions to roles
GRANT USAGE ON WAREHOUSE transform_wh TO ROLE dengineer;
GRANT USAGE, CREATE SCHEMA ON DATABASE data_transformation TO ROLE dengineer;