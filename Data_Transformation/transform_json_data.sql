USE ROLE accountadmin;

CREATE WAREHOUSE IF NOT EXISTS transform_wh WITH warehouse_size='x-small';
CREATE DATABASE IF NOT EXISTS data_transformation;

-- Replace "nnizar" with your specific Snowflake username.
-- Optional: the "nnsuperadmin" role is used to manage all of my key data projects.
CREATE ROLE IF NOT EXISTS nnsuperadmin;
GRANT ROLE nnsuperadmin TO USER nnizar;
CREATE ROLE IF NOT EXISTS dengineer;
GRANT ROLE dengineer TO ROLE nnsuperadmin;
GRANT ROLE dengineer TO USER nnizar;

GRANT ROLE dengineer TO USER nnizar;
GRANT ROLE dengineer TO ROLE nnsuperadmin;

GRANT USAGE ON WAREHOUSE transform_wh TO ROLE dengineer;
GRANT USAGE, CREATE SCHEMA ON DATABASE data_transformation TO ROLE dengineer;

USE ROLE dengineer;

CREATE SCHEMA IF NOT EXISTS data_transformation.semistructured;

-- Create an internal stage
CREATE STAGE IF NOT EXISTS data_transformation.semistructured.intstage_bulkload
directory = (enable = TRUE);

-- Create "demo_sales_json" table to bulk load data from a file in a stage
CREATE TABLE IF NOT EXISTS data_transformation.semistructured.demo_sales_json (
    data VARIANT);

-- Load local files to an internal stage using SnowSQL. Modify the file path according to where you have saved the files.
/*
PUT file:///Users/helloworld/Documents/Workspace/Development/GitHub/nn-snowflake/snowflake-scripts/Data_Transformation/sample_data/sample_json_data_sales.json
@data_transformation.semistructured.intstage_bulkload/json/sample_data;
*/

-- Load files from an internal stage to a Snowflake table.
COPY INTO data_transformation.semistructured.demo_sales_json
FROM @data_transformation.semistructured.intstage_bulkload/json/sample_data/sample_json_data_sales.json
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'CONTINUE';

-- After loading data from stage to Snowflake table, remove it from the Stage
REMOVE @data_transformation.semistructured.intstage_bulkload/json/sample_data/sample_json_data_sales.json;

-- Confirm that the file has been removed
LIST @data_transformation.semistructured.intstage_bulkload/json/sample_data/;

CREATE VIEW IF NOT EXISTS data_transformation.semistructured.vw_demo_sales_customers
AS
SELECT  c.value:customer_id::INTEGER                    AS cust_id,
        c.value:name::STRING                            AS name,
        c.value:email::STRING                           AS email,
        c.value:phone::STRING                           AS phone,
        c.value:address:street::STRING                  AS street,
        c.value:address:city::STRING                    AS city,
        c.value:address:state::STRING                   AS state,
        c.value:address:zip_code::INTEGER               AS zipcode,
        c.value:created_at::TIMESTAMPTZ                 AS created_at
FROM data_transformation.semistructured.demo_sales_json AS s,
LATERAL FLATTEN (input => s.data:customers) AS c
WHERE EXTRACT("Year", c.value:created_at::TIMESTAMPTZ) = '2023'
AND EXTRACT("Month", c.value:created_at::TIMESTAMPTZ) = '01';

CREATE VIEW IF NOT EXISTS data_transformation.semistructured.vw_demo_sales_orders
AS
SELECT  o.value:order_id::INTEGER                       AS order_id,
        o.value:order_date::TIMESTAMPTZ                 AS order_date,
        o.value:customer_id::INTEGER                    AS customer_id,
        op.value:product_id::INTEGER                    AS product_id,
        op.value:price::FLOAT                           AS price,
        op.value:quantity::INTEGER                      AS quantity,
        o.value:status::STRING                          AS status,
        o.value:total_amount::FLOAT                     AS total_amount
FROM data_transformation.semistructured.demo_sales_json AS s,
LATERAL FLATTEN (input => s.data:orders) AS o,
LATERAL FLATTEN (input => o.value:products) AS op;

SELECT  p.value:product_id::INTEGER                     AS product_id,
        p.value:name::STRING                            AS name,
        p.value:description::STRING                     AS description,
        p.value:price::FLOAT                            AS price,
        p.value:stock::INTEGER                          AS stock,
        p.value:category::STRING                        AS category
FROM data_transformation.semistructured.demo_sales_json AS s,
LATERAL FLATTEN (input => s.data:products) AS p;

-- Create "demo_library_json" table to bulk load data from a file in a stage
CREATE TABLE IF NOT EXISTS data_transformation.semistructured.demo_library_json (
    data VARIANT);

-- Load local files to an internal stage using SnowSQL. Modify the file path according to where you have saved the files.
/*
PUT file:///Users/helloworld/Documents/Workspace/Development/GitHub/nn-snowflake/snowflake-scripts/Data_Transformation/sample_data/sample_json_data_library.json 
@data_transformation.semistructured.intstage_bulkload/json/sample_data;
*/

-- Load files from an internal stage to a Snowflake table using SnowSQL
COPY INTO data_transformation.semistructured.demo_library_json
FROM @data_transformation.semistructured.intstage_bulkload/json/sample_data/sample_json_data_library.json
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'CONTINUE';

-- After loading data from stage to Snowflake table, remove it from the Stage
REMOVE @data_transformation.semistructured.intstage_bulkload/json/sample_data/sample_json_data_library.json;

-- Confirm that the file has been removed
LIST @data_transformation.semistructured.intstage_bulkload/json/sample_data/;

CREATE VIEW IF NOT EXISTS data_transformation.semistructured.vw_demo_library_books
AS
SELECT  b.value:book_id::INTEGER                        AS book_id,
        b.value:title::STRING                           AS title,
        b.value:author_id::INTEGER                      AS author_id,
        b.value:genre::STRING                           AS genre,
        b.value:availability::STRING                    AS availability,
        b.value:isbn::INTEGER                           AS isbn,
        b.value:published_date::DATE                    AS published_date
FROM data_transformation.semistructured.demo_library_json lib,
LATERAL FLATTEN (input => lib.data:library_data) as ld,
LATERAL FLATTEN (input => ld.value:data) as b
WHERE (ld.value:object::STRING) = 'books';

CREATE VIEW IF NOT EXISTS data_transformation.semistructured.vw_demo_library_authors
AS
SELECT  a.value:author_id::INTEGER                      AS author_id,
        a.value:name::STRING                            AS name,
        a.value:nationality::STRING                     AS nationality,
        a.value:birth_date::DATE                        AS birth_date,
        a.value:death_date::DATE                        AS death_date
FROM data_transformation.semistructured.demo_library_json lib,
LATERAL FLATTEN (input => lib.data:library_data) as ld,
LATERAL FLATTEN (input => ld.value:data) as a
WHERE ld.value:object::STRING = 'authors'
AND a.value:nationality::STRING = 'American';

CREATE VIEW IF NOT EXISTS data_transformation.semistructured.vw_demo_library_members
AS
SELECT  m.value:member_id::INTEGER                      AS member_id,
        m.value:name::STRING                            AS name,
        m.value:email::STRING                           AS email,
        m.value:phone::STRING                           AS phone,
        m.value:address::STRING                         AS address,
        m.value:membership_start_date::DATE AS membership_start_date
FROM data_transformation.semistructured.demo_library_json lib,
LATERAL FLATTEN (lib.data:library_data) AS ld,
LATERAL FLATTEN (ld.value:data) AS m
WHERE ld.value:object = 'members';