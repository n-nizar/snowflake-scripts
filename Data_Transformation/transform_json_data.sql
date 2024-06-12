-- Refer to Security_and_Access_Control/setup_transform_json_data.sql for the initial setup.
-- Use the dengineer role
USE ROLE dengineer;

-- Create schema for semi-structured data
CREATE SCHEMA IF NOT EXISTS data_transformation.semistructured;

-- Create an internal stage with directory enabled
CREATE STAGE IF NOT EXISTS data_transformation.semistructured.intstage_bulkload
  DIRECTORY = (ENABLE = TRUE);

-- Create a table to bulk load JSON data
CREATE TABLE IF NOT EXISTS data_transformation.semistructured.demo_sales_json (
    data VARIANT
);

-- Load local files to internal stage using SnowSQL
-- Uncomment and modify the file path according to your local setup
/*
PUT file:///Users/helloworld/Documents/Workspace/Development/GitHub/nn-snowflake/snowflake-scripts/Data_Transformation/sample_data/sample_json_data_sales.json
@data_transformation.semistructured.intstage_bulkload/json/sample_data;
*/

-- Load files from an internal stage to a Snowflake table
COPY INTO data_transformation.semistructured.demo_sales_json
FROM @data_transformation.semistructured.intstage_bulkload/json/sample_data/sample_json_data_sales.json
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'CONTINUE';

-- Remove files from the stage after loading
REMOVE @data_transformation.semistructured.intstage_bulkload/json/sample_data/sample_json_data_sales.json;

-- Confirm that the files have been removed
LIST @data_transformation.semistructured.intstage_bulkload/json/sample_data/;

-- Create views for transformed sales data
CREATE VIEW IF NOT EXISTS data_transformation.semistructured.vw_demo_sales_customers AS
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
WHERE EXTRACT(YEAR, c.value:created_at::TIMESTAMPTZ) = 2023
  AND EXTRACT(MONTH, c.value:created_at::TIMESTAMPTZ) = 1;

CREATE VIEW IF NOT EXISTS data_transformation.semistructured.vw_demo_sales_orders AS
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

CREATE VIEW IF NOT EXISTS data_transformation.semistructured.vw_demo_sales_products AS
SELECT  p.value:product_id::INTEGER                     AS product_id,
        p.value:name::STRING                            AS name,
        p.value:description::STRING                     AS description,
        p.value:price::FLOAT                            AS price,
        p.value:stock::INTEGER                          AS stock,
        p.value:category::STRING                        AS category
FROM data_transformation.semistructured.demo_sales_json AS s,
LATERAL FLATTEN (input => s.data:products) AS p;

-- Create a table to bulk load JSON data for library
CREATE TABLE IF NOT EXISTS data_transformation.semistructured.demo_library_json (
    data VARIANT
);

-- Load local files to internal stage using SnowSQL
-- Uncomment and modify the file path according to your local setup
/*
PUT file:///Users/helloworld/Documents/Workspace/Development/GitHub/nn-snowflake/snowflake-scripts/Data_Transformation/sample_data/sample_json_data_library.json 
@data_transformation.semistructured.intstage_bulkload/json/sample_data;
*/

-- Load files from an internal stage to a Snowflake table
COPY INTO data_transformation.semistructured.demo_library_json
FROM @data_transformation.semistructured.intstage_bulkload/json/sample_data/sample_json_data_library.json
FILE_FORMAT = (TYPE = 'JSON')
ON_ERROR = 'CONTINUE';

-- Remove files from the stage after loading
REMOVE @data_transformation.semistructured.intstage_bulkload/json/sample_data/sample_json_data_library.json;

-- Confirm that the files have been removed
LIST @data_transformation.semistructured.intstage_bulkload/json/sample_data/;

-- Create views for transformed library data
CREATE VIEW IF NOT EXISTS data_transformation.semistructured.vw_demo_library_books AS
SELECT  b.value:book_id::INTEGER                        AS book_id,
        b.value:title::STRING                           AS title,
        b.value:author_id::INTEGER                      AS author_id,
        b.value:genre::STRING                           AS genre,
        b.value:availability::STRING                    AS availability,
        b.value:isbn::INTEGER                           AS isbn,
        b.value:published_date::DATE                    AS published_date
FROM data_transformation.semistructured.demo_library_json AS lib,
LATERAL FLATTEN (input => lib.data:library_data) AS ld,
LATERAL FLATTEN (input => ld.value:data) AS b
WHERE ld.value:object::STRING = 'books';

CREATE VIEW IF NOT EXISTS data_transformation.semistructured.vw_demo_library_authors AS
SELECT  a.value:author_id::INTEGER                      AS author_id,
        a.value:name::STRING                            AS name,
        a.value:nationality::STRING                     AS nationality,
        a.value:birth_date::DATE                        AS birth_date,
        a.value:death_date::DATE                        AS death_date
FROM data_transformation.semistructured.demo_library_json AS lib,
LATERAL FLATTEN (input => lib.data:library_data) AS ld,
LATERAL FLATTEN (input => ld.value:data) AS a
WHERE ld.value:object::STRING = 'authors'
  AND a.value:nationality::STRING = 'American';

CREATE VIEW IF NOT EXISTS data_transformation.semistructured.vw_demo_library_members AS
SELECT  m.value:member_id::INTEGER                      AS member_id,
        m.value:name::STRING                            AS name,
        m.value:email::STRING                           AS email,
        m.value:phone::STRING                           AS phone,
        m.value:address::STRING                         AS address,
        m.value:membership_start_date::DATE             AS membership_start_date
FROM data_transformation.semistructured.demo_library_json AS lib,
LATERAL FLATTEN (input => lib.data:library_data) AS ld,
LATERAL FLATTEN (input => ld.value:data) AS m
WHERE ld.value:object = 'members';