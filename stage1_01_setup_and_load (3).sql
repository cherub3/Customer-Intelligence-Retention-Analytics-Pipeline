-- ============================================================
-- OLIST BRAZILIAN E-COMMERCE ANALYTICS
-- STAGE 1 | FILE 1: DATABASE SETUP & RAW DATA LOADING
-- Environment : MySQL 8+
-- Encoding    : UTF-8
-- ============================================================
-- BEFORE RUNNING:
--   1. Copy all CSV files to the MySQL secure directory.
--      Check your path: SHOW VARIABLES LIKE 'secure_file_priv';
--      Default on Linux : /var/lib/mysql-files/
--      Default on Windows: C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/
--   2. Place all 9 CSV files inside a sub-folder named  olist/
--      e.g. /var/lib/mysql-files/olist/olist_customers_dataset.csv
--   3. Update the path literals in Section 3 if your path differs.
--   4. Run as a MySQL user with FILE privilege.
--      GRANT FILE ON *.* TO 'your_user'@'localhost';
-- ============================================================

-- ============================================================
-- SECTION 0: GLOBAL SETTINGS
-- ============================================================
-- DuckDB: no server/session settings required.

-- ============================================================
-- SECTION 1: DATABASE CREATION
-- ============================================================
-- DuckDB: database is managed by the execution script (no CREATE DATABASE/USE).

-- ============================================================
-- SECTION 2: RAW STAGING TABLE DEFINITIONS
--   All original columns preserved. No transformation here.
-- ============================================================

-- 2.1  CUSTOMERS -------------------------------------------------
DROP TABLE IF EXISTS raw_olist_customers_dataset;
CREATE TABLE raw_olist_customers_dataset (
    customer_id              VARCHAR(50),
    customer_unique_id       VARCHAR(50),
    customer_zip_code_prefix INT,
    customer_city            VARCHAR(120),
    customer_state           VARCHAR(5)
);


-- 2.2  ORDERS ---------------------------------------------------
DROP TABLE IF EXISTS raw_olist_orders_dataset;
CREATE TABLE raw_olist_orders_dataset (
    order_id                       VARCHAR(50),
    customer_id                    VARCHAR(50),
    order_status                   VARCHAR(30),
    order_purchase_timestamp       TIMESTAMP,
    order_approved_at              TIMESTAMP,
    order_delivered_carrier_date   TIMESTAMP,
    order_delivered_customer_date  TIMESTAMP,
    order_estimated_delivery_date  TIMESTAMP
);


-- 2.3  ORDER ITEMS ----------------------------------------------
DROP TABLE IF EXISTS raw_olist_order_items_dataset;
CREATE TABLE raw_olist_order_items_dataset (
    order_id            VARCHAR(50),
    order_item_id       INT,
    product_id          VARCHAR(50),
    seller_id           VARCHAR(50),
    shipping_limit_date TIMESTAMP,
    price               DECIMAL(12,2),
    freight_value       DECIMAL(12,2)
);


-- 2.4  ORDER PAYMENTS -------------------------------------------
DROP TABLE IF EXISTS raw_olist_order_payments_dataset;
CREATE TABLE raw_olist_order_payments_dataset (
    order_id             VARCHAR(50),
    payment_sequential   INT,
    payment_type         VARCHAR(30),
    payment_installments INT,
    payment_value        DECIMAL(12,2)
);


-- 2.5  PRODUCTS -------------------------------------------------
DROP TABLE IF EXISTS raw_olist_products_dataset;
CREATE TABLE raw_olist_products_dataset (
    product_id                 VARCHAR(50),
    product_category_name      VARCHAR(120),
    product_name_length        INT,
    product_description_length INT,
    product_photos_qty         INT,
    product_weight_g           DECIMAL(12,2),
    product_length_cm          DECIMAL(10,2),
    product_height_cm          DECIMAL(10,2),
    product_width_cm           DECIMAL(10,2)
);


-- 2.6  SELLERS --------------------------------------------------
DROP TABLE IF EXISTS raw_olist_sellers_dataset;
CREATE TABLE raw_olist_sellers_dataset (
    seller_id              VARCHAR(50),
    seller_zip_code_prefix INT,
    seller_city            VARCHAR(120),
    seller_state           VARCHAR(5)
);


-- 2.7  ORDER REVIEWS --------------------------------------------
DROP TABLE IF EXISTS raw_olist_order_reviews_dataset;
CREATE TABLE raw_olist_order_reviews_dataset (
    review_id               VARCHAR(50),
    order_id                VARCHAR(50),
    review_score            INT,
    review_comment_title    VARCHAR(255),
    review_comment_message  TEXT,
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);


-- 2.8  GEOLOCATION ----------------------------------------------
DROP TABLE IF EXISTS raw_olist_geolocation_dataset;
CREATE TABLE raw_olist_geolocation_dataset (
    geolocation_zip_code_prefix INT,
    geolocation_lat             DECIMAL(18,8),
    geolocation_lng             DECIMAL(18,8),
    geolocation_city            VARCHAR(120),
    geolocation_state           VARCHAR(5)
);


-- 2.9  PRODUCT CATEGORY NAME TRANSLATION ------------------------
DROP TABLE IF EXISTS raw_product_category_name_translation;
CREATE TABLE raw_product_category_name_translation (
    product_category_name         VARCHAR(120),
    product_category_name_english VARCHAR(120)
);


-- ============================================================
-- SECTION 3: LOAD DATA FROM CSV FILES (DuckDB)
--   Files located in /app/data
-- ============================================================

-- 3.1  Load Customers -------------------------------------------
COPY raw_olist_customers_dataset
FROM '/app/data/olist_customers_dataset.csv'
(HEADER, DELIMITER ',', NULL '');

-- 3.2  Load Orders ----------------------------------------------
COPY raw_olist_orders_dataset
FROM '/app/data/olist_orders_dataset.csv'
(HEADER, DELIMITER ',', NULL '');

-- 3.3  Load Order Items -----------------------------------------
COPY raw_olist_order_items_dataset
FROM '/app/data/olist_order_items_dataset.csv'
(HEADER, DELIMITER ',', NULL '');

-- 3.4  Load Order Payments --------------------------------------
COPY raw_olist_order_payments_dataset
FROM '/app/data/olist_order_payments_dataset.csv'
(HEADER, DELIMITER ',', NULL '');

-- 3.5  Load Products --------------------------------------------
COPY raw_olist_products_dataset
FROM '/app/data/olist_products_dataset.csv'
(HEADER, DELIMITER ',', NULL '');

-- 3.6  Load Sellers ---------------------------------------------
COPY raw_olist_sellers_dataset
FROM '/app/data/olist_sellers_dataset.csv'
(HEADER, DELIMITER ',', NULL '');

-- 3.7  Load Order Reviews ---------------------------------------
COPY raw_olist_order_reviews_dataset
FROM '/app/data/olist_order_reviews_dataset.csv'
(HEADER, DELIMITER ',', NULL '');

-- 3.8  Load Geolocation -----------------------------------------
COPY raw_olist_geolocation_dataset
FROM '/app/data/olist_geolocation_dataset.csv'
(HEADER, DELIMITER ',', NULL '');

-- 3.9  Load Product Category Name Translation -------------------
COPY raw_product_category_name_translation
FROM '/app/data/product_category_name_translation.csv'
(HEADER, DELIMITER ',', NULL '');


-- ============================================================
-- SECTION 4: POST-LOAD ROW COUNT VERIFICATION
-- ============================================================

SELECT
    'raw_olist_customers_dataset'           AS table_name,
    COUNT(*)                                AS loaded_rows
FROM raw_olist_customers_dataset
UNION ALL
SELECT 'raw_olist_orders_dataset',          COUNT(*) FROM raw_olist_orders_dataset
UNION ALL
SELECT 'raw_olist_order_items_dataset',     COUNT(*) FROM raw_olist_order_items_dataset
UNION ALL
SELECT 'raw_olist_order_payments_dataset',  COUNT(*) FROM raw_olist_order_payments_dataset
UNION ALL
SELECT 'raw_olist_products_dataset',        COUNT(*) FROM raw_olist_products_dataset
UNION ALL
SELECT 'raw_olist_sellers_dataset',         COUNT(*) FROM raw_olist_sellers_dataset
UNION ALL
SELECT 'raw_olist_order_reviews_dataset',   COUNT(*) FROM raw_olist_order_reviews_dataset
UNION ALL
SELECT 'raw_olist_geolocation_dataset',     COUNT(*) FROM raw_olist_geolocation_dataset
UNION ALL
SELECT 'raw_product_category_name_translation', COUNT(*) FROM raw_product_category_name_translation;

-- ============================================================
-- END OF FILE 1
-- Next step: Run stage1_02_profiling.sql
-- ============================================================
