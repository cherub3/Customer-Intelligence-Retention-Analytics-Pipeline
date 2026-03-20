-- ============================================================
-- OLIST BRAZILIAN E-COMMERCE ANALYTICS
-- STAGE 1 | FILE 3: DATA CLEANING
-- Environment : MySQL 8+
-- Run After   : stage1_02_profiling.sql
-- ============================================================
-- For EACH table this script:
--   1. Creates reject_<table>  with all original columns + reject_reason
--   2. Creates cleaned_<table> with all original columns + cleaning metadata
--   3. Populates reject table  (invalid / duplicate rows)
--   4. Populates cleaned table (valid, standardised rows)
-- ============================================================
-- CLEANING RULES APPLIED:
--   - EXACT DUPLICATE ROW    : all columns identical  → reject (reason: EXACT_DUPLICATE_ROW)
--   - NULL PRIMARY KEY       : key column is NULL     → reject (reason: NULL_<COLUMN>)
--   - DUPLICATE KEY          : same key, different data → reject all but first (reason: DUPLICATE_<COLUMN>)
--   - BUSINESS RULE VIOLATION: price ≤ 0, score out of range etc. → reject
--   - TEXT STANDARDISATION   : TRIM + UPPER on state / TRIM + LOWER on city  (in cleaned table only)
--   - TIMESTAMP FLAGS        : orders with sequence violations flagged in cleaned table (not rejected)
-- ============================================================

-- USE olist_ecommerce;

-- ============================================================
-- TABLE 1: CUSTOMERS
-- ============================================================

DROP TABLE IF EXISTS reject_olist_customers_dataset;
CREATE TABLE reject_olist_customers_dataset (
    customer_id              VARCHAR(50),
    customer_unique_id       VARCHAR(50),
    customer_zip_code_prefix INT,
    customer_city            VARCHAR(120),
    customer_state           VARCHAR(5),
    reject_reason            VARCHAR(200),
    rejected_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS cleaned_olist_customers_dataset;
CREATE TABLE cleaned_olist_customers_dataset (
    customer_id              VARCHAR(50)   NOT NULL,
    customer_unique_id       VARCHAR(50),
    customer_zip_code_prefix INT,
    customer_city            VARCHAR(120),
    customer_state           VARCHAR(5),
    cleaned_at               TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- STEP 1A: Reject rows where customer_id IS NULL
INSERT INTO reject_olist_customers_dataset
    (customer_id, customer_unique_id, customer_zip_code_prefix,
     customer_city, customer_state, reject_reason)
SELECT
    customer_id, customer_unique_id, customer_zip_code_prefix,
    customer_city, customer_state,
    'NULL_CUSTOMER_ID'
FROM raw_olist_customers_dataset
WHERE customer_id IS NULL;

-- STEP 1B: Reject exact duplicate rows (all 5 columns identical, keep first by rn=1)
INSERT INTO reject_olist_customers_dataset
    (customer_id, customer_unique_id, customer_zip_code_prefix,
     customer_city, customer_state, reject_reason)
SELECT
    customer_id, customer_unique_id, customer_zip_code_prefix,
    customer_city, customer_state,
    'EXACT_DUPLICATE_ROW'
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, customer_unique_id,
                         customer_zip_code_prefix, customer_city, customer_state
            ORDER BY customer_id
        ) AS rn
    FROM raw_olist_customers_dataset
    WHERE customer_id IS NOT NULL
) t
WHERE rn > 1;

-- STEP 1C: Reject non-exact duplicate customer_ids
--          (same customer_id, but different other columns – after removing exact dups)
INSERT INTO reject_olist_customers_dataset
    (customer_id, customer_unique_id, customer_zip_code_prefix,
     customer_city, customer_state, reject_reason)
SELECT
    customer_id, customer_unique_id, customer_zip_code_prefix,
    customer_city, customer_state,
    'DUPLICATE_CUSTOMER_ID'
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) AS rn,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id, customer_unique_id,
                         customer_zip_code_prefix, customer_city, customer_state
            ORDER BY customer_id
        ) AS exact_rn
    FROM raw_olist_customers_dataset
    WHERE customer_id IS NOT NULL
) t
WHERE rn > 1 AND exact_rn = 1;

-- STEP 1D: Insert valid rows into cleaned table (TRIM + UPPER/LOWER standardisation)
INSERT INTO cleaned_olist_customers_dataset
    (customer_id, customer_unique_id, customer_zip_code_prefix,
     customer_city, customer_state)
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    LOWER(TRIM(customer_city))  AS customer_city,
    UPPER(TRIM(customer_state)) AS customer_state
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_id) AS rn
    FROM raw_olist_customers_dataset
    WHERE customer_id IS NOT NULL
) t
WHERE rn = 1;


-- ============================================================
-- TABLE 2: ORDERS
-- ============================================================

DROP TABLE IF EXISTS reject_olist_orders_dataset;
CREATE TABLE reject_olist_orders_dataset (
    order_id                       VARCHAR(50),
    customer_id                    VARCHAR(50),
    order_status                   VARCHAR(30),
    order_purchase_timestamp       TIMESTAMP,
    order_approved_at              TIMESTAMP,
    order_delivered_carrier_date   TIMESTAMP,
    order_delivered_customer_date  TIMESTAMP,
    order_estimated_delivery_date  TIMESTAMP,
    reject_reason                  VARCHAR(200),
    rejected_at                    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS cleaned_olist_orders_dataset;
CREATE TABLE cleaned_olist_orders_dataset (
    order_id                       VARCHAR(50) NOT NULL,
    customer_id                    VARCHAR(50),
    order_status                   VARCHAR(30),
    order_purchase_timestamp       TIMESTAMP,
    order_approved_at              TIMESTAMP,
    order_delivered_carrier_date   TIMESTAMP,
    order_delivered_customer_date  TIMESTAMP,
    order_estimated_delivery_date  TIMESTAMP,
    -- Quality flag: 0=no issue, 1=timestamp sequence inconsistency detected
    timestamp_sequence_flag        TINYINT DEFAULT 0,
    cleaned_at                     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- STEP 2A: Reject NULL order_id
INSERT INTO reject_olist_orders_dataset
    (order_id, customer_id, order_status, order_purchase_timestamp,
     order_approved_at, order_delivered_carrier_date,
     order_delivered_customer_date, order_estimated_delivery_date, reject_reason)
SELECT
    order_id, customer_id, order_status, order_purchase_timestamp,
    order_approved_at, order_delivered_carrier_date,
    order_delivered_customer_date, order_estimated_delivery_date,
    'NULL_ORDER_ID'
FROM raw_olist_orders_dataset
WHERE order_id IS NULL;

-- STEP 2B: Reject exact duplicate rows
INSERT INTO reject_olist_orders_dataset
    (order_id, customer_id, order_status, order_purchase_timestamp,
     order_approved_at, order_delivered_carrier_date,
     order_delivered_customer_date, order_estimated_delivery_date, reject_reason)
SELECT
    order_id, customer_id, order_status, order_purchase_timestamp,
    order_approved_at, order_delivered_carrier_date,
    order_delivered_customer_date, order_estimated_delivery_date,
    'EXACT_DUPLICATE_ROW'
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, customer_id, order_status,
                         order_purchase_timestamp, order_approved_at,
                         order_delivered_carrier_date, order_delivered_customer_date,
                         order_estimated_delivery_date
            ORDER BY order_id
        ) AS rn
    FROM raw_olist_orders_dataset
    WHERE order_id IS NOT NULL
) t
WHERE rn > 1;

-- STEP 2C: Reject non-exact duplicate order_ids
INSERT INTO reject_olist_orders_dataset
    (order_id, customer_id, order_status, order_purchase_timestamp,
     order_approved_at, order_delivered_carrier_date,
     order_delivered_customer_date, order_estimated_delivery_date, reject_reason)
SELECT
    order_id, customer_id, order_status, order_purchase_timestamp,
    order_approved_at, order_delivered_carrier_date,
    order_delivered_customer_date, order_estimated_delivery_date,
    'DUPLICATE_ORDER_ID'
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_id) AS rn,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, customer_id, order_status,
                         order_purchase_timestamp, order_approved_at,
                         order_delivered_carrier_date, order_delivered_customer_date,
                         order_estimated_delivery_date
            ORDER BY order_id
        ) AS exact_rn
    FROM raw_olist_orders_dataset
    WHERE order_id IS NOT NULL
) t
WHERE rn > 1 AND exact_rn = 1;

-- STEP 2D: Insert valid rows into cleaned table
--          Timestamp violations are FLAGGED (not rejected) via timestamp_sequence_flag
INSERT INTO cleaned_olist_orders_dataset
    (order_id, customer_id, order_status, order_purchase_timestamp,
     order_approved_at, order_delivered_carrier_date,
     order_delivered_customer_date, order_estimated_delivery_date,
     timestamp_sequence_flag)
SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    CASE
        WHEN (order_purchase_timestamp IS NOT NULL AND order_approved_at IS NOT NULL
              AND order_purchase_timestamp > order_approved_at)
          OR (order_approved_at IS NOT NULL AND order_delivered_carrier_date IS NOT NULL
              AND order_approved_at > order_delivered_carrier_date)
          OR (order_delivered_carrier_date IS NOT NULL AND order_delivered_customer_date IS NOT NULL
              AND order_delivered_carrier_date > order_delivered_customer_date)
        THEN 1
        ELSE 0
    END AS timestamp_sequence_flag
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_id) AS rn
    FROM raw_olist_orders_dataset
    WHERE order_id IS NOT NULL
) t
WHERE rn = 1;


-- ============================================================
-- TABLE 3: ORDER ITEMS
-- ============================================================

DROP TABLE IF EXISTS reject_olist_order_items_dataset;
CREATE TABLE reject_olist_order_items_dataset (
    order_id            VARCHAR(50),
    order_item_id       INT,
    product_id          VARCHAR(50),
    seller_id           VARCHAR(50),
    shipping_limit_date TIMESTAMP,
    price               DECIMAL(12,2),
    freight_value       DECIMAL(12,2),
    reject_reason       VARCHAR(200),
    rejected_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS cleaned_olist_order_items_dataset;
CREATE TABLE cleaned_olist_order_items_dataset (
    order_id            VARCHAR(50) NOT NULL,
    order_item_id       INT         NOT NULL,
    product_id          VARCHAR(50),
    seller_id           VARCHAR(50),
    shipping_limit_date TIMESTAMP,
    price               DECIMAL(12,2),
    freight_value       DECIMAL(12,2),
    cleaned_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- STEP 3A: Reject NULL order_id
INSERT INTO reject_olist_order_items_dataset
    (order_id, order_item_id, product_id, seller_id,
     shipping_limit_date, price, freight_value, reject_reason)
SELECT order_id, order_item_id, product_id, seller_id,
       shipping_limit_date, price, freight_value,
       'NULL_ORDER_ID'
FROM raw_olist_order_items_dataset
WHERE order_id IS NULL;

-- STEP 3B: Reject price <= 0
INSERT INTO reject_olist_order_items_dataset
    (order_id, order_item_id, product_id, seller_id,
     shipping_limit_date, price, freight_value, reject_reason)
SELECT order_id, order_item_id, product_id, seller_id,
       shipping_limit_date, price, freight_value,
       'INVALID_PRICE_ZERO_OR_NEGATIVE'
FROM raw_olist_order_items_dataset
WHERE order_id IS NOT NULL
  AND (price IS NULL OR price <= 0);

-- STEP 3C: Reject freight_value < 0 (0 is acceptable – free shipping)
INSERT INTO reject_olist_order_items_dataset
    (order_id, order_item_id, product_id, seller_id,
     shipping_limit_date, price, freight_value, reject_reason)
SELECT order_id, order_item_id, product_id, seller_id,
       shipping_limit_date, price, freight_value,
       'INVALID_FREIGHT_NEGATIVE'
FROM raw_olist_order_items_dataset
WHERE order_id IS NOT NULL
  AND (price IS NOT NULL AND price > 0)
  AND (freight_value IS NOT NULL AND freight_value < 0);

-- STEP 3D: Reject exact duplicate rows
INSERT INTO reject_olist_order_items_dataset
    (order_id, order_item_id, product_id, seller_id,
     shipping_limit_date, price, freight_value, reject_reason)
SELECT order_id, order_item_id, product_id, seller_id,
       shipping_limit_date, price, freight_value,
       'EXACT_DUPLICATE_ROW'
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, order_item_id, product_id, seller_id,
                         shipping_limit_date, price, freight_value
            ORDER BY order_id
        ) AS rn
    FROM raw_olist_order_items_dataset
    WHERE order_id IS NOT NULL
      AND (price IS NOT NULL AND price > 0)
      AND (freight_value IS NULL OR freight_value >= 0)
) t
WHERE rn > 1;

-- STEP 3E: Insert valid rows into cleaned table
INSERT INTO cleaned_olist_order_items_dataset
    (order_id, order_item_id, product_id, seller_id,
     shipping_limit_date, price, freight_value)
SELECT order_id, order_item_id, product_id, seller_id,
       shipping_limit_date, price, freight_value
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, order_item_id, product_id, seller_id,
                         shipping_limit_date, price, freight_value
            ORDER BY order_id
        ) AS rn
    FROM raw_olist_order_items_dataset
    WHERE order_id IS NOT NULL
      AND price IS NOT NULL AND price > 0
      AND (freight_value IS NULL OR freight_value >= 0)
) t
WHERE rn = 1;


-- ============================================================
-- TABLE 4: ORDER PAYMENTS
-- ============================================================

DROP TABLE IF EXISTS reject_olist_order_payments_dataset;
CREATE TABLE reject_olist_order_payments_dataset (
    order_id             VARCHAR(50),
    payment_sequential   INT,
    payment_type         VARCHAR(30),
    payment_installments INT,
    payment_value        DECIMAL(12,2),
    reject_reason        VARCHAR(200),
    rejected_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS cleaned_olist_order_payments_dataset;
CREATE TABLE cleaned_olist_order_payments_dataset (
    order_id             VARCHAR(50) NOT NULL,
    payment_sequential   INT,
    payment_type         VARCHAR(30),
    payment_installments INT,
    payment_value        DECIMAL(12,2),
    cleaned_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- STEP 4A: Reject NULL order_id
INSERT INTO reject_olist_order_payments_dataset
    (order_id, payment_sequential, payment_type,
     payment_installments, payment_value, reject_reason)
SELECT order_id, payment_sequential, payment_type,
       payment_installments, payment_value,
       'NULL_ORDER_ID'
FROM raw_olist_order_payments_dataset
WHERE order_id IS NULL;

-- STEP 4B: Reject payment_value <= 0
INSERT INTO reject_olist_order_payments_dataset
    (order_id, payment_sequential, payment_type,
     payment_installments, payment_value, reject_reason)
SELECT order_id, payment_sequential, payment_type,
       payment_installments, payment_value,
       'INVALID_PAYMENT_VALUE_ZERO_OR_NEGATIVE'
FROM raw_olist_order_payments_dataset
WHERE order_id IS NOT NULL
  AND (payment_value IS NULL OR payment_value <= 0);

-- STEP 4C: Reject invalid payment_type
INSERT INTO reject_olist_order_payments_dataset
    (order_id, payment_sequential, payment_type,
     payment_installments, payment_value, reject_reason)
SELECT order_id, payment_sequential, payment_type,
       payment_installments, payment_value,
       'INVALID_PAYMENT_TYPE'
FROM raw_olist_order_payments_dataset
WHERE order_id    IS NOT NULL
  AND (payment_value IS NOT NULL AND payment_value > 0)
  AND payment_type NOT IN ('credit_card','boleto','voucher','debit_card','not_defined');

-- STEP 4D: Reject payment_installments < 1
INSERT INTO reject_olist_order_payments_dataset
    (order_id, payment_sequential, payment_type,
     payment_installments, payment_value, reject_reason)
SELECT order_id, payment_sequential, payment_type,
       payment_installments, payment_value,
       'INVALID_INSTALLMENTS_LESS_THAN_ONE'
FROM raw_olist_order_payments_dataset
WHERE order_id    IS NOT NULL
  AND (payment_value IS NOT NULL AND payment_value > 0)
  AND payment_type IN ('credit_card','boleto','voucher','debit_card','not_defined')
  AND (payment_installments IS NULL OR payment_installments < 1);

-- STEP 4E: Reject exact duplicate rows
INSERT INTO reject_olist_order_payments_dataset
    (order_id, payment_sequential, payment_type,
     payment_installments, payment_value, reject_reason)
SELECT order_id, payment_sequential, payment_type,
       payment_installments, payment_value,
       'EXACT_DUPLICATE_ROW'
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, payment_sequential, payment_type,
                         payment_installments, payment_value
            ORDER BY order_id
        ) AS rn
    FROM raw_olist_order_payments_dataset
    WHERE order_id    IS NOT NULL
      AND (payment_value IS NOT NULL AND payment_value > 0)
      AND payment_type IN ('credit_card','boleto','voucher','debit_card','not_defined')
      AND (payment_installments IS NOT NULL AND payment_installments >= 1)
) t
WHERE rn > 1;

-- STEP 4F: Insert valid rows into cleaned table
INSERT INTO cleaned_olist_order_payments_dataset
    (order_id, payment_sequential, payment_type,
     payment_installments, payment_value)
SELECT order_id, payment_sequential, payment_type,
       payment_installments, payment_value
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id, payment_sequential, payment_type,
                         payment_installments, payment_value
            ORDER BY order_id
        ) AS rn
    FROM raw_olist_order_payments_dataset
    WHERE order_id    IS NOT NULL
      AND payment_value IS NOT NULL AND payment_value > 0
      AND payment_type IN ('credit_card','boleto','voucher','debit_card','not_defined')
      AND payment_installments IS NOT NULL AND payment_installments >= 1
) t
WHERE rn = 1;


-- ============================================================
-- TABLE 5: PRODUCTS
-- ============================================================

DROP TABLE IF EXISTS reject_olist_products_dataset;
CREATE TABLE reject_olist_products_dataset (
    product_id                 VARCHAR(50),
    product_category_name      VARCHAR(120),
    product_name_length        INT,
    product_description_length INT,
    product_photos_qty         INT,
    product_weight_g           DECIMAL(12,2),
    product_length_cm          DECIMAL(10,2),
    product_height_cm          DECIMAL(10,2),
    product_width_cm           DECIMAL(10,2),
    reject_reason              VARCHAR(200),
    rejected_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS cleaned_olist_products_dataset;
CREATE TABLE cleaned_olist_products_dataset (
    product_id                 VARCHAR(50) NOT NULL,
    product_category_name      VARCHAR(120),
    product_name_length        INT,
    product_description_length INT,
    product_photos_qty         INT,
    product_weight_g           DECIMAL(12,2),
    product_length_cm          DECIMAL(10,2),
    product_height_cm          DECIMAL(10,2),
    product_width_cm           DECIMAL(10,2),
    -- quality flags (flagged but NOT rejected – missing data is acceptable)
    category_missing_flag      TINYINT DEFAULT 0,
    dimensions_missing_flag    TINYINT DEFAULT 0,
    cleaned_at                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- STEP 5A: Reject NULL product_id
INSERT INTO reject_olist_products_dataset
    (product_id, product_category_name, product_name_length,
     product_description_length, product_photos_qty,
     product_weight_g, product_length_cm, product_height_cm, product_width_cm,
     reject_reason)
SELECT product_id, product_category_name, product_name_length,
       product_description_length, product_photos_qty,
       product_weight_g, product_length_cm, product_height_cm, product_width_cm,
       'NULL_PRODUCT_ID'
FROM raw_olist_products_dataset
WHERE product_id IS NULL;

-- STEP 5B: Reject exact duplicate rows
INSERT INTO reject_olist_products_dataset
    (product_id, product_category_name, product_name_length,
     product_description_length, product_photos_qty,
     product_weight_g, product_length_cm, product_height_cm, product_width_cm,
     reject_reason)
SELECT product_id, product_category_name, product_name_length,
       product_description_length, product_photos_qty,
       product_weight_g, product_length_cm, product_height_cm, product_width_cm,
       'EXACT_DUPLICATE_ROW'
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id, product_category_name, product_name_length,
                         product_description_length, product_photos_qty,
                         product_weight_g, product_length_cm, product_height_cm, product_width_cm
            ORDER BY product_id
        ) AS rn
    FROM raw_olist_products_dataset
    WHERE product_id IS NOT NULL
) t
WHERE rn > 1;

-- STEP 5C: Reject non-exact duplicate product_ids
INSERT INTO reject_olist_products_dataset
    (product_id, product_category_name, product_name_length,
     product_description_length, product_photos_qty,
     product_weight_g, product_length_cm, product_height_cm, product_width_cm,
     reject_reason)
SELECT product_id, product_category_name, product_name_length,
       product_description_length, product_photos_qty,
       product_weight_g, product_length_cm, product_height_cm, product_width_cm,
       'DUPLICATE_PRODUCT_ID'
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn,
        ROW_NUMBER() OVER (
            PARTITION BY product_id, product_category_name, product_name_length,
                         product_description_length, product_photos_qty,
                         product_weight_g, product_length_cm, product_height_cm, product_width_cm
            ORDER BY product_id
        ) AS exact_rn
    FROM raw_olist_products_dataset
    WHERE product_id IS NOT NULL
) t
WHERE rn > 1 AND exact_rn = 1;

-- STEP 5D: Insert valid rows into cleaned table
--          Category name: TRIM + LOWER
--          Missing category or dimensions are FLAGGED (not rejected)
INSERT INTO cleaned_olist_products_dataset
    (product_id, product_category_name, product_name_length,
     product_description_length, product_photos_qty,
     product_weight_g, product_length_cm, product_height_cm, product_width_cm,
     category_missing_flag, dimensions_missing_flag)
SELECT
    product_id,
    LOWER(TRIM(product_category_name)) AS product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    CASE WHEN product_category_name IS NULL THEN 1 ELSE 0 END AS category_missing_flag,
    CASE WHEN product_weight_g IS NULL
          OR  product_length_cm IS NULL
          OR  product_height_cm IS NULL
          OR  product_width_cm  IS NULL THEN 1 ELSE 0 END     AS dimensions_missing_flag
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn
    FROM raw_olist_products_dataset
    WHERE product_id IS NOT NULL
) t
WHERE rn = 1;


-- ============================================================
-- TABLE 6: SELLERS
-- ============================================================

DROP TABLE IF EXISTS reject_olist_sellers_dataset;
CREATE TABLE reject_olist_sellers_dataset (
    seller_id              VARCHAR(50),
    seller_zip_code_prefix INT,
    seller_city            VARCHAR(120),
    seller_state           VARCHAR(5),
    reject_reason          VARCHAR(200),
    rejected_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS cleaned_olist_sellers_dataset;
CREATE TABLE cleaned_olist_sellers_dataset (
    seller_id              VARCHAR(50) NOT NULL,
    seller_zip_code_prefix INT,
    seller_city            VARCHAR(120),
    seller_state           VARCHAR(5),
    cleaned_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- STEP 6A: Reject NULL seller_id
INSERT INTO reject_olist_sellers_dataset
    (seller_id, seller_zip_code_prefix, seller_city, seller_state, reject_reason)
SELECT seller_id, seller_zip_code_prefix, seller_city, seller_state, 'NULL_SELLER_ID'
FROM raw_olist_sellers_dataset
WHERE seller_id IS NULL;

-- STEP 6B: Reject exact duplicate rows
INSERT INTO reject_olist_sellers_dataset
    (seller_id, seller_zip_code_prefix, seller_city, seller_state, reject_reason)
SELECT seller_id, seller_zip_code_prefix, seller_city, seller_state, 'EXACT_DUPLICATE_ROW'
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY seller_id, seller_zip_code_prefix, seller_city, seller_state
            ORDER BY seller_id
        ) AS rn
    FROM raw_olist_sellers_dataset
    WHERE seller_id IS NOT NULL
) t
WHERE rn > 1;

-- STEP 6C: Reject non-exact duplicate seller_ids
INSERT INTO reject_olist_sellers_dataset
    (seller_id, seller_zip_code_prefix, seller_city, seller_state, reject_reason)
SELECT seller_id, seller_zip_code_prefix, seller_city, seller_state, 'DUPLICATE_SELLER_ID'
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY seller_id) AS rn,
        ROW_NUMBER() OVER (
            PARTITION BY seller_id, seller_zip_code_prefix, seller_city, seller_state
            ORDER BY seller_id
        ) AS exact_rn
    FROM raw_olist_sellers_dataset
    WHERE seller_id IS NOT NULL
) t
WHERE rn > 1 AND exact_rn = 1;

-- STEP 6D: Insert valid rows into cleaned table (TRIM + UPPER/LOWER)
INSERT INTO cleaned_olist_sellers_dataset
    (seller_id, seller_zip_code_prefix, seller_city, seller_state)
SELECT
    seller_id,
    seller_zip_code_prefix,
    LOWER(TRIM(seller_city))  AS seller_city,
    UPPER(TRIM(seller_state)) AS seller_state
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY seller_id) AS rn
    FROM raw_olist_sellers_dataset
    WHERE seller_id IS NOT NULL
) t
WHERE rn = 1;


-- ============================================================
-- TABLE 7: ORDER REVIEWS
-- ============================================================

DROP TABLE IF EXISTS reject_olist_order_reviews_dataset;
CREATE TABLE reject_olist_order_reviews_dataset (
    review_id               VARCHAR(50),
    order_id                VARCHAR(50),
    review_score            INT,
    review_comment_title    VARCHAR(255),
    review_comment_message  TEXT,
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    reject_reason           VARCHAR(200),
    rejected_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS cleaned_olist_order_reviews_dataset;
CREATE TABLE cleaned_olist_order_reviews_dataset (
    review_id               VARCHAR(50) NOT NULL,
    order_id                VARCHAR(50),
    review_score            INT,
    review_comment_title    VARCHAR(255),
    review_comment_message  TEXT,
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    cleaned_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- STEP 7A: Reject NULL review_id
INSERT INTO reject_olist_order_reviews_dataset
    (review_id, order_id, review_score, review_comment_title,
     review_comment_message, review_creation_date, review_answer_timestamp, reject_reason)
SELECT review_id, order_id, review_score, review_comment_title,
       review_comment_message, review_creation_date, review_answer_timestamp,
       'NULL_REVIEW_ID'
FROM raw_olist_order_reviews_dataset
WHERE review_id IS NULL;

-- STEP 7B: Reject invalid review_score (must be 1–5)
INSERT INTO reject_olist_order_reviews_dataset
    (review_id, order_id, review_score, review_comment_title,
     review_comment_message, review_creation_date, review_answer_timestamp, reject_reason)
SELECT review_id, order_id, review_score, review_comment_title,
       review_comment_message, review_creation_date, review_answer_timestamp,
       'INVALID_REVIEW_SCORE_OUT_OF_RANGE'
FROM raw_olist_order_reviews_dataset
WHERE review_id IS NOT NULL
  AND (review_score IS NULL OR review_score NOT BETWEEN 1 AND 5);

-- STEP 7C: Reject exact duplicate rows (all 7 columns)
INSERT INTO reject_olist_order_reviews_dataset
    (review_id, order_id, review_score, review_comment_title,
     review_comment_message, review_creation_date, review_answer_timestamp, reject_reason)
SELECT review_id, order_id, review_score, review_comment_title,
       review_comment_message, review_creation_date, review_answer_timestamp,
       'EXACT_DUPLICATE_ROW'
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY review_id, order_id, review_score,
                         review_comment_title, review_comment_message,
                         review_creation_date, review_answer_timestamp
            ORDER BY review_id
        ) AS rn
    FROM raw_olist_order_reviews_dataset
    WHERE review_id IS NOT NULL
      AND review_score BETWEEN 1 AND 5
) t
WHERE rn > 1;

-- STEP 7D: Insert valid rows into cleaned table
--          Same review_id can appear for different orders – deduplicate on all columns
INSERT INTO cleaned_olist_order_reviews_dataset
    (review_id, order_id, review_score, review_comment_title,
     review_comment_message, review_creation_date, review_answer_timestamp)
SELECT
    review_id,
    order_id,
    review_score,
    TRIM(review_comment_title)   AS review_comment_title,
    TRIM(review_comment_message) AS review_comment_message,
    review_creation_date,
    review_answer_timestamp
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY review_id, order_id, review_score,
                         review_comment_title, review_comment_message,
                         review_creation_date, review_answer_timestamp
            ORDER BY review_id
        ) AS rn
    FROM raw_olist_order_reviews_dataset
    WHERE review_id IS NOT NULL
      AND review_score BETWEEN 1 AND 5
) t
WHERE rn = 1;


-- ============================================================
-- TABLE 8: GEOLOCATION
-- ============================================================

DROP TABLE IF EXISTS reject_olist_geolocation_dataset;
CREATE TABLE reject_olist_geolocation_dataset (
    geolocation_zip_code_prefix INT,
    geolocation_lat             DECIMAL(18,8),
    geolocation_lng             DECIMAL(18,8),
    geolocation_city            VARCHAR(120),
    geolocation_state           VARCHAR(5),
    reject_reason               VARCHAR(200),
    rejected_at                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS cleaned_olist_geolocation_dataset;
CREATE TABLE cleaned_olist_geolocation_dataset (
    geolocation_zip_code_prefix INT,
    geolocation_lat             DECIMAL(18,8),
    geolocation_lng             DECIMAL(18,8),
    geolocation_city            VARCHAR(120),
    geolocation_state           VARCHAR(5),
    cleaned_at                  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- NOTE: Multiple lat/lng per zip code is expected in this dataset.
--       We keep all distinct (zip, lat, lng, city, state) combinations.
--       Only exact duplicate rows (all 5 columns identical) and
--       coordinates outside Brazil's bounding box are rejected.

-- STEP 8A: Reject exact duplicate rows (all 5 columns identical)
INSERT INTO reject_olist_geolocation_dataset
    (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
     geolocation_city, geolocation_state, reject_reason)
SELECT geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
       geolocation_city, geolocation_state,
       'EXACT_DUPLICATE_ROW'
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
                         geolocation_city, geolocation_state
            ORDER BY geolocation_zip_code_prefix
        ) AS rn
    FROM raw_olist_geolocation_dataset
) t
WHERE rn > 1;

-- STEP 8B: Reject lat/lng outside Brazil's bounding box
--          Brazil lat: -33.75 to +5.27 | lng: -73.99 to -34.79
INSERT INTO reject_olist_geolocation_dataset
    (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
     geolocation_city, geolocation_state, reject_reason)
SELECT geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
       geolocation_city, geolocation_state,
       'COORDINATES_OUTSIDE_BRAZIL_BOUNDS'
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
                         geolocation_city, geolocation_state
            ORDER BY geolocation_zip_code_prefix
        ) AS rn
    FROM raw_olist_geolocation_dataset
) t
WHERE rn = 1
  AND (
      geolocation_lat < -33.75 OR geolocation_lat > 5.27
   OR geolocation_lng < -73.99 OR geolocation_lng > -34.79
  );

-- STEP 8C: Insert valid rows into cleaned table
--          Standardise: LOWER(TRIM(city)), UPPER(TRIM(state))
INSERT INTO cleaned_olist_geolocation_dataset
    (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
     geolocation_city, geolocation_state)
SELECT DISTINCT
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    LOWER(TRIM(geolocation_city))  AS geolocation_city,
    UPPER(TRIM(geolocation_state)) AS geolocation_state
FROM raw_olist_geolocation_dataset
WHERE geolocation_lat BETWEEN -33.75 AND 5.27
  AND geolocation_lng BETWEEN -73.99 AND -34.79;


-- ============================================================
-- TABLE 9: PRODUCT CATEGORY NAME TRANSLATION
-- ============================================================

DROP TABLE IF EXISTS reject_product_category_name_translation;
CREATE TABLE reject_product_category_name_translation (
    product_category_name         VARCHAR(120),
    product_category_name_english VARCHAR(120),
    reject_reason                 VARCHAR(200),
    rejected_at                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS cleaned_product_category_name_translation;
CREATE TABLE cleaned_product_category_name_translation (
    product_category_name         VARCHAR(120) NOT NULL,
    product_category_name_english VARCHAR(120),
    cleaned_at                    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- STEP 9A: Reject NULL product_category_name (Portuguese key)
INSERT INTO reject_product_category_name_translation
    (product_category_name, product_category_name_english, reject_reason)
SELECT product_category_name, product_category_name_english, 'NULL_CATEGORY_NAME'
FROM raw_product_category_name_translation
WHERE product_category_name IS NULL;

-- STEP 9B: Reject exact duplicate rows
INSERT INTO reject_product_category_name_translation
    (product_category_name, product_category_name_english, reject_reason)
SELECT product_category_name, product_category_name_english, 'EXACT_DUPLICATE_ROW'
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY product_category_name, product_category_name_english
            ORDER BY product_category_name
        ) AS rn
    FROM raw_product_category_name_translation
    WHERE product_category_name IS NOT NULL
) t
WHERE rn > 1;

-- STEP 9C: Reject duplicate Portuguese category names (keep first)
INSERT INTO reject_product_category_name_translation
    (product_category_name, product_category_name_english, reject_reason)
SELECT product_category_name, product_category_name_english, 'DUPLICATE_CATEGORY_NAME'
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY product_category_name ORDER BY product_category_name) AS rn,
        ROW_NUMBER() OVER (
            PARTITION BY product_category_name, product_category_name_english
            ORDER BY product_category_name
        ) AS exact_rn
    FROM raw_product_category_name_translation
    WHERE product_category_name IS NOT NULL
) t
WHERE rn > 1 AND exact_rn = 1;

-- STEP 9D: Insert valid rows into cleaned table (TRIM + LOWER)
INSERT INTO cleaned_product_category_name_translation
    (product_category_name, product_category_name_english)
SELECT
    LOWER(TRIM(product_category_name))         AS product_category_name,
    LOWER(TRIM(product_category_name_english)) AS product_category_name_english
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY product_category_name ORDER BY product_category_name) AS rn
    FROM raw_product_category_name_translation
    WHERE product_category_name IS NOT NULL
) t
WHERE rn = 1;

-- ============================================================
-- END OF FILE 3
-- Next step: Run stage1_04_validation.sql
-- ============================================================
