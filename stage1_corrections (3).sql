-- ============================================================
-- OLIST BRAZILIAN E-COMMERCE ANALYTICS
-- STAGE 1 CORRECTIONS — REVISED CLEANING (ALL 9 TABLES)
-- Environment : MySQL 8+
-- Run After   : stage1_01_setup_and_load.sql (raw tables must exist)
-- ============================================================
-- CORRECTIONS APPLIED vs ORIGINAL STAGE 1:
--   C1 : Duplicate primary keys  → duplicate_flag column added; ALL rows kept
--   C2 : Order items composite key → (order_id, order_item_id)
--   C3 : Price: price < 0 → reject; price = 0 → flag as is_free_item
--   C4 : Geolocation out-of-bounds → is_out_of_bounds flag (NOT rejected)
--   C5 : Reviews → composite key (review_id, order_id); review_id alone not an error
--   C6 : Referential integrity → handled in Stage 2 (File 1)
-- ============================================================
-- APPROACH FOR DUPLICATES:
--   - Exact duplicate rows (ALL columns identical) : one kept in cleaned,
--     extras represented in reject with EXACT_DUPLICATE_ROW + extra_count
--   - Non-exact duplicate primary keys             : ALL rows kept in cleaned,
--     duplicate_flag = 1 on every row sharing that key
-- ============================================================

-- USE olist_ecommerce;

-- ============================================================
-- TABLE 1: CUSTOMERS  (C1)
-- ============================================================

DROP TABLE IF EXISTS reject_olist_customers_dataset;
CREATE TABLE reject_olist_customers_dataset (
    customer_id              VARCHAR(50),
    customer_unique_id       VARCHAR(50),
    customer_zip_code_prefix INT,
    customer_city            VARCHAR(120),
    customer_state           VARCHAR(5),
    reject_reason            VARCHAR(300),
    rejected_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS cleaned_olist_customers_dataset;
CREATE TABLE cleaned_olist_customers_dataset (
    customer_id              VARCHAR(50),
    customer_unique_id       VARCHAR(50),
    customer_zip_code_prefix INT,
    customer_city            VARCHAR(120),
    customer_state           VARCHAR(5),
    duplicate_flag           TINYINT  DEFAULT 0,
    cleaned_at               TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
);

-- Reject: NULL customer_id
INSERT INTO reject_olist_customers_dataset
    (customer_id, customer_unique_id, customer_zip_code_prefix,
     customer_city, customer_state, reject_reason)
SELECT customer_id, customer_unique_id, customer_zip_code_prefix,
       customer_city, customer_state, 'NULL_CUSTOMER_ID'
FROM raw_olist_customers_dataset
WHERE customer_id IS NULL;

-- Reject: Exact duplicate rows (1 representative row per group, noting extra count)
INSERT INTO reject_olist_customers_dataset
    (customer_id, customer_unique_id, customer_zip_code_prefix,
     customer_city, customer_state, reject_reason)
SELECT customer_id, customer_unique_id, customer_zip_code_prefix,
       customer_city, customer_state,
       CONCAT('EXACT_DUPLICATE_ROW (', COUNT(*) - 1, ' extra copies removed)')
FROM raw_olist_customers_dataset
WHERE customer_id IS NOT NULL
GROUP BY customer_id, customer_unique_id, customer_zip_code_prefix,
         customer_city, customer_state
HAVING COUNT(*) > 1;

-- Cleaned: ALL distinct (non-null, no-exact-dup) rows; duplicate_flag marks shared customer_id
INSERT INTO cleaned_olist_customers_dataset
    (customer_id, customer_unique_id, customer_zip_code_prefix,
     customer_city, customer_state, duplicate_flag)
SELECT
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    LOWER(TRIM(customer_city))  AS customer_city,
    UPPER(TRIM(customer_state)) AS customer_state,
    CASE WHEN COUNT(*) OVER (PARTITION BY customer_id) > 1 THEN 1 ELSE 0 END AS duplicate_flag
FROM (
    SELECT customer_id, customer_unique_id, customer_zip_code_prefix,
           customer_city, customer_state
    FROM raw_olist_customers_dataset
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id, customer_unique_id, customer_zip_code_prefix,
             customer_city, customer_state
) t;


-- ============================================================
-- TABLE 2: ORDERS  (C1)
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
    reject_reason                  VARCHAR(300),
    rejected_at                    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS cleaned_olist_orders_dataset;
CREATE TABLE cleaned_olist_orders_dataset (
    order_id                       VARCHAR(50),
    customer_id                    VARCHAR(50),
    order_status                   VARCHAR(30),
    order_purchase_timestamp       TIMESTAMP,
    order_approved_at              TIMESTAMP,
    order_delivered_carrier_date   TIMESTAMP,
    order_delivered_customer_date  TIMESTAMP,
    order_estimated_delivery_date  TIMESTAMP,
    timestamp_sequence_flag        TINYINT DEFAULT 0,
    duplicate_flag                 TINYINT DEFAULT 0,
    cleaned_at                     TIMESTAMP  DEFAULT CURRENT_TIMESTAMP
);

-- Reject: NULL order_id
INSERT INTO reject_olist_orders_dataset
    (order_id, customer_id, order_status,
     order_purchase_timestamp, order_approved_at,
     order_delivered_carrier_date, order_delivered_customer_date,
     order_estimated_delivery_date, reject_reason)
SELECT order_id, customer_id, order_status,
       order_purchase_timestamp, order_approved_at,
       order_delivered_carrier_date, order_delivered_customer_date,
       order_estimated_delivery_date, 'NULL_ORDER_ID'
FROM raw_olist_orders_dataset
WHERE order_id IS NULL;

-- Reject: Exact duplicate rows
INSERT INTO reject_olist_orders_dataset
    (order_id, customer_id, order_status,
     order_purchase_timestamp, order_approved_at,
     order_delivered_carrier_date, order_delivered_customer_date,
     order_estimated_delivery_date, reject_reason)
SELECT order_id, customer_id, order_status,
       order_purchase_timestamp, order_approved_at,
       order_delivered_carrier_date, order_delivered_customer_date,
       order_estimated_delivery_date,
       CONCAT('EXACT_DUPLICATE_ROW (', COUNT(*) - 1, ' extra copies removed)')
FROM raw_olist_orders_dataset
WHERE order_id IS NOT NULL
GROUP BY order_id, customer_id, order_status,
         order_purchase_timestamp, order_approved_at,
         order_delivered_carrier_date, order_delivered_customer_date,
         order_estimated_delivery_date
HAVING COUNT(*) > 1;

-- Cleaned: ALL distinct rows; flag timestamp violations; flag duplicate order_ids
INSERT INTO cleaned_olist_orders_dataset
    (order_id, customer_id, order_status,
     order_purchase_timestamp, order_approved_at,
     order_delivered_carrier_date, order_delivered_customer_date,
     order_estimated_delivery_date,
     timestamp_sequence_flag, duplicate_flag)
SELECT
    order_id, customer_id, order_status,
    order_purchase_timestamp, order_approved_at,
    order_delivered_carrier_date, order_delivered_customer_date,
    order_estimated_delivery_date,
    CASE
        WHEN (order_purchase_timestamp  IS NOT NULL AND order_approved_at            IS NOT NULL
              AND order_purchase_timestamp  > order_approved_at)
          OR (order_approved_at            IS NOT NULL AND order_delivered_carrier_date  IS NOT NULL
              AND order_approved_at            > order_delivered_carrier_date)
          OR (order_delivered_carrier_date  IS NOT NULL AND order_delivered_customer_date IS NOT NULL
              AND order_delivered_carrier_date  > order_delivered_customer_date)
        THEN 1 ELSE 0
    END AS timestamp_sequence_flag,
    CASE WHEN COUNT(*) OVER (PARTITION BY order_id) > 1 THEN 1 ELSE 0 END AS duplicate_flag
FROM (
    SELECT order_id, customer_id, order_status,
           order_purchase_timestamp, order_approved_at,
           order_delivered_carrier_date, order_delivered_customer_date,
           order_estimated_delivery_date
    FROM raw_olist_orders_dataset
    WHERE order_id IS NOT NULL
    GROUP BY order_id, customer_id, order_status,
             order_purchase_timestamp, order_approved_at,
             order_delivered_carrier_date, order_delivered_customer_date,
             order_estimated_delivery_date
) t;


-- ============================================================
-- TABLE 3: ORDER ITEMS  (C1, C2, C3)
-- ============================================================
-- C2: Composite key = (order_id, order_item_id)
-- C3: price < 0  → reject  |  price = 0 → keep with is_free_item = 1

DROP TABLE IF EXISTS reject_olist_order_items_dataset;
CREATE TABLE reject_olist_order_items_dataset (
    order_id            VARCHAR(50),
    order_item_id       INT,
    product_id          VARCHAR(50),
    seller_id           VARCHAR(50),
    shipping_limit_date TIMESTAMP,
    price               DECIMAL(12,2),
    freight_value       DECIMAL(12,2),
    reject_reason       VARCHAR(300),
    rejected_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS cleaned_olist_order_items_dataset;
CREATE TABLE cleaned_olist_order_items_dataset (
    order_id            VARCHAR(50),
    order_item_id       INT,
    product_id          VARCHAR(50),
    seller_id           VARCHAR(50),
    shipping_limit_date TIMESTAMP,
    price               DECIMAL(12,2),
    freight_value       DECIMAL(12,2),
    is_free_item        TINYINT  DEFAULT 0,
    duplicate_flag      TINYINT  DEFAULT 0,
    cleaned_at          TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
);

-- Reject: NULL order_id
INSERT INTO reject_olist_order_items_dataset
    (order_id, order_item_id, product_id, seller_id,
     shipping_limit_date, price, freight_value, reject_reason)
SELECT order_id, order_item_id, product_id, seller_id,
       shipping_limit_date, price, freight_value, 'NULL_ORDER_ID'
FROM raw_olist_order_items_dataset
WHERE order_id IS NULL;

-- Reject: price IS NULL OR price < 0  (C3: price = 0 is now valid → is_free_item)
INSERT INTO reject_olist_order_items_dataset
    (order_id, order_item_id, product_id, seller_id,
     shipping_limit_date, price, freight_value, reject_reason)
SELECT order_id, order_item_id, product_id, seller_id,
       shipping_limit_date, price, freight_value,
       'INVALID_PRICE_NULL_OR_NEGATIVE'
FROM raw_olist_order_items_dataset
WHERE order_id IS NOT NULL
  AND (price IS NULL OR price < 0);

-- Reject: freight_value < 0
INSERT INTO reject_olist_order_items_dataset
    (order_id, order_item_id, product_id, seller_id,
     shipping_limit_date, price, freight_value, reject_reason)
SELECT order_id, order_item_id, product_id, seller_id,
       shipping_limit_date, price, freight_value,
       'INVALID_FREIGHT_NEGATIVE'
FROM raw_olist_order_items_dataset
WHERE order_id IS NOT NULL
  AND (price IS NOT NULL AND price >= 0)
  AND (freight_value IS NOT NULL AND freight_value < 0);

-- Reject: Exact duplicate rows
INSERT INTO reject_olist_order_items_dataset
    (order_id, order_item_id, product_id, seller_id,
     shipping_limit_date, price, freight_value, reject_reason)
SELECT order_id, order_item_id, product_id, seller_id,
       shipping_limit_date, price, freight_value,
       CONCAT('EXACT_DUPLICATE_ROW (', COUNT(*) - 1, ' extra copies removed)')
FROM raw_olist_order_items_dataset
WHERE order_id IS NOT NULL
  AND price IS NOT NULL AND price >= 0
  AND (freight_value IS NULL OR freight_value >= 0)
GROUP BY order_id, order_item_id, product_id, seller_id,
         shipping_limit_date, price, freight_value
HAVING COUNT(*) > 1;

-- Cleaned: ALL valid distinct rows; composite key (order_id, order_item_id) for dup detection
INSERT INTO cleaned_olist_order_items_dataset
    (order_id, order_item_id, product_id, seller_id,
     shipping_limit_date, price, freight_value,
     is_free_item, duplicate_flag)
SELECT
    order_id, order_item_id, product_id, seller_id,
    shipping_limit_date, price, freight_value,
    CASE WHEN price = 0 THEN 1 ELSE 0 END AS is_free_item,
    CASE WHEN COUNT(*) OVER (PARTITION BY order_id, order_item_id) > 1 THEN 1 ELSE 0 END AS duplicate_flag
FROM (
    SELECT order_id, order_item_id, product_id, seller_id,
           shipping_limit_date, price, freight_value
    FROM raw_olist_order_items_dataset
    WHERE order_id IS NOT NULL
      AND price IS NOT NULL AND price >= 0
      AND (freight_value IS NULL OR freight_value >= 0)
    GROUP BY order_id, order_item_id, product_id, seller_id,
             shipping_limit_date, price, freight_value
) t;


-- ============================================================
-- TABLE 4: ORDER PAYMENTS  (C1)
-- ============================================================

DROP TABLE IF EXISTS reject_olist_order_payments_dataset;
CREATE TABLE reject_olist_order_payments_dataset (
    order_id             VARCHAR(50),
    payment_sequential   INT,
    payment_type         VARCHAR(30),
    payment_installments INT,
    payment_value        DECIMAL(12,2),
    reject_reason        VARCHAR(300),
    rejected_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS cleaned_olist_order_payments_dataset;
CREATE TABLE cleaned_olist_order_payments_dataset (
    order_id             VARCHAR(50),
    payment_sequential   INT,
    payment_type         VARCHAR(30),
    payment_installments INT,
    payment_value        DECIMAL(12,2),
    duplicate_flag       TINYINT DEFAULT 0,
    cleaned_at           TIMESTAMP  DEFAULT CURRENT_TIMESTAMP
);

-- Reject: NULL order_id
INSERT INTO reject_olist_order_payments_dataset
    (order_id, payment_sequential, payment_type,
     payment_installments, payment_value, reject_reason)
SELECT order_id, payment_sequential, payment_type,
       payment_installments, payment_value, 'NULL_ORDER_ID'
FROM raw_olist_order_payments_dataset
WHERE order_id IS NULL;

-- Reject: payment_value <= 0
INSERT INTO reject_olist_order_payments_dataset
    (order_id, payment_sequential, payment_type,
     payment_installments, payment_value, reject_reason)
SELECT order_id, payment_sequential, payment_type,
       payment_installments, payment_value,
       'INVALID_PAYMENT_VALUE_ZERO_OR_NEGATIVE'
FROM raw_olist_order_payments_dataset
WHERE order_id IS NOT NULL
  AND (payment_value IS NULL OR payment_value <= 0);

-- Reject: invalid payment_type
INSERT INTO reject_olist_order_payments_dataset
    (order_id, payment_sequential, payment_type,
     payment_installments, payment_value, reject_reason)
SELECT order_id, payment_sequential, payment_type,
       payment_installments, payment_value, 'INVALID_PAYMENT_TYPE'
FROM raw_olist_order_payments_dataset
WHERE order_id IS NOT NULL
  AND (payment_value IS NOT NULL AND payment_value > 0)
  AND payment_type NOT IN ('credit_card','boleto','voucher','debit_card','not_defined');

-- Reject: payment_installments < 1
INSERT INTO reject_olist_order_payments_dataset
    (order_id, payment_sequential, payment_type,
     payment_installments, payment_value, reject_reason)
SELECT order_id, payment_sequential, payment_type,
       payment_installments, payment_value, 'INVALID_INSTALLMENTS_LESS_THAN_ONE'
FROM raw_olist_order_payments_dataset
WHERE order_id IS NOT NULL
  AND payment_value IS NOT NULL AND payment_value > 0
  AND payment_type IN ('credit_card','boleto','voucher','debit_card','not_defined')
  AND (payment_installments IS NULL OR payment_installments < 1);

-- Reject: Exact duplicate rows
INSERT INTO reject_olist_order_payments_dataset
    (order_id, payment_sequential, payment_type,
     payment_installments, payment_value, reject_reason)
SELECT order_id, payment_sequential, payment_type,
       payment_installments, payment_value,
       CONCAT('EXACT_DUPLICATE_ROW (', COUNT(*) - 1, ' extra copies removed)')
FROM raw_olist_order_payments_dataset
WHERE order_id IS NOT NULL
  AND payment_value IS NOT NULL AND payment_value > 0
  AND payment_type IN ('credit_card','boleto','voucher','debit_card','not_defined')
  AND payment_installments IS NOT NULL AND payment_installments >= 1
GROUP BY order_id, payment_sequential, payment_type,
         payment_installments, payment_value
HAVING COUNT(*) > 1;

-- Cleaned: ALL valid distinct rows; composite key (order_id, payment_sequential) for dup detection
INSERT INTO cleaned_olist_order_payments_dataset
    (order_id, payment_sequential, payment_type,
     payment_installments, payment_value, duplicate_flag)
SELECT
    order_id, payment_sequential, payment_type,
    payment_installments, payment_value,
    CASE WHEN COUNT(*) OVER (PARTITION BY order_id, payment_sequential) > 1 THEN 1 ELSE 0 END AS duplicate_flag
FROM (
    SELECT order_id, payment_sequential, payment_type,
           payment_installments, payment_value
    FROM raw_olist_order_payments_dataset
    WHERE order_id IS NOT NULL
      AND payment_value IS NOT NULL AND payment_value > 0
      AND payment_type IN ('credit_card','boleto','voucher','debit_card','not_defined')
      AND payment_installments IS NOT NULL AND payment_installments >= 1
    GROUP BY order_id, payment_sequential, payment_type,
             payment_installments, payment_value
) t;


-- ============================================================
-- TABLE 5: PRODUCTS  (C1)
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
    reject_reason              VARCHAR(300),
    rejected_at                TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS cleaned_olist_products_dataset;
CREATE TABLE cleaned_olist_products_dataset (
    product_id                 VARCHAR(50),
    product_category_name      VARCHAR(120),
    product_name_length        INT,
    product_description_length INT,
    product_photos_qty         INT,
    product_weight_g           DECIMAL(12,2),
    product_length_cm          DECIMAL(10,2),
    product_height_cm          DECIMAL(10,2),
    product_width_cm           DECIMAL(10,2),
    category_missing_flag      TINYINT DEFAULT 0,
    dimensions_missing_flag    TINYINT DEFAULT 0,
    duplicate_flag             TINYINT DEFAULT 0,
    cleaned_at                 TIMESTAMP  DEFAULT CURRENT_TIMESTAMP
);

-- Reject: NULL product_id
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

-- Reject: Exact duplicate rows
INSERT INTO reject_olist_products_dataset
    (product_id, product_category_name, product_name_length,
     product_description_length, product_photos_qty,
     product_weight_g, product_length_cm, product_height_cm, product_width_cm,
     reject_reason)
SELECT product_id, product_category_name, product_name_length,
       product_description_length, product_photos_qty,
       product_weight_g, product_length_cm, product_height_cm, product_width_cm,
       CONCAT('EXACT_DUPLICATE_ROW (', COUNT(*) - 1, ' extra copies removed)')
FROM raw_olist_products_dataset
WHERE product_id IS NOT NULL
GROUP BY product_id, product_category_name, product_name_length,
         product_description_length, product_photos_qty,
         product_weight_g, product_length_cm, product_height_cm, product_width_cm
HAVING COUNT(*) > 1;

-- Cleaned: ALL valid distinct rows; duplicate_flag for repeated product_id
INSERT INTO cleaned_olist_products_dataset
    (product_id, product_category_name, product_name_length,
     product_description_length, product_photos_qty,
     product_weight_g, product_length_cm, product_height_cm, product_width_cm,
     category_missing_flag, dimensions_missing_flag, duplicate_flag)
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
          OR  product_width_cm  IS NULL THEN 1 ELSE 0 END     AS dimensions_missing_flag,
    CASE WHEN COUNT(*) OVER (PARTITION BY product_id) > 1 THEN 1 ELSE 0 END AS duplicate_flag
FROM (
    SELECT product_id, product_category_name, product_name_length,
           product_description_length, product_photos_qty,
           product_weight_g, product_length_cm, product_height_cm, product_width_cm
    FROM raw_olist_products_dataset
    WHERE product_id IS NOT NULL
    GROUP BY product_id, product_category_name, product_name_length,
             product_description_length, product_photos_qty,
             product_weight_g, product_length_cm, product_height_cm, product_width_cm
) t;


-- ============================================================
-- TABLE 6: SELLERS  (C1)
-- ============================================================

DROP TABLE IF EXISTS reject_olist_sellers_dataset;
CREATE TABLE reject_olist_sellers_dataset (
    seller_id              VARCHAR(50),
    seller_zip_code_prefix INT,
    seller_city            VARCHAR(120),
    seller_state           VARCHAR(5),
    reject_reason          VARCHAR(300),
    rejected_at            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS cleaned_olist_sellers_dataset;
CREATE TABLE cleaned_olist_sellers_dataset (
    seller_id              VARCHAR(50),
    seller_zip_code_prefix INT,
    seller_city            VARCHAR(120),
    seller_state           VARCHAR(5),
    duplicate_flag         TINYINT DEFAULT 0,
    cleaned_at             TIMESTAMP  DEFAULT CURRENT_TIMESTAMP
);

-- Reject: NULL seller_id
INSERT INTO reject_olist_sellers_dataset
    (seller_id, seller_zip_code_prefix, seller_city, seller_state, reject_reason)
SELECT seller_id, seller_zip_code_prefix, seller_city, seller_state, 'NULL_SELLER_ID'
FROM raw_olist_sellers_dataset
WHERE seller_id IS NULL;

-- Reject: Exact duplicate rows
INSERT INTO reject_olist_sellers_dataset
    (seller_id, seller_zip_code_prefix, seller_city, seller_state, reject_reason)
SELECT seller_id, seller_zip_code_prefix, seller_city, seller_state,
       CONCAT('EXACT_DUPLICATE_ROW (', COUNT(*) - 1, ' extra copies removed)')
FROM raw_olist_sellers_dataset
WHERE seller_id IS NOT NULL
GROUP BY seller_id, seller_zip_code_prefix, seller_city, seller_state
HAVING COUNT(*) > 1;

-- Cleaned: ALL valid distinct rows
INSERT INTO cleaned_olist_sellers_dataset
    (seller_id, seller_zip_code_prefix, seller_city, seller_state, duplicate_flag)
SELECT
    seller_id,
    seller_zip_code_prefix,
    LOWER(TRIM(seller_city))  AS seller_city,
    UPPER(TRIM(seller_state)) AS seller_state,
    CASE WHEN COUNT(*) OVER (PARTITION BY seller_id) > 1 THEN 1 ELSE 0 END AS duplicate_flag
FROM (
    SELECT seller_id, seller_zip_code_prefix, seller_city, seller_state
    FROM raw_olist_sellers_dataset
    WHERE seller_id IS NOT NULL
    GROUP BY seller_id, seller_zip_code_prefix, seller_city, seller_state
) t;


-- ============================================================
-- TABLE 7: ORDER REVIEWS  (C1, C5)
-- ============================================================
-- C5: Composite key = (review_id, order_id)
--     Same review_id for different order_ids is VALID

DROP TABLE IF EXISTS reject_olist_order_reviews_dataset;
CREATE TABLE reject_olist_order_reviews_dataset (
    review_id               VARCHAR(50),
    order_id                VARCHAR(50),
    review_score            INT,
    review_comment_title    VARCHAR(255),
    review_comment_message  TEXT,
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    reject_reason           VARCHAR(300),
    rejected_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS cleaned_olist_order_reviews_dataset;
CREATE TABLE cleaned_olist_order_reviews_dataset (
    review_id               VARCHAR(50),
    order_id                VARCHAR(50),
    review_score            INT,
    review_comment_title    VARCHAR(255),
    review_comment_message  TEXT,
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP,
    duplicate_flag          TINYINT DEFAULT 0,
    cleaned_at              TIMESTAMP  DEFAULT CURRENT_TIMESTAMP
);

-- Reject: NULL review_id
INSERT INTO reject_olist_order_reviews_dataset
    (review_id, order_id, review_score, review_comment_title,
     review_comment_message, review_creation_date, review_answer_timestamp, reject_reason)
SELECT review_id, order_id, review_score, review_comment_title,
       review_comment_message, review_creation_date, review_answer_timestamp,
       'NULL_REVIEW_ID'
FROM raw_olist_order_reviews_dataset
WHERE review_id IS NULL;

-- Reject: invalid review_score
INSERT INTO reject_olist_order_reviews_dataset
    (review_id, order_id, review_score, review_comment_title,
     review_comment_message, review_creation_date, review_answer_timestamp, reject_reason)
SELECT review_id, order_id, review_score, review_comment_title,
       review_comment_message, review_creation_date, review_answer_timestamp,
       'INVALID_REVIEW_SCORE_OUT_OF_RANGE'
FROM raw_olist_order_reviews_dataset
WHERE review_id IS NOT NULL
  AND (review_score IS NULL OR review_score NOT BETWEEN 1 AND 5);

-- Reject: Exact duplicate rows (all 7 columns)
INSERT INTO reject_olist_order_reviews_dataset
    (review_id, order_id, review_score, review_comment_title,
     review_comment_message, review_creation_date, review_answer_timestamp, reject_reason)
SELECT review_id, order_id, review_score, review_comment_title,
       review_comment_message, review_creation_date, review_answer_timestamp,
       CONCAT('EXACT_DUPLICATE_ROW (', COUNT(*) - 1, ' extra copies removed)')
FROM raw_olist_order_reviews_dataset
WHERE review_id IS NOT NULL
  AND review_score BETWEEN 1 AND 5
GROUP BY review_id, order_id, review_score, review_comment_title,
         review_comment_message, review_creation_date, review_answer_timestamp
HAVING COUNT(*) > 1;

-- Cleaned: ALL valid distinct rows; composite key (review_id, order_id) for dup detection
INSERT INTO cleaned_olist_order_reviews_dataset
    (review_id, order_id, review_score, review_comment_title,
     review_comment_message, review_creation_date, review_answer_timestamp,
     duplicate_flag)
SELECT
    review_id,
    order_id,
    review_score,
    TRIM(review_comment_title)   AS review_comment_title,
    TRIM(review_comment_message) AS review_comment_message,
    review_creation_date,
    review_answer_timestamp,
    CASE WHEN COUNT(*) OVER (PARTITION BY review_id, order_id) > 1 THEN 1 ELSE 0 END AS duplicate_flag
FROM (
    SELECT review_id, order_id, review_score, review_comment_title,
           review_comment_message, review_creation_date, review_answer_timestamp
    FROM raw_olist_order_reviews_dataset
    WHERE review_id IS NOT NULL
      AND review_score BETWEEN 1 AND 5
    GROUP BY review_id, order_id, review_score, review_comment_title,
             review_comment_message, review_creation_date, review_answer_timestamp
) t;


-- ============================================================
-- TABLE 8: GEOLOCATION  (C4)
-- ============================================================
-- C4: Out-of-bounds coordinates → is_out_of_bounds = 1 (NOT rejected)
--     Only exact duplicate rows are rejected.

DROP TABLE IF EXISTS reject_olist_geolocation_dataset;
CREATE TABLE reject_olist_geolocation_dataset (
    geolocation_zip_code_prefix INT,
    geolocation_lat             DECIMAL(18,8),
    geolocation_lng             DECIMAL(18,8),
    geolocation_city            VARCHAR(120),
    geolocation_state           VARCHAR(5),
    reject_reason               VARCHAR(300),
    rejected_at                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS cleaned_olist_geolocation_dataset;
CREATE TABLE cleaned_olist_geolocation_dataset (
    geolocation_zip_code_prefix INT,
    geolocation_lat             DECIMAL(18,8),
    geolocation_lng             DECIMAL(18,8),
    geolocation_city            VARCHAR(120),
    geolocation_state           VARCHAR(5),
    is_out_of_bounds            TINYINT DEFAULT 0,
    cleaned_at                  TIMESTAMP  DEFAULT CURRENT_TIMESTAMP
);

-- Reject: Exact duplicate rows only
INSERT INTO reject_olist_geolocation_dataset
    (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
     geolocation_city, geolocation_state, reject_reason)
SELECT geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
       geolocation_city, geolocation_state,
       CONCAT('EXACT_DUPLICATE_ROW (', COUNT(*) - 1, ' extra copies removed)')
FROM raw_olist_geolocation_dataset
GROUP BY geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
         geolocation_city, geolocation_state
HAVING COUNT(*) > 1;

-- Cleaned: ALL distinct rows; out-of-bounds FLAGGED (not rejected)
INSERT INTO cleaned_olist_geolocation_dataset
    (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
     geolocation_city, geolocation_state, is_out_of_bounds)
SELECT
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    LOWER(TRIM(geolocation_city))  AS geolocation_city,
    UPPER(TRIM(geolocation_state)) AS geolocation_state,
    CASE
        WHEN geolocation_lat NOT BETWEEN -33.75 AND 5.27
          OR geolocation_lng NOT BETWEEN -73.99 AND -34.79
        THEN 1 ELSE 0
    END AS is_out_of_bounds
FROM (
    SELECT geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
           geolocation_city, geolocation_state
    FROM raw_olist_geolocation_dataset
    GROUP BY geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
             geolocation_city, geolocation_state
) t;


-- ============================================================
-- TABLE 9: PRODUCT CATEGORY NAME TRANSLATION  (C1)
-- ============================================================

DROP TABLE IF EXISTS reject_product_category_name_translation;
CREATE TABLE reject_product_category_name_translation (
    product_category_name         VARCHAR(120),
    product_category_name_english VARCHAR(120),
    reject_reason                 VARCHAR(300),
    rejected_at                   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS cleaned_product_category_name_translation;
CREATE TABLE cleaned_product_category_name_translation (
    product_category_name         VARCHAR(120),
    product_category_name_english VARCHAR(120),
    duplicate_flag                TINYINT DEFAULT 0,
    cleaned_at                    TIMESTAMP  DEFAULT CURRENT_TIMESTAMP
);

-- Reject: NULL Portuguese category name
INSERT INTO reject_product_category_name_translation
    (product_category_name, product_category_name_english, reject_reason)
SELECT product_category_name, product_category_name_english, 'NULL_CATEGORY_NAME'
FROM raw_product_category_name_translation
WHERE product_category_name IS NULL;

-- Reject: Exact duplicate rows
INSERT INTO reject_product_category_name_translation
    (product_category_name, product_category_name_english, reject_reason)
SELECT product_category_name, product_category_name_english,
       CONCAT('EXACT_DUPLICATE_ROW (', COUNT(*) - 1, ' extra copies removed)')
FROM raw_product_category_name_translation
WHERE product_category_name IS NOT NULL
GROUP BY product_category_name, product_category_name_english
HAVING COUNT(*) > 1;

-- Cleaned: ALL valid distinct rows; flag duplicate Portuguese names
INSERT INTO cleaned_product_category_name_translation
    (product_category_name, product_category_name_english, duplicate_flag)
SELECT
    LOWER(TRIM(product_category_name))         AS product_category_name,
    LOWER(TRIM(product_category_name_english)) AS product_category_name_english,
    CASE WHEN COUNT(*) OVER (PARTITION BY LOWER(TRIM(product_category_name))) > 1 THEN 1 ELSE 0 END AS duplicate_flag
FROM (
    SELECT product_category_name, product_category_name_english
    FROM raw_product_category_name_translation
    WHERE product_category_name IS NOT NULL
    GROUP BY product_category_name, product_category_name_english
) t;


-- ============================================================
-- POST-CORRECTION VERIFICATION
-- ============================================================

SELECT
    'cleaned_olist_customers_dataset'            AS table_name,
    COUNT(*)                                     AS cleaned_rows,
    SUM(duplicate_flag)                          AS flagged_duplicates,
    (SELECT COUNT(*) FROM reject_olist_customers_dataset) AS rejected_rows
FROM cleaned_olist_customers_dataset
UNION ALL
SELECT
    'cleaned_olist_orders_dataset',
    COUNT(*),
    SUM(duplicate_flag),
    (SELECT COUNT(*) FROM reject_olist_orders_dataset)
FROM cleaned_olist_orders_dataset
UNION ALL
SELECT
    'cleaned_olist_order_items_dataset',
    COUNT(*),
    SUM(duplicate_flag),
    (SELECT COUNT(*) FROM reject_olist_order_items_dataset)
FROM cleaned_olist_order_items_dataset
UNION ALL
SELECT
    'cleaned_olist_order_payments_dataset',
    COUNT(*),
    SUM(duplicate_flag),
    (SELECT COUNT(*) FROM reject_olist_order_payments_dataset)
FROM cleaned_olist_order_payments_dataset
UNION ALL
SELECT
    'cleaned_olist_products_dataset',
    COUNT(*),
    SUM(duplicate_flag),
    (SELECT COUNT(*) FROM reject_olist_products_dataset)
FROM cleaned_olist_products_dataset
UNION ALL
SELECT
    'cleaned_olist_sellers_dataset',
    COUNT(*),
    SUM(duplicate_flag),
    (SELECT COUNT(*) FROM reject_olist_sellers_dataset)
FROM cleaned_olist_sellers_dataset
UNION ALL
SELECT
    'cleaned_olist_order_reviews_dataset',
    COUNT(*),
    SUM(duplicate_flag),
    (SELECT COUNT(*) FROM reject_olist_order_reviews_dataset)
FROM cleaned_olist_order_reviews_dataset
UNION ALL
SELECT
    'cleaned_olist_geolocation_dataset',
    COUNT(*),
    SUM(is_out_of_bounds),
    (SELECT COUNT(*) FROM reject_olist_geolocation_dataset)
FROM cleaned_olist_geolocation_dataset
UNION ALL
SELECT
    'cleaned_product_category_name_translation',
    COUNT(*),
    SUM(duplicate_flag),
    (SELECT COUNT(*) FROM reject_product_category_name_translation)
FROM cleaned_product_category_name_translation;

-- ============================================================
-- END OF STAGE 1 CORRECTIONS
-- Next step: Run stage2_01_fact_order_master.sql
-- ============================================================
