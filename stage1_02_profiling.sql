-- ============================================================
-- OLIST BRAZILIAN E-COMMERCE ANALYTICS
-- STAGE 1 | FILE 2: DATA PROFILING (ALL 9 TABLES)
-- Environment : MySQL 8+
-- Run After   : stage1_01_setup_and_load.sql
-- ============================================================
-- PURPOSE: Profile every column in every raw table.
--          Output includes:
--            - Total row count
--            - NULL count per column
--            - Distinct value count per column
--            - Duplicate row detection
--            - Data range / business rule checks
-- ============================================================

-- USE olist_ecommerce;

-- ============================================================
-- TABLE 1: raw_olist_customers_dataset
-- ============================================================

-- 1.A  Row count
SELECT
    'raw_olist_customers_dataset'   AS table_name,
    COUNT(*)                        AS total_rows
FROM raw_olist_customers_dataset;

-- 1.B  NULL count per column
SELECT
    SUM(CASE WHEN customer_id              IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
    SUM(CASE WHEN customer_unique_id       IS NULL THEN 1 ELSE 0 END) AS null_customer_unique_id,
    SUM(CASE WHEN customer_zip_code_prefix IS NULL THEN 1 ELSE 0 END) AS null_customer_zip_code_prefix,
    SUM(CASE WHEN customer_city            IS NULL THEN 1 ELSE 0 END) AS null_customer_city,
    SUM(CASE WHEN customer_state           IS NULL THEN 1 ELSE 0 END) AS null_customer_state
FROM raw_olist_customers_dataset;

-- 1.C  Distinct value count per column
SELECT
    COUNT(DISTINCT customer_id)              AS distinct_customer_id,
    COUNT(DISTINCT customer_unique_id)       AS distinct_customer_unique_id,
    COUNT(DISTINCT customer_zip_code_prefix) AS distinct_zip_codes,
    COUNT(DISTINCT customer_city)            AS distinct_cities,
    COUNT(DISTINCT customer_state)           AS distinct_states
FROM raw_olist_customers_dataset;

-- 1.D  Duplicate customer_id detection
SELECT
    customer_id,
    COUNT(*) AS occurrences
FROM raw_olist_customers_dataset
GROUP BY customer_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC
LIMIT 20;

-- 1.E  Exact duplicate rows (all 5 columns)
SELECT COUNT(*) AS exact_duplicate_rows
FROM (
    SELECT
        customer_id, customer_unique_id, customer_zip_code_prefix,
        customer_city, customer_state,
        COUNT(*) AS cnt
    FROM raw_olist_customers_dataset
    GROUP BY customer_id, customer_unique_id, customer_zip_code_prefix,
             customer_city, customer_state
    HAVING COUNT(*) > 1
) t;

-- 1.F  State distribution (should be 27 Brazilian states)
SELECT customer_state, COUNT(*) AS cnt
FROM raw_olist_customers_dataset
GROUP BY customer_state
ORDER BY cnt DESC;

-- 1.G  Zip code range check (Brazil: 01000 – 99999)
SELECT
    MIN(customer_zip_code_prefix) AS min_zip,
    MAX(customer_zip_code_prefix) AS max_zip,
    SUM(CASE WHEN customer_zip_code_prefix < 1000
              OR  customer_zip_code_prefix > 99999 THEN 1 ELSE 0 END) AS out_of_range_zip
FROM raw_olist_customers_dataset;


-- ============================================================
-- TABLE 2: raw_olist_orders_dataset
-- ============================================================

-- 2.A  Row count
SELECT
    'raw_olist_orders_dataset'  AS table_name,
    COUNT(*)                    AS total_rows
FROM raw_olist_orders_dataset;

-- 2.B  NULL count per column
SELECT
    SUM(CASE WHEN order_id                       IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN customer_id                    IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
    SUM(CASE WHEN order_status                   IS NULL THEN 1 ELSE 0 END) AS null_order_status,
    SUM(CASE WHEN order_purchase_timestamp       IS NULL THEN 1 ELSE 0 END) AS null_purchase_ts,
    SUM(CASE WHEN order_approved_at              IS NULL THEN 1 ELSE 0 END) AS null_approved_at,
    SUM(CASE WHEN order_delivered_carrier_date   IS NULL THEN 1 ELSE 0 END) AS null_carrier_date,
    SUM(CASE WHEN order_delivered_customer_date  IS NULL THEN 1 ELSE 0 END) AS null_customer_date,
    SUM(CASE WHEN order_estimated_delivery_date  IS NULL THEN 1 ELSE 0 END) AS null_estimated_date
FROM raw_olist_orders_dataset;

-- 2.C  Distinct values per key column
SELECT
    COUNT(DISTINCT order_id)     AS distinct_order_ids,
    COUNT(DISTINCT customer_id)  AS distinct_customers,
    COUNT(DISTINCT order_status) AS distinct_statuses
FROM raw_olist_orders_dataset;

-- 2.D  Duplicate order_id detection
SELECT order_id, COUNT(*) AS occurrences
FROM raw_olist_orders_dataset
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC
LIMIT 20;

-- 2.E  Exact duplicate rows
SELECT COUNT(*) AS exact_duplicate_rows
FROM (
    SELECT
        order_id, customer_id, order_status,
        order_purchase_timestamp, order_approved_at,
        order_delivered_carrier_date, order_delivered_customer_date,
        order_estimated_delivery_date,
        COUNT(*) AS cnt
    FROM raw_olist_orders_dataset
    GROUP BY order_id, customer_id, order_status,
             order_purchase_timestamp, order_approved_at,
             order_delivered_carrier_date, order_delivered_customer_date,
             order_estimated_delivery_date
    HAVING COUNT(*) > 1
) t;

-- 2.F  Order status distribution
SELECT order_status, COUNT(*) AS cnt
FROM raw_olist_orders_dataset
GROUP BY order_status
ORDER BY cnt DESC;

-- 2.G  Timestamp sequence violations (only where both timestamps are NOT NULL)
--      Violation 1: purchase > approval
SELECT COUNT(*) AS purchase_after_approval
FROM raw_olist_orders_dataset
WHERE order_purchase_timestamp  IS NOT NULL
  AND order_approved_at         IS NOT NULL
  AND order_purchase_timestamp  > order_approved_at;

--      Violation 2: approval > carrier delivery
SELECT COUNT(*) AS approval_after_carrier_delivery
FROM raw_olist_orders_dataset
WHERE order_approved_at             IS NOT NULL
  AND order_delivered_carrier_date  IS NOT NULL
  AND order_approved_at             > order_delivered_carrier_date;

--      Violation 3: carrier > customer delivery
SELECT COUNT(*) AS carrier_after_customer_delivery
FROM raw_olist_orders_dataset
WHERE order_delivered_carrier_date  IS NOT NULL
  AND order_delivered_customer_date IS NOT NULL
  AND order_delivered_carrier_date  > order_delivered_customer_date;

-- 2.H  Timestamp range (min / max per field)
SELECT
    MIN(order_purchase_timestamp)      AS min_purchase,
    MAX(order_purchase_timestamp)      AS max_purchase,
    MIN(order_approved_at)             AS min_approved,
    MAX(order_approved_at)             AS max_approved,
    MIN(order_delivered_carrier_date)  AS min_carrier,
    MAX(order_delivered_carrier_date)  AS max_carrier,
    MIN(order_delivered_customer_date) AS min_customer_del,
    MAX(order_delivered_customer_date) AS max_customer_del,
    MIN(order_estimated_delivery_date) AS min_estimated,
    MAX(order_estimated_delivery_date) AS max_estimated
FROM raw_olist_orders_dataset;


-- ============================================================
-- TABLE 3: raw_olist_order_items_dataset
-- ============================================================

-- 3.A  Row count
SELECT 'raw_olist_order_items_dataset' AS table_name, COUNT(*) AS total_rows
FROM raw_olist_order_items_dataset;

-- 3.B  NULL count per column
SELECT
    SUM(CASE WHEN order_id            IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN order_item_id       IS NULL THEN 1 ELSE 0 END) AS null_order_item_id,
    SUM(CASE WHEN product_id          IS NULL THEN 1 ELSE 0 END) AS null_product_id,
    SUM(CASE WHEN seller_id           IS NULL THEN 1 ELSE 0 END) AS null_seller_id,
    SUM(CASE WHEN shipping_limit_date IS NULL THEN 1 ELSE 0 END) AS null_shipping_limit_date,
    SUM(CASE WHEN price               IS NULL THEN 1 ELSE 0 END) AS null_price,
    SUM(CASE WHEN freight_value       IS NULL THEN 1 ELSE 0 END) AS null_freight_value
FROM raw_olist_order_items_dataset;

-- 3.C  Distinct key counts
SELECT
    COUNT(DISTINCT order_id)   AS distinct_orders,
    COUNT(DISTINCT product_id) AS distinct_products,
    COUNT(DISTINCT seller_id)  AS distinct_sellers
FROM raw_olist_order_items_dataset;

-- 3.D  Composite key duplicate check (order_id + order_item_id)
SELECT order_id, order_item_id, COUNT(*) AS occurrences
FROM raw_olist_order_items_dataset
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC
LIMIT 20;

-- 3.E  Exact duplicate rows
SELECT COUNT(*) AS exact_duplicate_rows
FROM (
    SELECT
        order_id, order_item_id, product_id, seller_id,
        shipping_limit_date, price, freight_value,
        COUNT(*) AS cnt
    FROM raw_olist_order_items_dataset
    GROUP BY order_id, order_item_id, product_id, seller_id,
             shipping_limit_date, price, freight_value
    HAVING COUNT(*) > 1
) t;

-- 3.F  Price business rule checks
SELECT
    COUNT(*)                                                              AS total_rows,
    SUM(CASE WHEN price IS NULL      THEN 1 ELSE 0 END)                  AS null_price,
    SUM(CASE WHEN price <= 0         THEN 1 ELSE 0 END)                  AS price_zero_or_negative,
    MIN(price)                                                            AS min_price,
    MAX(price)                                                            AS max_price,
    ROUND(AVG(price), 2)                                                  AS avg_price
FROM raw_olist_order_items_dataset;

-- 3.G  Freight value checks
SELECT
    SUM(CASE WHEN freight_value IS NULL THEN 1 ELSE 0 END) AS null_freight,
    SUM(CASE WHEN freight_value < 0     THEN 1 ELSE 0 END) AS negative_freight,
    MIN(freight_value)                                      AS min_freight,
    MAX(freight_value)                                      AS max_freight,
    ROUND(AVG(freight_value), 2)                            AS avg_freight
FROM raw_olist_order_items_dataset;

-- 3.H  Items per order distribution
SELECT
    MAX(order_item_id) AS max_items_in_one_order,
    AVG(order_item_id) AS avg_items_per_order
FROM raw_olist_order_items_dataset;


-- ============================================================
-- TABLE 4: raw_olist_order_payments_dataset
-- ============================================================

-- 4.A  Row count
SELECT 'raw_olist_order_payments_dataset' AS table_name, COUNT(*) AS total_rows
FROM raw_olist_order_payments_dataset;

-- 4.B  NULL count per column
SELECT
    SUM(CASE WHEN order_id             IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN payment_sequential   IS NULL THEN 1 ELSE 0 END) AS null_payment_sequential,
    SUM(CASE WHEN payment_type         IS NULL THEN 1 ELSE 0 END) AS null_payment_type,
    SUM(CASE WHEN payment_installments IS NULL THEN 1 ELSE 0 END) AS null_payment_installments,
    SUM(CASE WHEN payment_value        IS NULL THEN 1 ELSE 0 END) AS null_payment_value
FROM raw_olist_order_payments_dataset;

-- 4.C  Distinct key counts
SELECT
    COUNT(DISTINCT order_id)      AS distinct_orders,
    COUNT(DISTINCT payment_type)  AS distinct_payment_types
FROM raw_olist_order_payments_dataset;

-- 4.D  Composite key duplicate check (order_id + payment_sequential)
SELECT order_id, payment_sequential, COUNT(*) AS occurrences
FROM raw_olist_order_payments_dataset
GROUP BY order_id, payment_sequential
HAVING COUNT(*) > 1
ORDER BY occurrences DESC
LIMIT 20;

-- 4.E  Exact duplicate rows
SELECT COUNT(*) AS exact_duplicate_rows
FROM (
    SELECT
        order_id, payment_sequential, payment_type,
        payment_installments, payment_value, COUNT(*) AS cnt
    FROM raw_olist_order_payments_dataset
    GROUP BY order_id, payment_sequential, payment_type,
             payment_installments, payment_value
    HAVING COUNT(*) > 1
) t;

-- 4.F  Payment type distribution
SELECT payment_type, COUNT(*) AS cnt
FROM raw_olist_order_payments_dataset
GROUP BY payment_type
ORDER BY cnt DESC;

-- 4.G  Payment value checks
SELECT
    SUM(CASE WHEN payment_value <= 0  THEN 1 ELSE 0 END) AS payment_value_zero_or_negative,
    MIN(payment_value)                                     AS min_payment_value,
    MAX(payment_value)                                     AS max_payment_value,
    ROUND(AVG(payment_value), 2)                           AS avg_payment_value
FROM raw_olist_order_payments_dataset;

-- 4.H  Installment checks
SELECT
    SUM(CASE WHEN payment_installments < 1 THEN 1 ELSE 0 END) AS invalid_installments,
    MIN(payment_installments)                                   AS min_installments,
    MAX(payment_installments)                                   AS max_installments
FROM raw_olist_order_payments_dataset;

-- 4.I  Multiple payment methods per single order
SELECT
    COUNT(DISTINCT order_id) AS orders_with_multiple_payments
FROM (
    SELECT order_id, COUNT(*) AS payment_count
    FROM raw_olist_order_payments_dataset
    GROUP BY order_id
    HAVING COUNT(*) > 1
) t;


-- ============================================================
-- TABLE 5: raw_olist_products_dataset
-- ============================================================

-- 5.A  Row count
SELECT 'raw_olist_products_dataset' AS table_name, COUNT(*) AS total_rows
FROM raw_olist_products_dataset;

-- 5.B  NULL count per column
SELECT
    SUM(CASE WHEN product_id                 IS NULL THEN 1 ELSE 0 END) AS null_product_id,
    SUM(CASE WHEN product_category_name      IS NULL THEN 1 ELSE 0 END) AS null_category_name,
    SUM(CASE WHEN product_name_length        IS NULL THEN 1 ELSE 0 END) AS null_name_length,
    SUM(CASE WHEN product_description_length IS NULL THEN 1 ELSE 0 END) AS null_desc_length,
    SUM(CASE WHEN product_photos_qty         IS NULL THEN 1 ELSE 0 END) AS null_photos_qty,
    SUM(CASE WHEN product_weight_g           IS NULL THEN 1 ELSE 0 END) AS null_weight_g,
    SUM(CASE WHEN product_length_cm          IS NULL THEN 1 ELSE 0 END) AS null_length_cm,
    SUM(CASE WHEN product_height_cm          IS NULL THEN 1 ELSE 0 END) AS null_height_cm,
    SUM(CASE WHEN product_width_cm           IS NULL THEN 1 ELSE 0 END) AS null_width_cm
FROM raw_olist_products_dataset;

-- 5.C  Distinct product count
SELECT COUNT(DISTINCT product_id) AS distinct_product_ids
FROM raw_olist_products_dataset;

-- 5.D  Duplicate product_id detection
SELECT product_id, COUNT(*) AS occurrences
FROM raw_olist_products_dataset
GROUP BY product_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC
LIMIT 20;

-- 5.E  Exact duplicate rows
SELECT COUNT(*) AS exact_duplicate_rows
FROM (
    SELECT
        product_id, product_category_name, product_name_length,
        product_description_length, product_photos_qty,
        product_weight_g, product_length_cm, product_height_cm,
        product_width_cm, COUNT(*) AS cnt
    FROM raw_olist_products_dataset
    GROUP BY product_id, product_category_name, product_name_length,
             product_description_length, product_photos_qty,
             product_weight_g, product_length_cm, product_height_cm,
             product_width_cm
    HAVING COUNT(*) > 1
) t;

-- 5.F  Category name distribution (top 20)
SELECT product_category_name, COUNT(*) AS cnt
FROM raw_olist_products_dataset
GROUP BY product_category_name
ORDER BY cnt DESC
LIMIT 20;

-- 5.G  Dimension / weight range checks
SELECT
    MIN(product_weight_g)  AS min_weight_g,  MAX(product_weight_g)  AS max_weight_g,
    MIN(product_length_cm) AS min_length_cm, MAX(product_length_cm) AS max_length_cm,
    MIN(product_height_cm) AS min_height_cm, MAX(product_height_cm) AS max_height_cm,
    MIN(product_width_cm)  AS min_width_cm,  MAX(product_width_cm)  AS max_width_cm,
    SUM(CASE WHEN product_weight_g  <= 0 THEN 1 ELSE 0 END) AS zero_neg_weight,
    SUM(CASE WHEN product_length_cm <= 0 THEN 1 ELSE 0 END) AS zero_neg_length,
    SUM(CASE WHEN product_height_cm <= 0 THEN 1 ELSE 0 END) AS zero_neg_height,
    SUM(CASE WHEN product_width_cm  <= 0 THEN 1 ELSE 0 END) AS zero_neg_width
FROM raw_olist_products_dataset;

-- 5.H  Text length checks
SELECT
    MIN(product_name_length)        AS min_name_len,
    MAX(product_name_length)        AS max_name_len,
    MIN(product_description_length) AS min_desc_len,
    MAX(product_description_length) AS max_desc_len,
    MIN(product_photos_qty)         AS min_photos,
    MAX(product_photos_qty)         AS max_photos
FROM raw_olist_products_dataset;


-- ============================================================
-- TABLE 6: raw_olist_sellers_dataset
-- ============================================================

-- 6.A  Row count
SELECT 'raw_olist_sellers_dataset' AS table_name, COUNT(*) AS total_rows
FROM raw_olist_sellers_dataset;

-- 6.B  NULL count per column
SELECT
    SUM(CASE WHEN seller_id              IS NULL THEN 1 ELSE 0 END) AS null_seller_id,
    SUM(CASE WHEN seller_zip_code_prefix IS NULL THEN 1 ELSE 0 END) AS null_zip_code,
    SUM(CASE WHEN seller_city            IS NULL THEN 1 ELSE 0 END) AS null_city,
    SUM(CASE WHEN seller_state           IS NULL THEN 1 ELSE 0 END) AS null_state
FROM raw_olist_sellers_dataset;

-- 6.C  Distinct seller count
SELECT COUNT(DISTINCT seller_id) AS distinct_seller_ids
FROM raw_olist_sellers_dataset;

-- 6.D  Duplicate seller_id detection
SELECT seller_id, COUNT(*) AS occurrences
FROM raw_olist_sellers_dataset
GROUP BY seller_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC
LIMIT 20;

-- 6.E  Exact duplicate rows
SELECT COUNT(*) AS exact_duplicate_rows
FROM (
    SELECT seller_id, seller_zip_code_prefix, seller_city, seller_state,
           COUNT(*) AS cnt
    FROM raw_olist_sellers_dataset
    GROUP BY seller_id, seller_zip_code_prefix, seller_city, seller_state
    HAVING COUNT(*) > 1
) t;

-- 6.F  State distribution
SELECT seller_state, COUNT(*) AS cnt
FROM raw_olist_sellers_dataset
GROUP BY seller_state
ORDER BY cnt DESC;

-- 6.G  Zip code range check
SELECT
    MIN(seller_zip_code_prefix)  AS min_zip,
    MAX(seller_zip_code_prefix)  AS max_zip,
    SUM(CASE WHEN seller_zip_code_prefix < 1000
              OR  seller_zip_code_prefix > 99999 THEN 1 ELSE 0 END) AS out_of_range_zip
FROM raw_olist_sellers_dataset;


-- ============================================================
-- TABLE 7: raw_olist_order_reviews_dataset
-- ============================================================

-- 7.A  Row count
SELECT 'raw_olist_order_reviews_dataset' AS table_name, COUNT(*) AS total_rows
FROM raw_olist_order_reviews_dataset;

-- 7.B  NULL count per column
SELECT
    SUM(CASE WHEN review_id               IS NULL THEN 1 ELSE 0 END) AS null_review_id,
    SUM(CASE WHEN order_id                IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN review_score            IS NULL THEN 1 ELSE 0 END) AS null_review_score,
    SUM(CASE WHEN review_comment_title    IS NULL THEN 1 ELSE 0 END) AS null_comment_title,
    SUM(CASE WHEN review_comment_message  IS NULL THEN 1 ELSE 0 END) AS null_comment_message,
    SUM(CASE WHEN review_creation_date    IS NULL THEN 1 ELSE 0 END) AS null_creation_date,
    SUM(CASE WHEN review_answer_timestamp IS NULL THEN 1 ELSE 0 END) AS null_answer_timestamp
FROM raw_olist_order_reviews_dataset;

-- 7.C  Distinct counts
SELECT
    COUNT(DISTINCT review_id) AS distinct_review_ids,
    COUNT(DISTINCT order_id)  AS distinct_order_ids
FROM raw_olist_order_reviews_dataset;

-- 7.D  Duplicate review_id detection (same review_id appearing multiple times)
SELECT review_id, COUNT(*) AS occurrences
FROM raw_olist_order_reviews_dataset
GROUP BY review_id
HAVING COUNT(*) > 1
ORDER BY occurrences DESC
LIMIT 20;

-- 7.E  Exact duplicate rows (all 7 columns)
SELECT COUNT(*) AS exact_duplicate_rows
FROM (
    SELECT
        review_id, order_id, review_score, review_comment_title,
        review_comment_message, review_creation_date, review_answer_timestamp,
        COUNT(*) AS cnt
    FROM raw_olist_order_reviews_dataset
    GROUP BY review_id, order_id, review_score, review_comment_title,
             review_comment_message, review_creation_date, review_answer_timestamp
    HAVING COUNT(*) > 1
) t;

-- 7.F  Review score distribution (valid: 1–5)
SELECT
    review_score,
    COUNT(*) AS cnt
FROM raw_olist_order_reviews_dataset
GROUP BY review_score
ORDER BY review_score;

-- 7.G  Invalid review scores
SELECT COUNT(*) AS invalid_review_scores
FROM raw_olist_order_reviews_dataset
WHERE review_score NOT BETWEEN 1 AND 5
   OR review_score IS NULL;

-- 7.H  Review date range
SELECT
    MIN(review_creation_date)    AS earliest_review,
    MAX(review_creation_date)    AS latest_review,
    MIN(review_answer_timestamp) AS earliest_answer,
    MAX(review_answer_timestamp) AS latest_answer
FROM raw_olist_order_reviews_dataset;


-- ============================================================
-- TABLE 8: raw_olist_geolocation_dataset
-- ============================================================

-- 8.A  Row count
SELECT 'raw_olist_geolocation_dataset' AS table_name, COUNT(*) AS total_rows
FROM raw_olist_geolocation_dataset;

-- 8.B  NULL count per column
SELECT
    SUM(CASE WHEN geolocation_zip_code_prefix IS NULL THEN 1 ELSE 0 END) AS null_zip,
    SUM(CASE WHEN geolocation_lat             IS NULL THEN 1 ELSE 0 END) AS null_lat,
    SUM(CASE WHEN geolocation_lng             IS NULL THEN 1 ELSE 0 END) AS null_lng,
    SUM(CASE WHEN geolocation_city            IS NULL THEN 1 ELSE 0 END) AS null_city,
    SUM(CASE WHEN geolocation_state           IS NULL THEN 1 ELSE 0 END) AS null_state
FROM raw_olist_geolocation_dataset;

-- 8.C  Distinct zip codes (zip codes have multiple lat/lng – this is expected)
SELECT
    COUNT(*)                               AS total_rows,
    COUNT(DISTINCT geolocation_zip_code_prefix) AS distinct_zip_codes
FROM raw_olist_geolocation_dataset;

-- 8.D  Exact duplicate rows (all 5 columns identical)
SELECT COUNT(*) AS exact_duplicate_rows
FROM (
    SELECT
        geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
        geolocation_city, geolocation_state, COUNT(*) AS cnt
    FROM raw_olist_geolocation_dataset
    GROUP BY geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
             geolocation_city, geolocation_state
    HAVING COUNT(*) > 1
) t;

-- 8.E  Latitude bounds check (Brazil: -33.75 to +5.27)
SELECT
    MIN(geolocation_lat) AS min_lat,
    MAX(geolocation_lat) AS max_lat,
    SUM(CASE WHEN geolocation_lat < -33.75
              OR  geolocation_lat >   5.27 THEN 1 ELSE 0 END) AS lat_out_of_brazil
FROM raw_olist_geolocation_dataset;

-- 8.F  Longitude bounds check (Brazil: -73.99 to -34.79)
SELECT
    MIN(geolocation_lng) AS min_lng,
    MAX(geolocation_lng) AS max_lng,
    SUM(CASE WHEN geolocation_lng < -73.99
              OR  geolocation_lng > -34.79 THEN 1 ELSE 0 END) AS lng_out_of_brazil
FROM raw_olist_geolocation_dataset;

-- 8.G  State distribution (should be 27 Brazilian states)
SELECT geolocation_state, COUNT(*) AS cnt
FROM raw_olist_geolocation_dataset
GROUP BY geolocation_state
ORDER BY cnt DESC;

-- 8.H  Zip code range check
SELECT
    MIN(geolocation_zip_code_prefix) AS min_zip,
    MAX(geolocation_zip_code_prefix) AS max_zip,
    SUM(CASE WHEN geolocation_zip_code_prefix < 1000
              OR  geolocation_zip_code_prefix > 99999 THEN 1 ELSE 0 END) AS out_of_range_zip
FROM raw_olist_geolocation_dataset;


-- ============================================================
-- TABLE 9: raw_product_category_name_translation
-- ============================================================

-- 9.A  Row count
SELECT 'raw_product_category_name_translation' AS table_name, COUNT(*) AS total_rows
FROM raw_product_category_name_translation;

-- 9.B  NULL count per column
SELECT
    SUM(CASE WHEN product_category_name         IS NULL THEN 1 ELSE 0 END) AS null_pt_name,
    SUM(CASE WHEN product_category_name_english IS NULL THEN 1 ELSE 0 END) AS null_en_name
FROM raw_product_category_name_translation;

-- 9.C  Distinct counts
SELECT
    COUNT(DISTINCT product_category_name)         AS distinct_pt_names,
    COUNT(DISTINCT product_category_name_english) AS distinct_en_names
FROM raw_product_category_name_translation;

-- 9.D  Duplicate Portuguese category names
SELECT product_category_name, COUNT(*) AS occurrences
FROM raw_product_category_name_translation
GROUP BY product_category_name
HAVING COUNT(*) > 1
ORDER BY occurrences DESC;

-- 9.E  Exact duplicate rows
SELECT COUNT(*) AS exact_duplicate_rows
FROM (
    SELECT product_category_name, product_category_name_english, COUNT(*) AS cnt
    FROM raw_product_category_name_translation
    GROUP BY product_category_name, product_category_name_english
    HAVING COUNT(*) > 1
) t;

-- 9.F  Full list of category name pairs (for manual review)
SELECT product_category_name, product_category_name_english
FROM raw_product_category_name_translation
ORDER BY product_category_name;


-- ============================================================
-- CROSS-TABLE REFERENTIAL INTEGRITY CHECKS (informational)
-- NOTE: These are reference counts ONLY. No joins are performed.
--       Full referential joins will happen in later stages.
-- ============================================================

-- How many distinct customer_ids exist in orders vs customers?
SELECT
    (SELECT COUNT(DISTINCT customer_id) FROM raw_olist_customers_dataset) AS customers_in_customers_table,
    (SELECT COUNT(DISTINCT customer_id) FROM raw_olist_orders_dataset)     AS customers_in_orders_table;

-- How many distinct order_ids exist in each table?
SELECT
    (SELECT COUNT(DISTINCT order_id) FROM raw_olist_orders_dataset)          AS orders_in_orders,
    (SELECT COUNT(DISTINCT order_id) FROM raw_olist_order_items_dataset)     AS orders_in_items,
    (SELECT COUNT(DISTINCT order_id) FROM raw_olist_order_payments_dataset)  AS orders_in_payments,
    (SELECT COUNT(DISTINCT order_id) FROM raw_olist_order_reviews_dataset)   AS orders_in_reviews;

-- How many distinct product_ids in items vs products?
SELECT
    (SELECT COUNT(DISTINCT product_id) FROM raw_olist_products_dataset)      AS products_in_products_table,
    (SELECT COUNT(DISTINCT product_id) FROM raw_olist_order_items_dataset)   AS products_in_items_table;

-- How many distinct seller_ids in items vs sellers?
SELECT
    (SELECT COUNT(DISTINCT seller_id) FROM raw_olist_sellers_dataset)        AS sellers_in_sellers_table,
    (SELECT COUNT(DISTINCT seller_id) FROM raw_olist_order_items_dataset)    AS sellers_in_items_table;

-- ============================================================
-- END OF FILE 2
-- Next step: Run stage1_03_cleaning.sql
-- ============================================================
