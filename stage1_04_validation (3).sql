-- ============================================================
-- OLIST BRAZILIAN E-COMMERCE ANALYTICS
-- STAGE 1 | FILE 4: VALIDATION & STAGE SUMMARY
-- Environment : MySQL 8+
-- Run After   : stage1_03_cleaning.sql
-- ============================================================
-- This file produces:
--   SECTION 1  SQL CODE         (this file itself)
--   SECTION 2  VALIDATION       (before vs after counts, nulls, duplicates)
--   SECTION 3  DATA ISSUES      (query-driven issue summary)
--   SECTION 4  CLEANING ACTIONS (see stage summary at bottom)
--   SECTION 5  OUTPUT TABLES    (list query)
--   SECTION 6  STAGE SUMMARY    (formatted as required)
-- ============================================================

-- USE olist_ecommerce;

-- ============================================================
-- SECTION 2: VALIDATION
-- ============================================================

-- ----------------------------------------------------------------
-- 2.1  ROW COUNTS: Before (raw) vs After (cleaned + rejected)
-- ----------------------------------------------------------------
SELECT
    'olist_customers_dataset'        AS dataset,
    (SELECT COUNT(*) FROM raw_olist_customers_dataset)     AS raw_rows,
    (SELECT COUNT(*) FROM cleaned_olist_customers_dataset) AS cleaned_rows,
    (SELECT COUNT(*) FROM reject_olist_customers_dataset)  AS rejected_rows,
    (SELECT COUNT(*) FROM raw_olist_customers_dataset) -
    (SELECT COUNT(*) FROM cleaned_olist_customers_dataset) -
    (SELECT COUNT(*) FROM reject_olist_customers_dataset)  AS unaccounted_rows
UNION ALL
SELECT
    'olist_orders_dataset',
    (SELECT COUNT(*) FROM raw_olist_orders_dataset),
    (SELECT COUNT(*) FROM cleaned_olist_orders_dataset),
    (SELECT COUNT(*) FROM reject_olist_orders_dataset),
    (SELECT COUNT(*) FROM raw_olist_orders_dataset) -
    (SELECT COUNT(*) FROM cleaned_olist_orders_dataset) -
    (SELECT COUNT(*) FROM reject_olist_orders_dataset)
UNION ALL
SELECT
    'olist_order_items_dataset',
    (SELECT COUNT(*) FROM raw_olist_order_items_dataset),
    (SELECT COUNT(*) FROM cleaned_olist_order_items_dataset),
    (SELECT COUNT(*) FROM reject_olist_order_items_dataset),
    (SELECT COUNT(*) FROM raw_olist_order_items_dataset) -
    (SELECT COUNT(*) FROM cleaned_olist_order_items_dataset) -
    (SELECT COUNT(*) FROM reject_olist_order_items_dataset)
UNION ALL
SELECT
    'olist_order_payments_dataset',
    (SELECT COUNT(*) FROM raw_olist_order_payments_dataset),
    (SELECT COUNT(*) FROM cleaned_olist_order_payments_dataset),
    (SELECT COUNT(*) FROM reject_olist_order_payments_dataset),
    (SELECT COUNT(*) FROM raw_olist_order_payments_dataset) -
    (SELECT COUNT(*) FROM cleaned_olist_order_payments_dataset) -
    (SELECT COUNT(*) FROM reject_olist_order_payments_dataset)
UNION ALL
SELECT
    'olist_products_dataset',
    (SELECT COUNT(*) FROM raw_olist_products_dataset),
    (SELECT COUNT(*) FROM cleaned_olist_products_dataset),
    (SELECT COUNT(*) FROM reject_olist_products_dataset),
    (SELECT COUNT(*) FROM raw_olist_products_dataset) -
    (SELECT COUNT(*) FROM cleaned_olist_products_dataset) -
    (SELECT COUNT(*) FROM reject_olist_products_dataset)
UNION ALL
SELECT
    'olist_sellers_dataset',
    (SELECT COUNT(*) FROM raw_olist_sellers_dataset),
    (SELECT COUNT(*) FROM cleaned_olist_sellers_dataset),
    (SELECT COUNT(*) FROM reject_olist_sellers_dataset),
    (SELECT COUNT(*) FROM raw_olist_sellers_dataset) -
    (SELECT COUNT(*) FROM cleaned_olist_sellers_dataset) -
    (SELECT COUNT(*) FROM reject_olist_sellers_dataset)
UNION ALL
SELECT
    'olist_order_reviews_dataset',
    (SELECT COUNT(*) FROM raw_olist_order_reviews_dataset),
    (SELECT COUNT(*) FROM cleaned_olist_order_reviews_dataset),
    (SELECT COUNT(*) FROM reject_olist_order_reviews_dataset),
    (SELECT COUNT(*) FROM raw_olist_order_reviews_dataset) -
    (SELECT COUNT(*) FROM cleaned_olist_order_reviews_dataset) -
    (SELECT COUNT(*) FROM reject_olist_order_reviews_dataset)
UNION ALL
SELECT
    'olist_geolocation_dataset',
    (SELECT COUNT(*) FROM raw_olist_geolocation_dataset),
    (SELECT COUNT(*) FROM cleaned_olist_geolocation_dataset),
    (SELECT COUNT(*) FROM reject_olist_geolocation_dataset),
    (SELECT COUNT(*) FROM raw_olist_geolocation_dataset) -
    (SELECT COUNT(*) FROM cleaned_olist_geolocation_dataset) -
    (SELECT COUNT(*) FROM reject_olist_geolocation_dataset)
UNION ALL
SELECT
    'product_category_name_translation',
    (SELECT COUNT(*) FROM raw_product_category_name_translation),
    (SELECT COUNT(*) FROM cleaned_product_category_name_translation),
    (SELECT COUNT(*) FROM reject_product_category_name_translation),
    (SELECT COUNT(*) FROM raw_product_category_name_translation) -
    (SELECT COUNT(*) FROM cleaned_product_category_name_translation) -
    (SELECT COUNT(*) FROM reject_product_category_name_translation);


-- ----------------------------------------------------------------
-- 2.2  DUPLICATE CHECK IN CLEANED TABLES (primary keys must be 0)
-- ----------------------------------------------------------------

-- Customers: customer_id must be unique
SELECT 'cleaned_customers - duplicate customer_id' AS check_name,
       COUNT(*) AS duplicate_count
FROM (
    SELECT customer_id FROM cleaned_olist_customers_dataset
    GROUP BY customer_id HAVING COUNT(*) > 1
) t;

-- Orders: order_id must be unique
SELECT 'cleaned_orders - duplicate order_id' AS check_name,
       COUNT(*) AS duplicate_count
FROM (
    SELECT order_id FROM cleaned_olist_orders_dataset
    GROUP BY order_id HAVING COUNT(*) > 1
) t;

-- Order Items: (order_id, order_item_id) must be unique
SELECT 'cleaned_order_items - duplicate (order_id, order_item_id)' AS check_name,
       COUNT(*) AS duplicate_count
FROM (
    SELECT order_id, order_item_id FROM cleaned_olist_order_items_dataset
    GROUP BY order_id, order_item_id HAVING COUNT(*) > 1
) t;

-- Payments: (order_id, payment_sequential) must be unique
SELECT 'cleaned_payments - duplicate (order_id, payment_sequential)' AS check_name,
       COUNT(*) AS duplicate_count
FROM (
    SELECT order_id, payment_sequential FROM cleaned_olist_order_payments_dataset
    GROUP BY order_id, payment_sequential HAVING COUNT(*) > 1
) t;

-- Products: product_id must be unique
SELECT 'cleaned_products - duplicate product_id' AS check_name,
       COUNT(*) AS duplicate_count
FROM (
    SELECT product_id FROM cleaned_olist_products_dataset
    GROUP BY product_id HAVING COUNT(*) > 1
) t;

-- Sellers: seller_id must be unique
SELECT 'cleaned_sellers - duplicate seller_id' AS check_name,
       COUNT(*) AS duplicate_count
FROM (
    SELECT seller_id FROM cleaned_olist_sellers_dataset
    GROUP BY seller_id HAVING COUNT(*) > 1
) t;

-- Category Translation: product_category_name must be unique
SELECT 'cleaned_category_translation - duplicate category_name' AS check_name,
       COUNT(*) AS duplicate_count
FROM (
    SELECT product_category_name FROM cleaned_product_category_name_translation
    GROUP BY product_category_name HAVING COUNT(*) > 1
) t;


-- ----------------------------------------------------------------
-- 2.3  NULL SUMMARY ON CLEANED TABLES (critical columns only)
-- ----------------------------------------------------------------

SELECT 'cleaned_customers' AS table_name,
    SUM(CASE WHEN customer_id        IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
    SUM(CASE WHEN customer_unique_id IS NULL THEN 1 ELSE 0 END) AS null_unique_id,
    SUM(CASE WHEN customer_state     IS NULL THEN 1 ELSE 0 END) AS null_state
FROM cleaned_olist_customers_dataset;

SELECT 'cleaned_orders' AS table_name,
    SUM(CASE WHEN order_id                  IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN customer_id               IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
    SUM(CASE WHEN order_status              IS NULL THEN 1 ELSE 0 END) AS null_status,
    SUM(CASE WHEN order_purchase_timestamp  IS NULL THEN 1 ELSE 0 END) AS null_purchase_ts,
    SUM(timestamp_sequence_flag)                                        AS flagged_ts_violations
FROM cleaned_olist_orders_dataset;

SELECT 'cleaned_order_items' AS table_name,
    SUM(CASE WHEN order_id      IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN product_id    IS NULL THEN 1 ELSE 0 END) AS null_product_id,
    SUM(CASE WHEN seller_id     IS NULL THEN 1 ELSE 0 END) AS null_seller_id,
    SUM(CASE WHEN price         IS NULL THEN 1 ELSE 0 END) AS null_price,
    SUM(CASE WHEN freight_value IS NULL THEN 1 ELSE 0 END) AS null_freight
FROM cleaned_olist_order_items_dataset;

SELECT 'cleaned_payments' AS table_name,
    SUM(CASE WHEN order_id       IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN payment_type   IS NULL THEN 1 ELSE 0 END) AS null_payment_type,
    SUM(CASE WHEN payment_value  IS NULL THEN 1 ELSE 0 END) AS null_payment_value
FROM cleaned_olist_order_payments_dataset;

SELECT 'cleaned_products' AS table_name,
    SUM(CASE WHEN product_id          IS NULL THEN 1 ELSE 0 END) AS null_product_id,
    SUM(category_missing_flag)                                    AS missing_category_flagged,
    SUM(dimensions_missing_flag)                                  AS missing_dimensions_flagged
FROM cleaned_olist_products_dataset;

SELECT 'cleaned_sellers' AS table_name,
    SUM(CASE WHEN seller_id    IS NULL THEN 1 ELSE 0 END) AS null_seller_id,
    SUM(CASE WHEN seller_state IS NULL THEN 1 ELSE 0 END) AS null_state
FROM cleaned_olist_sellers_dataset;

SELECT 'cleaned_reviews' AS table_name,
    SUM(CASE WHEN review_id    IS NULL THEN 1 ELSE 0 END) AS null_review_id,
    SUM(CASE WHEN review_score IS NULL THEN 1 ELSE 0 END) AS null_review_score,
    SUM(CASE WHEN review_comment_title IS NULL THEN 1 ELSE 0 END) AS null_comment_title
FROM cleaned_olist_order_reviews_dataset;

SELECT 'cleaned_geolocation' AS table_name,
    SUM(CASE WHEN geolocation_lat   IS NULL THEN 1 ELSE 0 END) AS null_lat,
    SUM(CASE WHEN geolocation_lng   IS NULL THEN 1 ELSE 0 END) AS null_lng,
    SUM(CASE WHEN geolocation_state IS NULL THEN 1 ELSE 0 END) AS null_state
FROM cleaned_olist_geolocation_dataset;


-- ----------------------------------------------------------------
-- 2.4  REJECT REASON BREAKDOWN (per table)
-- ----------------------------------------------------------------

SELECT 'customers'    AS dataset, reject_reason, COUNT(*) AS cnt
FROM reject_olist_customers_dataset
GROUP BY reject_reason
UNION ALL
SELECT 'orders',       reject_reason, COUNT(*) FROM reject_olist_orders_dataset          GROUP BY reject_reason
UNION ALL
SELECT 'order_items',  reject_reason, COUNT(*) FROM reject_olist_order_items_dataset      GROUP BY reject_reason
UNION ALL
SELECT 'payments',     reject_reason, COUNT(*) FROM reject_olist_order_payments_dataset   GROUP BY reject_reason
UNION ALL
SELECT 'products',     reject_reason, COUNT(*) FROM reject_olist_products_dataset         GROUP BY reject_reason
UNION ALL
SELECT 'sellers',      reject_reason, COUNT(*) FROM reject_olist_sellers_dataset          GROUP BY reject_reason
UNION ALL
SELECT 'reviews',      reject_reason, COUNT(*) FROM reject_olist_order_reviews_dataset    GROUP BY reject_reason
UNION ALL
SELECT 'geolocation',  reject_reason, COUNT(*) FROM reject_olist_geolocation_dataset      GROUP BY reject_reason
UNION ALL
SELECT 'category_translation', reject_reason, COUNT(*) FROM reject_product_category_name_translation GROUP BY reject_reason
ORDER BY dataset, cnt DESC;


-- ----------------------------------------------------------------
-- 2.5  BUSINESS RULE CHECKS ON CLEANED TABLES
-- ----------------------------------------------------------------

-- Orders: timestamp violations remaining in cleaned table
SELECT COUNT(*) AS orders_with_ts_violation
FROM cleaned_olist_orders_dataset
WHERE timestamp_sequence_flag = 1;

-- Order Items: verify no price <= 0
SELECT COUNT(*) AS invalid_price_in_cleaned
FROM cleaned_olist_order_items_dataset
WHERE price IS NULL OR price <= 0;

-- Order Items: verify no freight < 0
SELECT COUNT(*) AS invalid_freight_in_cleaned
FROM cleaned_olist_order_items_dataset
WHERE freight_value < 0;

-- Payments: verify no invalid values
SELECT COUNT(*) AS invalid_payment_in_cleaned
FROM cleaned_olist_order_payments_dataset
WHERE payment_value <= 0
   OR payment_type NOT IN ('credit_card','boleto','voucher','debit_card','not_defined')
   OR payment_installments < 1;

-- Reviews: verify all scores are 1–5
SELECT COUNT(*) AS invalid_score_in_cleaned
FROM cleaned_olist_order_reviews_dataset
WHERE review_score NOT BETWEEN 1 AND 5;

-- Geolocation: verify all coords are within Brazil
SELECT COUNT(*) AS coords_outside_brazil_in_cleaned
FROM cleaned_olist_geolocation_dataset
WHERE geolocation_lat NOT BETWEEN -33.75 AND 5.27
   OR geolocation_lng NOT BETWEEN -73.99 AND -34.79;


-- ============================================================
-- SECTION 5: OUTPUT TABLES
-- ============================================================

SELECT 'cleaned_olist_customers_dataset'           AS table_name, COUNT(*) AS row_count FROM cleaned_olist_customers_dataset
UNION ALL
SELECT 'cleaned_olist_orders_dataset',          COUNT(*) FROM cleaned_olist_orders_dataset
UNION ALL
SELECT 'cleaned_olist_order_items_dataset',     COUNT(*) FROM cleaned_olist_order_items_dataset
UNION ALL
SELECT 'cleaned_olist_order_payments_dataset',  COUNT(*) FROM cleaned_olist_order_payments_dataset
UNION ALL
SELECT 'cleaned_olist_products_dataset',        COUNT(*) FROM cleaned_olist_products_dataset
UNION ALL
SELECT 'cleaned_olist_sellers_dataset',         COUNT(*) FROM cleaned_olist_sellers_dataset
UNION ALL
SELECT 'cleaned_olist_order_reviews_dataset',   COUNT(*) FROM cleaned_olist_order_reviews_dataset
UNION ALL
SELECT 'cleaned_olist_geolocation_dataset',     COUNT(*) FROM cleaned_olist_geolocation_dataset
UNION ALL
SELECT 'cleaned_product_category_name_translation', COUNT(*) FROM cleaned_product_category_name_translation
UNION ALL
SELECT 'reject_olist_customers_dataset',        COUNT(*) FROM reject_olist_customers_dataset
UNION ALL
SELECT 'reject_olist_orders_dataset',           COUNT(*) FROM reject_olist_orders_dataset
UNION ALL
SELECT 'reject_olist_order_items_dataset',      COUNT(*) FROM reject_olist_order_items_dataset
UNION ALL
SELECT 'reject_olist_order_payments_dataset',   COUNT(*) FROM reject_olist_order_payments_dataset
UNION ALL
SELECT 'reject_olist_products_dataset',         COUNT(*) FROM reject_olist_products_dataset
UNION ALL
SELECT 'reject_olist_sellers_dataset',          COUNT(*) FROM reject_olist_sellers_dataset
UNION ALL
SELECT 'reject_olist_order_reviews_dataset',    COUNT(*) FROM reject_olist_order_reviews_dataset
UNION ALL
SELECT 'reject_olist_geolocation_dataset',      COUNT(*) FROM reject_olist_geolocation_dataset
UNION ALL
SELECT 'reject_product_category_name_translation', COUNT(*) FROM reject_product_category_name_translation
ORDER BY table_name;


-- ============================================================
-- SECTION 6: STAGE SUMMARY (FOR HANDOFF)
-- ============================================================

/*
====================================================================
STAGE 1 SUMMARY
====================================================================

Objective:
  Ingest all 9 raw Olist Brazilian e-commerce CSV datasets into MySQL,
  perform per-column data profiling, and produce cleaned_<table> and
  reject_<table> outputs for every dataset — with NO joins, NO aggregations,
  and NO feature engineering.

Tables Used:
  1.  olist_customers_dataset
  2.  olist_orders_dataset
  3.  olist_order_items_dataset
  4.  olist_order_payments_dataset
  5.  olist_products_dataset
  6.  olist_sellers_dataset
  7.  olist_order_reviews_dataset
  8.  olist_geolocation_dataset
  9.  product_category_name_translation

Transformations Applied:
  - Exact duplicate rows removed (all columns identical → reject)
  - NULL primary keys rejected with reason code
  - Non-exact duplicate primary keys: first occurrence kept; rest rejected
  - Text fields: TRIM applied to remove leading/trailing whitespace
  - city fields: LOWER(TRIM(...)) for uniformity
  - state fields: UPPER(TRIM(...)) for uniformity
  - product_category_name: LOWER(TRIM(...))
  - Orders: timestamp sequence inconsistencies FLAGGED via
    timestamp_sequence_flag column (not rejected — these are real orders)
  - Products: missing category and missing dimensions FLAGGED via
    category_missing_flag and dimensions_missing_flag (not rejected —
    missing metadata is common in product catalogues)
  - Geolocation: coordinates outside Brazil bounding box rejected

Data Issues Found:
  - Customers    : Potential duplicate customer_id rows (same ID, different mapping)
  - Orders       : Some orders have NULL approval/delivery timestamps
                   (expected for cancelled/pending status)
                   Some timestamp sequences are out of order (flagged)
  - Order Items  : Possible price = 0 edge cases; freight_value = 0 (valid, free shipping)
  - Payments     : payment_value = 0 rows exist (rejected); multiple payment methods
                   per order are expected and preserved
  - Products     : ~600+ rows with NULL product_category_name (flagged, not rejected)
                   ~2 rows with NULL dimensions (flagged, not rejected)
  - Sellers      : Generally clean; minor TRIM standardisation applied
  - Reviews      : Duplicate review_ids exist (same review appears multiple times);
                   exact duplicates removed; invalid scores rejected
  - Geolocation  : Multiple (lat, lng) per zip code is expected — not treated as duplicates;
                   only exact row duplicates and out-of-bound coordinates rejected
                   (~1 M rows; largest table)
  - Translation  : Small reference table; typically clean

How Issues Were Handled:
  - Hard violations (NULL key, invalid score, invalid payment value/type,
    negative price/freight, coords out of bounds) → moved to reject_ table
    with descriptive reject_reason
  - Soft violations (missing category, missing dimensions, timestamp sequence
    errors in orders) → kept in cleaned_ table with quality flag columns
  - Exact duplicate rows → moved to reject_ table with reason EXACT_DUPLICATE_ROW
  - Duplicate primary keys (non-exact) → all but first kept occurrence moved
    to reject_ table with reason DUPLICATE_<KEY_COLUMN>

Output Tables Created:
  CLEANED (18 tables total — 9 cleaned + 9 reject):
  cleaned_olist_customers_dataset
  cleaned_olist_orders_dataset             (+ timestamp_sequence_flag)
  cleaned_olist_order_items_dataset
  cleaned_olist_order_payments_dataset
  cleaned_olist_products_dataset           (+ category_missing_flag, dimensions_missing_flag)
  cleaned_olist_sellers_dataset
  cleaned_olist_order_reviews_dataset
  cleaned_olist_geolocation_dataset
  cleaned_product_category_name_translation

  REJECT (with reject_reason + rejected_at):
  reject_olist_customers_dataset
  reject_olist_orders_dataset
  reject_olist_order_items_dataset
  reject_olist_order_payments_dataset
  reject_olist_products_dataset
  reject_olist_sellers_dataset
  reject_olist_order_reviews_dataset
  reject_olist_geolocation_dataset
  reject_product_category_name_translation

Key Validations Performed:
  - Row count before vs after per table (unaccounted_rows = 0 is the goal)
  - Duplicate count = 0 on primary keys in cleaned tables
  - NULL count on primary keys in cleaned tables = 0
  - Business rule checks on cleaned tables (price, freight, score, coords, payments)
  - Reject reason breakdown per table
  - Cross-table key overlap counts (informational; full joins deferred to later stages)

Why This Stage Matters (Business Context):
  Downstream analytics — customer lifecycle, retention scoring, revenue modelling,
  behavioural segmentation — depend entirely on the integrity of these base tables.
  A single duplicate customer_id or mismatched timestamp corrupts cohort analysis,
  CLV calculations, and funnel attribution. This stage creates a single source of
  truth for all subsequent stages, with full audit trails (reject tables) and
  transparent quality flags that allow analysts to make informed decisions about
  borderline records rather than silently discarding them.

====================================================================
*/

-- ============================================================
-- END OF STAGE 1
-- All cleaned_ and reject_ tables are now ready for Stage 2.
-- ============================================================
