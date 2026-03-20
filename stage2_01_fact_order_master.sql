-- ============================================================
-- OLIST BRAZILIAN E-COMMERCE ANALYTICS
-- STAGE 2 | FILE 1: REFERENTIAL INTEGRITY + fact_order_master
-- Environment : MySQL 8+
-- Run After   : stage1_corrections.sql
-- ============================================================
-- STAGE 2 OBJECTIVE:
--   Build fact_order_master — ONE ROW PER order_id — by safely
--   joining all 9 cleaned tables with pre-aggregated intermediates.
-- ============================================================

-- USE olist_ecommerce;

-- ============================================================
-- SECTION 1: REFERENTIAL INTEGRITY CHECKS  (C6 — Mandatory)
-- ============================================================
-- These checks are informational audit queries.
-- Orphan rows are NOT deleted; they appear as NULL in the fact table
-- and are documented in the stage summary.
-- ============================================================

-- 1.1  orders.customer_id must exist in cleaned_customers
SELECT
    'orders → customers'                                   AS ri_check,
    COUNT(*)                                               AS total_orders,
    SUM(CASE WHEN c.customer_id IS NULL THEN 1 ELSE 0 END) AS orphan_orders_no_customer,
    ROUND(SUM(CASE WHEN c.customer_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
                                                           AS orphan_pct
FROM cleaned_olist_orders_dataset o
LEFT JOIN (
    SELECT DISTINCT customer_id FROM cleaned_olist_customers_dataset
) c ON o.customer_id = c.customer_id;

-- 1.2  order_items.order_id must exist in cleaned_orders
SELECT
    'order_items → orders'                                 AS ri_check,
    COUNT(*)                                               AS total_items,
    SUM(CASE WHEN ord.order_id IS NULL THEN 1 ELSE 0 END)  AS orphan_items_no_order,
    ROUND(SUM(CASE WHEN ord.order_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
                                                           AS orphan_pct
FROM cleaned_olist_order_items_dataset oi
LEFT JOIN (
    SELECT DISTINCT order_id FROM cleaned_olist_orders_dataset
) ord ON oi.order_id = ord.order_id;

-- 1.3  payments.order_id must exist in cleaned_orders
SELECT
    'payments → orders'                                    AS ri_check,
    COUNT(*)                                               AS total_payments,
    SUM(CASE WHEN ord.order_id IS NULL THEN 1 ELSE 0 END)  AS orphan_payments_no_order,
    ROUND(SUM(CASE WHEN ord.order_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
                                                           AS orphan_pct
FROM cleaned_olist_order_payments_dataset p
LEFT JOIN (
    SELECT DISTINCT order_id FROM cleaned_olist_orders_dataset
) ord ON p.order_id = ord.order_id;

-- 1.4  reviews.order_id must exist in cleaned_orders
SELECT
    'reviews → orders'                                     AS ri_check,
    COUNT(*)                                               AS total_reviews,
    SUM(CASE WHEN ord.order_id IS NULL THEN 1 ELSE 0 END)  AS orphan_reviews_no_order,
    ROUND(SUM(CASE WHEN ord.order_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
                                                           AS orphan_pct
FROM cleaned_olist_order_reviews_dataset r
LEFT JOIN (
    SELECT DISTINCT order_id FROM cleaned_olist_orders_dataset
) ord ON r.order_id = ord.order_id;

-- 1.5  order_items.product_id must exist in cleaned_products
SELECT
    'order_items → products'                               AS ri_check,
    COUNT(*)                                               AS total_items,
    SUM(CASE WHEN p.product_id IS NULL THEN 1 ELSE 0 END)  AS orphan_items_no_product,
    ROUND(SUM(CASE WHEN p.product_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
                                                           AS orphan_pct
FROM cleaned_olist_order_items_dataset oi
LEFT JOIN (
    SELECT DISTINCT product_id FROM cleaned_olist_products_dataset
) p ON oi.product_id = p.product_id;

-- 1.6  order_items.seller_id must exist in cleaned_sellers
SELECT
    'order_items → sellers'                                AS ri_check,
    COUNT(*)                                               AS total_items,
    SUM(CASE WHEN s.seller_id IS NULL THEN 1 ELSE 0 END)   AS orphan_items_no_seller,
    ROUND(SUM(CASE WHEN s.seller_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
                                                           AS orphan_pct
FROM cleaned_olist_order_items_dataset oi
LEFT JOIN (
    SELECT DISTINCT seller_id FROM cleaned_olist_sellers_dataset
) s ON oi.seller_id = s.seller_id;


-- ============================================================
-- SECTION 2: BASE VIEWS FOR DEDUPLICATION
-- ============================================================
-- For joins in the fact table, we need exactly one row per key.
-- Strategy: prefer duplicate_flag = 0 rows; if all are flagged,
--           use ROW_NUMBER to deterministically pick one.
-- These are inline subqueries used in later CTEs.
-- ============================================================

-- Helper: one customer per customer_id (preferred: non-duplicate)
DROP VIEW IF EXISTS v_customers_deduped;
CREATE VIEW v_customers_deduped AS
SELECT customer_id, customer_unique_id, customer_city, customer_state
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY duplicate_flag ASC, cleaned_at ASC
        ) AS rn
    FROM cleaned_olist_customers_dataset
    WHERE customer_id IS NOT NULL
) t
WHERE rn = 1;

-- Helper: one seller per seller_id
DROP VIEW IF EXISTS v_sellers_deduped;
CREATE VIEW v_sellers_deduped AS
SELECT seller_id, seller_state, seller_city
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY seller_id
            ORDER BY duplicate_flag ASC, cleaned_at ASC
        ) AS rn
    FROM cleaned_olist_sellers_dataset
    WHERE seller_id IS NOT NULL
) t
WHERE rn = 1;

-- Helper: one product per product_id
DROP VIEW IF EXISTS v_products_deduped;
CREATE VIEW v_products_deduped AS
SELECT product_id, product_category_name
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY duplicate_flag ASC, cleaned_at ASC
        ) AS rn
    FROM cleaned_olist_products_dataset
    WHERE product_id IS NOT NULL
) t
WHERE rn = 1;

-- Helper: one translation per Portuguese category name
DROP VIEW IF EXISTS v_translation_deduped;
CREATE VIEW v_translation_deduped AS
SELECT product_category_name, product_category_name_english
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY product_category_name
            ORDER BY duplicate_flag ASC, cleaned_at ASC
        ) AS rn
    FROM cleaned_product_category_name_translation
    WHERE product_category_name IS NOT NULL
) t
WHERE rn = 1;


-- ============================================================
-- SECTION 3: INTERMEDIATE AGGREGATION TABLES
-- These are pre-aggregated to ensure ONE ROW PER order_id.
-- Uses DISTINCT on composite keys before aggregating to avoid
-- double-counting from any remaining duplicate_flag = 1 rows.
-- ============================================================

-- 3.1  Order Items Aggregated (per order_id)
DROP TABLE IF EXISTS agg_order_items;
CREATE TABLE agg_order_items AS
SELECT
    order_id,
    COUNT(order_item_id)              AS total_items,
    COUNT(DISTINCT product_id)        AS unique_products,
    COUNT(DISTINCT seller_id)         AS unique_sellers,
    ROUND(SUM(price), 2)              AS total_item_value,
    ROUND(SUM(COALESCE(freight_value, 0)), 2) AS total_freight_value,
    SUM(is_free_item)                 AS free_item_count
FROM (
    SELECT DISTINCT order_id, order_item_id, product_id, seller_id,
                    price, freight_value, is_free_item
    FROM cleaned_olist_order_items_dataset
    WHERE order_id IS NOT NULL
) t
GROUP BY order_id;

-- 3.2  Dominant Category per Order
--      Most frequent product category among items in an order.
--      Tie-broken alphabetically for determinism.
DROP TABLE IF EXISTS agg_order_dominant_category;
CREATE TABLE agg_order_dominant_category AS
WITH item_cat AS (
    SELECT
        oi.order_id,
        COALESCE(p.product_category_name, 'uncategorised') AS category_name,
        COUNT(*) AS cat_count
    FROM (
        SELECT DISTINCT order_id, product_id
        FROM cleaned_olist_order_items_dataset
        WHERE order_id IS NOT NULL
    ) oi
    LEFT JOIN v_products_deduped p ON oi.product_id = p.product_id
    GROUP BY oi.order_id, COALESCE(p.product_category_name, 'uncategorised')
),
cat_ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY cat_count DESC, category_name ASC
        ) AS rn
    FROM item_cat
)
SELECT order_id, category_name AS dominant_category
FROM cat_ranked
WHERE rn = 1;

-- 3.3  Dominant Seller State per Order
DROP TABLE IF EXISTS agg_order_dominant_seller;
CREATE TABLE agg_order_dominant_seller AS
WITH item_state AS (
    SELECT
        oi.order_id,
        COALESCE(s.seller_state, 'unknown') AS seller_state,
        COUNT(*) AS state_count
    FROM (
        SELECT DISTINCT order_id, seller_id
        FROM cleaned_olist_order_items_dataset
        WHERE order_id IS NOT NULL
    ) oi
    LEFT JOIN v_sellers_deduped s ON oi.seller_id = s.seller_id
    GROUP BY oi.order_id, COALESCE(s.seller_state, 'unknown')
),
state_ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY state_count DESC, seller_state ASC
        ) AS rn
    FROM item_state
)
SELECT order_id, NULLIF(seller_state, 'unknown') AS dominant_seller_state
FROM state_ranked
WHERE rn = 1;

-- 3.4  Payments Aggregated (per order_id)
DROP TABLE IF EXISTS agg_order_payments;
CREATE TABLE agg_order_payments AS
SELECT
    order_id,
    ROUND(SUM(payment_value), 2)              AS total_payment_value,
    ROUND(AVG(payment_installments), 2)       AS avg_payment_installments,
    COUNT(*)                                   AS payment_row_count,
    COUNT(DISTINCT payment_type)              AS distinct_payment_types
FROM (
    SELECT DISTINCT order_id, payment_sequential, payment_type,
                    payment_installments, payment_value
    FROM cleaned_olist_order_payments_dataset
    WHERE order_id IS NOT NULL
) t
GROUP BY order_id;

-- 3.5  Reviews Aggregated (per order_id)
DROP TABLE IF EXISTS agg_order_reviews;
CREATE TABLE agg_order_reviews AS
SELECT
    order_id,
    ROUND(AVG(review_score), 2) AS avg_review_score,
    COUNT(*)                     AS review_count
FROM (
    SELECT DISTINCT order_id, review_id, review_score
    FROM cleaned_olist_order_reviews_dataset
    WHERE order_id IS NOT NULL
) t
GROUP BY order_id;

-- 3.6  High-Value Order Threshold (75th percentile of total_payment_value)
DROP TABLE IF EXISTS tmp_hv_threshold;
CREATE TABLE tmp_hv_threshold AS
WITH payment_cdf AS (
    SELECT
        total_payment_value,
        CUME_DIST() OVER (ORDER BY total_payment_value) AS cum_dist
    FROM agg_order_payments
)
SELECT MIN(total_payment_value) AS p75_threshold
FROM payment_cdf
WHERE cum_dist >= 0.75;


-- ============================================================
-- SECTION 4: BUILD fact_order_master
-- Granularity: ONE ROW PER order_id
-- Base table: cleaned_olist_orders_dataset (orders with duplicate_flag = 0
--             preferred; if all are duplicated, one is kept via ROW_NUMBER)
-- ============================================================

DROP TABLE IF EXISTS fact_order_master;
CREATE TABLE fact_order_master AS

WITH orders_base AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY duplicate_flag ASC, cleaned_at ASC
            ) AS rn
        FROM cleaned_olist_orders_dataset
        WHERE order_id IS NOT NULL
    ) t
    WHERE rn = 1
)

SELECT

    -- ── ORDER IDENTIFIERS ──────────────────────────────────────
    o.order_id,
    o.customer_id,
    c.customer_unique_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_delivered_customer_date,

    -- ── CUSTOMER GEOGRAPHY ────────────────────────────────────
    c.customer_city,
    c.customer_state,

    -- ── FINANCIALS ────────────────────────────────────────────
    COALESCE(ia.total_item_value,     0)    AS total_order_value,
    COALESCE(ia.total_freight_value,  0)    AS total_freight_value,
    COALESCE(pa.total_payment_value,  0)    AS total_payment_value,
    COALESCE(pa.avg_payment_installments, 1) AS avg_payment_installments,

    -- ── PRODUCT METRICS ───────────────────────────────────────
    COALESCE(ia.total_items,     0)         AS total_items,
    COALESCE(ia.unique_products, 0)         AS unique_products,
    dc.dominant_category,
    tr.product_category_name_english        AS category_english,

    -- ── SELLER METRICS ────────────────────────────────────────
    COALESCE(ia.unique_sellers, 0)          AS unique_sellers,
    ds.dominant_seller_state,

    -- ── REVIEW METRICS ────────────────────────────────────────
    ra.avg_review_score,
    COALESCE(ra.review_count, 0)            AS review_count,

    -- ── TEMPORAL DERIVED FEATURES ────────────────────────────
    MONTH(o.order_purchase_timestamp)        AS order_month,
    YEAR(o.order_purchase_timestamp)         AS order_year,
    strftime(o.order_purchase_timestamp, '%A')      AS order_day_of_week,

    -- ── OPERATIONAL DERIVED FEATURES ─────────────────────────
    CASE
        WHEN o.order_delivered_customer_date IS NOT NULL
         AND o.order_estimated_delivery_date IS NOT NULL
        THEN date_diff('day', o.order_estimated_delivery_date, o.order_delivered_customer_date)
        ELSE NULL
    END AS delivery_delay_days,

    CASE
        WHEN o.order_approved_at         IS NOT NULL
         AND o.order_purchase_timestamp  IS NOT NULL
        THEN date_diff('day', o.order_purchase_timestamp, o.order_approved_at)
        ELSE NULL
    END AS processing_time_days,

    -- ── FLAGS ─────────────────────────────────────────────────
    CASE
        WHEN o.order_delivered_customer_date IS NOT NULL
         AND o.order_estimated_delivery_date IS NOT NULL
         AND date_diff('day', o.order_estimated_delivery_date,
                      o.order_delivered_customer_date) > 0
        THEN 1 ELSE 0
    END AS is_delayed_delivery,

    CASE
        WHEN COALESCE(pa.total_payment_value, 0) >
             (SELECT p75_threshold FROM tmp_hv_threshold)
        THEN 1 ELSE 0
    END AS is_high_value_order,

    CASE
        WHEN COALESCE(pa.payment_row_count, 0) > 1
          OR COALESCE(pa.distinct_payment_types, 0) > 1
        THEN 1 ELSE 0
    END AS is_multi_payment,

    CASE
        WHEN COALESCE(ia.total_items, 0) > 1
        THEN 1 ELSE 0
    END AS is_multi_item_order,

    -- ── QUALITY METADATA ─────────────────────────────────────
    o.timestamp_sequence_flag,
    CASE WHEN c.customer_id    IS NULL THEN 1 ELSE 0 END AS ri_missing_customer,
    CASE WHEN ia.order_id      IS NULL THEN 1 ELSE 0 END AS ri_missing_items,
    CASE WHEN pa.order_id      IS NULL THEN 1 ELSE 0 END AS ri_missing_payment,
    CASE WHEN ra.order_id      IS NULL THEN 1 ELSE 0 END AS ri_missing_review,

    NOW() AS fact_created_at

FROM orders_base               o
LEFT JOIN v_customers_deduped  c  ON o.customer_id  = c.customer_id
LEFT JOIN agg_order_items      ia ON o.order_id     = ia.order_id
LEFT JOIN agg_order_dominant_category dc ON o.order_id = dc.order_id
LEFT JOIN v_translation_deduped      tr ON dc.dominant_category = tr.product_category_name
LEFT JOIN agg_order_dominant_seller  ds ON o.order_id = ds.order_id
LEFT JOIN agg_order_payments   pa ON o.order_id     = pa.order_id
LEFT JOIN agg_order_reviews    ra ON o.order_id     = ra.order_id;


-- ============================================================
-- SECTION 5: INDEXES FOR QUERY PERFORMANCE
-- ============================================================

ALTER TABLE fact_order_master
    ADD PRIMARY KEY (order_id);

CREATE INDEX idx_fom_customer_id      ON fact_order_master (customer_id);
CREATE INDEX idx_fom_customer_unique  ON fact_order_master (customer_unique_id);
CREATE INDEX idx_fom_order_status     ON fact_order_master (order_status);
CREATE INDEX idx_fom_order_year_month ON fact_order_master (order_year, order_month);
CREATE INDEX idx_fom_customer_state   ON fact_order_master (customer_state);
CREATE INDEX idx_fom_dominant_cat     ON fact_order_master (dominant_category);
CREATE INDEX idx_fom_is_delayed       ON fact_order_master (is_delayed_delivery);
CREATE INDEX idx_fom_is_high_value    ON fact_order_master (is_high_value_order);

-- ============================================================
-- END OF FILE 1
-- Next step: Run stage2_02_validation.sql
-- ============================================================
