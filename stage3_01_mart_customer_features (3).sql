-- ============================================================
-- OLIST BRAZILIAN E-COMMERCE ANALYTICS
-- STAGE 3 | FILE 1: mart_customer_features
-- Environment : MySQL 8+
-- Source      : fact_order_master ONLY (no raw table joins)
-- Run After   : stage2_01_fact_order_master.sql
-- ============================================================
-- STAGE 3 OBJECTIVE:
--   Build a customer-level analytics mart with:
--     - Core aggregations (spend, orders, dates)
--     - RFM scoring (1–5 NTILE scale)
--     - Segment labels (champions → lost)
--     - Lifecycle stage (ACTIVE / AT_RISK / DORMANT)
--     - Retention metrics (cohort, repeat purchase)
--     - Behavioral features (delays, multi-item, multi-payment)
-- ============================================================
-- NOTE ON CUSTOMER KEY:
--   Olist has two customer identifiers:
--     customer_id        = per-order session key (1:1 with orders)
--     customer_unique_id = actual person identifier (1:N with orders)
--   This mart is keyed on customer_unique_id for business correctness.
--   One person may have placed multiple orders with different customer_ids.
-- ============================================================

USE olist_ecommerce;

-- ============================================================
-- STEP 1: REFERENCE DATE
-- Using MAX(order_purchase_timestamp) from fact_order_master
-- so lifecycle logic is meaningful for this historical dataset.
-- ============================================================

DROP TABLE IF EXISTS tmp_reference_date;
CREATE TABLE tmp_reference_date AS
SELECT MAX(order_purchase_timestamp) AS ref_date
FROM fact_order_master
WHERE order_purchase_timestamp IS NOT NULL;

-- Confirm reference date
SELECT 'Reference date for recency calculation' AS label,
       ref_date FROM tmp_reference_date;


-- ============================================================
-- STEP 2: BASE CUSTOMER AGGREGATIONS
-- All metrics derived exclusively from fact_order_master.
-- ============================================================

DROP TABLE IF EXISTS tmp_customer_agg;
CREATE TABLE tmp_customer_agg AS
SELECT
    customer_unique_id,

    -- ── ORDER VOLUME ─────────────────────────────────────────
    COUNT(DISTINCT order_id)                                          AS total_orders,

    -- ── SPEND METRICS ────────────────────────────────────────
    ROUND(SUM(total_payment_value), 2)                                AS total_spend,
    ROUND(AVG(total_payment_value), 2)                                AS avg_order_value,
    ROUND(MIN(total_payment_value), 2)                                AS min_order_value,
    ROUND(MAX(total_payment_value), 2)                                AS max_order_value,

    -- ── RFM RAW COMPONENTS ───────────────────────────────────
    COUNT(DISTINCT order_id)                                          AS frequency,
    ROUND(SUM(total_payment_value), 2)                                AS monetary,
    DATEDIFF(
        (SELECT ref_date FROM tmp_reference_date),
        MAX(order_purchase_timestamp)
    )                                                                  AS recency_days,

    -- ── ORDER DATES ──────────────────────────────────────────
    MIN(order_purchase_timestamp)                                      AS first_order_date,
    MAX(order_purchase_timestamp)                                      AS last_order_date,

    -- ── CUSTOMER LIFETIME ────────────────────────────────────
    DATEDIFF(
        MAX(order_purchase_timestamp),
        MIN(order_purchase_timestamp)
    )                                                                  AS customer_lifetime_days,

    -- ── BEHAVIOURAL FEATURES ─────────────────────────────────
    ROUND(
        AVG(CASE WHEN delivery_delay_days IS NOT NULL
                 THEN delivery_delay_days END), 2
    )                                                                  AS avg_delivery_delay,

    ROUND(
        SUM(is_delayed_delivery) * 1.0 / NULLIF(COUNT(DISTINCT order_id), 0), 4
    )                                                                  AS delayed_order_ratio,

    ROUND(
        SUM(is_multi_item_order) * 1.0 / NULLIF(COUNT(DISTINCT order_id), 0), 4
    )                                                                  AS multi_item_ratio,

    ROUND(
        SUM(is_multi_payment) * 1.0 / NULLIF(COUNT(DISTINCT order_id), 0), 4
    )                                                                  AS multi_payment_ratio,

    ROUND(AVG(avg_review_score), 2)                                    AS avg_customer_review_score,
    ROUND(AVG(total_items), 2)                                         AS avg_items_per_order,
    ROUND(AVG(total_freight_value), 2)                                 AS avg_freight_per_order,
    ROUND(AVG(unique_sellers), 2)                                      AS avg_sellers_per_order,
    ROUND(AVG(unique_products), 2)                                     AS avg_products_per_order,
    COUNT(DISTINCT dominant_category)                                   AS total_categories_purchased,

    -- ── STATUS COUNTS ────────────────────────────────────────
    SUM(CASE WHEN order_status = 'delivered'   THEN 1 ELSE 0 END)     AS delivered_orders,
    SUM(CASE WHEN order_status = 'cancelled'   THEN 1 ELSE 0 END)     AS cancelled_orders,
    SUM(CASE WHEN order_status = 'shipped'     THEN 1 ELSE 0 END)     AS shipped_orders,

    -- ── HIGH VALUE FLAG RATE ──────────────────────────────────
    ROUND(SUM(is_high_value_order) * 1.0 / NULLIF(COUNT(DISTINCT order_id), 0), 4) AS high_value_order_ratio,

    -- ── REVIEW COUNT ─────────────────────────────────────────
    SUM(review_count)                                                   AS total_reviews_given

FROM fact_order_master
WHERE customer_unique_id IS NOT NULL
GROUP BY customer_unique_id;


-- ============================================================
-- STEP 3: MOST RECENT CUSTOMER LOCATION
-- A customer may have ordered from different cities over time.
-- We use the city/state from their most recent order.
-- ============================================================

DROP TABLE IF EXISTS tmp_customer_location;
CREATE TABLE tmp_customer_location AS
SELECT customer_unique_id, customer_city, customer_state
FROM (
    SELECT
        customer_unique_id,
        customer_city,
        customer_state,
        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id
            ORDER BY order_purchase_timestamp DESC
        ) AS rn
    FROM fact_order_master
    WHERE customer_unique_id IS NOT NULL
) t
WHERE rn = 1;


-- ============================================================
-- STEP 4: PREFERRED PRODUCT CATEGORY
-- Category most frequently purchased by the customer.
-- Alphabetical tie-break for determinism.
-- ============================================================

DROP TABLE IF EXISTS tmp_customer_preferred_cat;
CREATE TABLE tmp_customer_preferred_cat AS
SELECT customer_unique_id, preferred_category
FROM (
    SELECT
        customer_unique_id,
        dominant_category AS preferred_category,
        COUNT(*)          AS cat_count,
        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id
            ORDER BY COUNT(*) DESC, dominant_category ASC
        ) AS rn
    FROM fact_order_master
    WHERE customer_unique_id IS NOT NULL
      AND dominant_category IS NOT NULL
    GROUP BY customer_unique_id, dominant_category
) t
WHERE rn = 1;


-- ============================================================
-- STEP 5: PREFERRED DAY OF WEEK
-- Day on which the customer most frequently places orders.
-- ============================================================

DROP TABLE IF EXISTS tmp_customer_preferred_day;
CREATE TABLE tmp_customer_preferred_day AS
SELECT customer_unique_id, preferred_day_of_week
FROM (
    SELECT
        customer_unique_id,
        order_day_of_week AS preferred_day_of_week,
        COUNT(*)          AS day_count,
        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id
            ORDER BY COUNT(*) DESC, order_day_of_week ASC
        ) AS rn
    FROM fact_order_master
    WHERE customer_unique_id IS NOT NULL
      AND order_day_of_week IS NOT NULL
    GROUP BY customer_unique_id, order_day_of_week
) t
WHERE rn = 1;


-- ============================================================
-- STEP 6: RECENCY WINDOW ORDER COUNTS
-- Count of orders placed within the last 30 / 60 / 90 days
-- from the reference date — used for retention micro-analysis.
-- ============================================================

DROP TABLE IF EXISTS tmp_customer_recency_windows;
CREATE TABLE tmp_customer_recency_windows AS
SELECT
    f.customer_unique_id,
    SUM(CASE WHEN DATEDIFF(r.ref_date, f.order_purchase_timestamp) <= 30  THEN 1 ELSE 0 END) AS orders_last_30d,
    SUM(CASE WHEN DATEDIFF(r.ref_date, f.order_purchase_timestamp) <= 60  THEN 1 ELSE 0 END) AS orders_last_60d,
    SUM(CASE WHEN DATEDIFF(r.ref_date, f.order_purchase_timestamp) <= 90  THEN 1 ELSE 0 END) AS orders_last_90d,
    SUM(CASE WHEN DATEDIFF(r.ref_date, f.order_purchase_timestamp) <= 180 THEN 1 ELSE 0 END) AS orders_last_180d
FROM fact_order_master f
CROSS JOIN tmp_reference_date r
WHERE f.customer_unique_id IS NOT NULL
GROUP BY f.customer_unique_id;


-- ============================================================
-- STEP 7: RFM SCORING (NTILE 1–5)
-- R: lower recency_days = better → ORDER BY recency DESC → tile 5 = most recent
-- F: higher frequency    = better → ORDER BY frequency ASC → tile 5 = most frequent
-- M: higher monetary     = better → ORDER BY monetary   ASC → tile 5 = highest spend
-- rfm_score = r + f + m  → range 3–15
-- rfm_label = concatenated string e.g. '555', '213'
-- ============================================================

DROP TABLE IF EXISTS tmp_customer_rfm;
CREATE TABLE tmp_customer_rfm AS
SELECT
    customer_unique_id,
    r_score,
    f_score,
    m_score,
    (r_score + f_score + m_score)    AS rfm_score,
    CONCAT(r_score, f_score, m_score) AS rfm_label
FROM (
    SELECT
        customer_unique_id,
        NTILE(5) OVER (ORDER BY recency_days  DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency     ASC)  AS f_score,
        NTILE(5) OVER (ORDER BY monetary      ASC)  AS m_score
    FROM tmp_customer_agg
) scored;


-- ============================================================
-- STEP 8: FINAL ASSEMBLY — mart_customer_features
-- ============================================================

DROP TABLE IF EXISTS mart_customer_features;
CREATE TABLE mart_customer_features AS
SELECT

    -- ── IDENTITY ─────────────────────────────────────────────
    a.customer_unique_id,
    l.customer_city,
    l.customer_state,

    -- ── CORE ORDER METRICS ───────────────────────────────────
    a.total_orders,
    a.total_spend,
    a.avg_order_value,
    a.min_order_value,
    a.max_order_value,
    a.first_order_date,
    a.last_order_date,
    a.customer_lifetime_days,

    -- ── RFM RAW COMPONENTS ───────────────────────────────────
    a.recency_days,
    a.frequency,
    a.monetary,

    -- ── RFM SCORES ───────────────────────────────────────────
    f.r_score,
    f.f_score,
    f.m_score,
    f.rfm_score,
    f.rfm_label,

    -- ── LIFECYCLE STAGE ───────────────────────────────────────
    -- Based on recency_days from reference date
    CASE
        WHEN a.recency_days <= 30              THEN 'ACTIVE'
        WHEN a.recency_days BETWEEN 31 AND 60  THEN 'AT_RISK'
        ELSE                                        'DORMANT'
    END AS lifecycle_stage,

    -- ── SEGMENT LABEL ─────────────────────────────────────────
    -- Priority-ordered CASE: most specific/valuable segments first
    CASE
        WHEN f.r_score >= 4 AND f.f_score >= 4 AND f.m_score >= 4
            THEN 'champions'
        WHEN f.r_score <= 2 AND f.m_score >= 4
            THEN 'cannot_lose'
        WHEN f.f_score >= 4 AND f.m_score >= 3 AND f.r_score >= 3
            THEN 'loyal'
        WHEN f.m_score = 5 AND f.r_score >= 3
            THEN 'high_value'
        WHEN f.r_score >= 4 AND f.f_score = 1 AND a.total_orders = 1
            THEN 'new_customer'
        WHEN f.r_score >= 3 AND f.f_score BETWEEN 2 AND 3
            THEN 'potential_loyalist'
        WHEN f.r_score = 2 AND f.f_score >= 2
            THEN 'churn_risk'
        WHEN f.r_score <= 2 AND f.f_score >= 3
            THEN 'at_risk'
        WHEN f.r_score = 1 AND f.f_score = 1 AND f.m_score = 1
            THEN 'lost'
        WHEN f.r_score <= 2 AND f.f_score <= 2
            THEN 'hibernating'
        ELSE
            'others'
    END AS segment_label,

    -- ── RETENTION METRICS ────────────────────────────────────
    DATE_FORMAT(a.first_order_date, '%Y-%m')        AS cohort_month,
    YEAR(a.first_order_date)                        AS cohort_year,
    CASE WHEN a.total_orders > 1 THEN 1 ELSE 0 END  AS repeat_purchase_flag,
    CASE WHEN a.total_orders = 1 THEN 1 ELSE 0 END  AS is_one_time_buyer,
    CASE
        WHEN a.total_orders > 1 AND a.customer_lifetime_days > 0
        THEN ROUND(a.customer_lifetime_days * 1.0 / NULLIF((a.total_orders - 1), 0), 1)
        ELSE NULL
    END                                              AS avg_days_between_orders,
    DATEDIFF(
        (SELECT ref_date FROM tmp_reference_date),
        a.first_order_date
    )                                                AS days_since_first_order,

    -- ── RECENCY WINDOW COUNTS ────────────────────────────────
    COALESCE(w.orders_last_30d,  0)                 AS orders_last_30d,
    COALESCE(w.orders_last_60d,  0)                 AS orders_last_60d,
    COALESCE(w.orders_last_90d,  0)                 AS orders_last_90d,
    COALESCE(w.orders_last_180d, 0)                 AS orders_last_180d,

    -- ── BEHAVIOURAL FEATURES ─────────────────────────────────
    a.avg_delivery_delay,
    a.delayed_order_ratio,
    a.multi_item_ratio,
    a.multi_payment_ratio,
    a.avg_customer_review_score,
    a.avg_items_per_order,
    a.avg_freight_per_order,
    a.avg_sellers_per_order,
    a.avg_products_per_order,
    a.total_categories_purchased,
    a.high_value_order_ratio,
    a.total_reviews_given,

    -- ── ORDER STATUS BREAKDOWN ───────────────────────────────
    a.delivered_orders,
    a.cancelled_orders,
    a.shipped_orders,

    -- ── PREFERENCES ──────────────────────────────────────────
    p.preferred_category,
    d.preferred_day_of_week,

    -- ── AUDIT ────────────────────────────────────────────────
    NOW() AS mart_created_at

FROM tmp_customer_agg              a
LEFT JOIN tmp_customer_location    l  ON a.customer_unique_id = l.customer_unique_id
LEFT JOIN tmp_customer_rfm         f  ON a.customer_unique_id = f.customer_unique_id
LEFT JOIN tmp_customer_recency_windows w ON a.customer_unique_id = w.customer_unique_id
LEFT JOIN tmp_customer_preferred_cat  p  ON a.customer_unique_id = p.customer_unique_id
LEFT JOIN tmp_customer_preferred_day  d  ON a.customer_unique_id = d.customer_unique_id;


-- ============================================================
-- STEP 9: PRIMARY KEY + INDEXES
-- ============================================================

ALTER TABLE mart_customer_features
    ADD PRIMARY KEY (customer_unique_id);

CREATE INDEX idx_mcf_lifecycle     ON mart_customer_features (lifecycle_stage);
CREATE INDEX idx_mcf_segment       ON mart_customer_features (segment_label);
CREATE INDEX idx_mcf_cohort        ON mart_customer_features (cohort_month);
CREATE INDEX idx_mcf_rfm_score     ON mart_customer_features (rfm_score);
CREATE INDEX idx_mcf_state         ON mart_customer_features (customer_state);
CREATE INDEX idx_mcf_recency       ON mart_customer_features (recency_days);
CREATE INDEX idx_mcf_total_spend   ON mart_customer_features (total_spend);
CREATE INDEX idx_mcf_repeat        ON mart_customer_features (repeat_purchase_flag);


-- ============================================================
-- STEP 10: QUICK DISTRIBUTION PREVIEW
-- ============================================================

-- Lifecycle stage distribution
SELECT lifecycle_stage, COUNT(*) AS customers, ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER(), 0), 2) AS pct
FROM mart_customer_features
GROUP BY lifecycle_stage
ORDER BY customers DESC;

-- Segment distribution
SELECT segment_label, COUNT(*) AS customers, ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER(), 0), 2) AS pct
FROM mart_customer_features
GROUP BY segment_label
ORDER BY customers DESC;

-- RFM score histogram
SELECT rfm_score, COUNT(*) AS customers
FROM mart_customer_features
GROUP BY rfm_score
ORDER BY rfm_score;

-- Cohort size by month
SELECT cohort_month, COUNT(*) AS new_customers
FROM mart_customer_features
GROUP BY cohort_month
ORDER BY cohort_month;

-- ============================================================
-- END OF FILE 1
-- Next step: Run stage3_02_validation.sql
-- ============================================================
