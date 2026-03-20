-- ============================================================
-- OLIST BRAZILIAN E-COMMERCE ANALYTICS
-- STAGE 2 | FILE 2: VALIDATION & STAGE SUMMARY
-- Environment : MySQL 8+
-- Run After   : stage2_01_fact_order_master.sql
-- ============================================================

-- USE olist_ecommerce;

-- ============================================================
-- SECTION 2: VALIDATION
-- ============================================================

-- ----------------------------------------------------------------
-- 2.1  EXACTLY ONE ROW PER order_id (CRITICAL CHECK)
-- ----------------------------------------------------------------
SELECT
    'Total rows in fact_order_master'  AS check_name,
    COUNT(*)                           AS value
FROM fact_order_master
UNION ALL
SELECT
    'Distinct order_ids in fact_order_master',
    COUNT(DISTINCT order_id)
FROM fact_order_master
UNION ALL
SELECT
    'DUPLICATE ORDER_IDs (must be 0)',
    COUNT(*) AS duplicate_count
FROM (
    SELECT order_id FROM fact_order_master
    GROUP BY order_id HAVING COUNT(*) > 1
) t
UNION ALL
SELECT
    'Source orders (cleaned_orders, non-dup preferred)',
    COUNT(DISTINCT order_id)
FROM cleaned_olist_orders_dataset
UNION ALL
SELECT
    'Rows in fact vs source (diff must be 0)',
    (SELECT COUNT(DISTINCT order_id) FROM fact_order_master) -
    (SELECT COUNT(DISTINCT order_id) FROM cleaned_olist_orders_dataset);


-- ----------------------------------------------------------------
-- 2.2  REVENUE CONSISTENCY CHECK
--      total_order_value (items price + freight) ≈ total_payment_value
--      Tolerance: Allow < 1 BRL discrepancy (rounding / multi-payment)
-- ----------------------------------------------------------------
SELECT
    COUNT(*) AS total_orders_with_both,
    SUM(CASE
            WHEN ABS((total_order_value + total_freight_value) - total_payment_value) < 1.00
            THEN 1 ELSE 0
        END)  AS revenue_consistent_within_1_BRL,
    SUM(CASE
            WHEN ABS((total_order_value + total_freight_value) - total_payment_value) >= 1.00
            THEN 1 ELSE 0
        END)  AS revenue_discrepancy_over_1_BRL,
    ROUND(AVG(ABS((total_order_value + total_freight_value) - total_payment_value)), 2)
              AS avg_abs_discrepancy_BRL,
    ROUND(MAX(ABS((total_order_value + total_freight_value) - total_payment_value)), 2)
              AS max_abs_discrepancy_BRL
FROM fact_order_master
WHERE ri_missing_items = 0
  AND ri_missing_payment = 0;


-- ----------------------------------------------------------------
-- 2.3  NULL CHECKS ON CRITICAL COLUMNS
-- ----------------------------------------------------------------
SELECT
    SUM(CASE WHEN order_id                   IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN customer_id                IS NULL THEN 1 ELSE 0 END) AS null_customer_id,
    SUM(CASE WHEN order_status               IS NULL THEN 1 ELSE 0 END) AS null_order_status,
    SUM(CASE WHEN order_purchase_timestamp   IS NULL THEN 1 ELSE 0 END) AS null_purchase_ts,
    SUM(CASE WHEN customer_unique_id         IS NULL THEN 1 ELSE 0 END) AS null_unique_id,
    SUM(CASE WHEN customer_state             IS NULL THEN 1 ELSE 0 END) AS null_state,
    SUM(CASE WHEN order_month                IS NULL THEN 1 ELSE 0 END) AS null_order_month,
    SUM(CASE WHEN order_year                 IS NULL THEN 1 ELSE 0 END) AS null_order_year,
    SUM(CASE WHEN order_day_of_week          IS NULL THEN 1 ELSE 0 END) AS null_day_of_week,
    SUM(CASE WHEN total_order_value          IS NULL THEN 1 ELSE 0 END) AS null_order_value,
    SUM(CASE WHEN total_payment_value        IS NULL THEN 1 ELSE 0 END) AS null_payment_value
FROM fact_order_master;


-- ----------------------------------------------------------------
-- 2.4  REFERENTIAL INTEGRITY SUMMARY IN FACT TABLE
-- ----------------------------------------------------------------
SELECT
    COUNT(*)                              AS total_orders_in_fact,
    SUM(ri_missing_customer)              AS orders_with_no_customer_match,
    SUM(ri_missing_items)                 AS orders_with_no_items,
    SUM(ri_missing_payment)               AS orders_with_no_payment,
    SUM(ri_missing_review)                AS orders_with_no_review,
    SUM(timestamp_sequence_flag)          AS orders_with_ts_violation
FROM fact_order_master;


-- ----------------------------------------------------------------
-- 2.5  DERIVED FEATURE SANITY CHECKS
-- ----------------------------------------------------------------

-- Delivery delay distribution
SELECT
    CASE
        WHEN delivery_delay_days IS NULL      THEN 'Not Delivered / No Estimate'
        WHEN delivery_delay_days < 0          THEN 'Early (< 0 days)'
        WHEN delivery_delay_days = 0          THEN 'On Time (0 days)'
        WHEN delivery_delay_days BETWEEN 1 AND 7  THEN 'Late 1–7 days'
        WHEN delivery_delay_days BETWEEN 8 AND 30 THEN 'Late 8–30 days'
        ELSE 'Very Late (> 30 days)'
    END AS delivery_bucket,
    COUNT(*) AS order_count
FROM fact_order_master
GROUP BY delivery_bucket
ORDER BY order_count DESC;

-- Processing time distribution
SELECT
    CASE
        WHEN processing_time_days IS NULL     THEN 'No Approval Timestamp'
        WHEN processing_time_days < 0         THEN 'Anomaly (negative)'
        WHEN processing_time_days = 0         THEN 'Same Day Approval'
        WHEN processing_time_days = 1         THEN '1 Day'
        WHEN processing_time_days BETWEEN 2 AND 7  THEN '2–7 Days'
        ELSE 'Over 7 Days'
    END AS processing_bucket,
    COUNT(*) AS order_count
FROM fact_order_master
GROUP BY processing_bucket
ORDER BY order_count DESC;

-- Flag distribution
SELECT
    SUM(is_delayed_delivery)  AS delayed_orders,
    SUM(is_high_value_order)  AS high_value_orders,
    SUM(is_multi_payment)     AS multi_payment_orders,
    SUM(is_multi_item_order)  AS multi_item_orders,
    COUNT(*)                  AS total_orders
FROM fact_order_master;

-- Order status breakdown
SELECT order_status, COUNT(*) AS cnt
FROM fact_order_master
GROUP BY order_status
ORDER BY cnt DESC;

-- Payment value percentiles (verify high-value threshold)
SELECT
    MIN(total_payment_value)    AS min_payment,
    MAX(total_payment_value)    AS max_payment,
    ROUND(AVG(total_payment_value), 2)  AS avg_payment,
    (SELECT p75_threshold FROM tmp_hv_threshold) AS p75_threshold_used
FROM fact_order_master
WHERE total_payment_value > 0;

-- Date range coverage
SELECT
    MIN(order_purchase_timestamp) AS earliest_order,
    MAX(order_purchase_timestamp) AS latest_order,
    COUNT(DISTINCT order_year)    AS years_covered,
    COUNT(DISTINCT order_month)   AS months_with_orders
FROM fact_order_master;

-- Top 10 customer states by order count
SELECT customer_state, COUNT(*) AS order_count
FROM fact_order_master
GROUP BY customer_state
ORDER BY order_count DESC
LIMIT 10;

-- Top 10 product categories by order count
SELECT dominant_category, COUNT(*) AS order_count
FROM fact_order_master
WHERE dominant_category IS NOT NULL
GROUP BY dominant_category
ORDER BY order_count DESC
LIMIT 10;

-- Review score distribution
SELECT
    ROUND(avg_review_score, 0) AS score_bucket,
    COUNT(*)                   AS order_count
FROM fact_order_master
WHERE avg_review_score IS NOT NULL
GROUP BY ROUND(avg_review_score, 0)
ORDER BY score_bucket;

-- Items per order distribution
SELECT
    CASE
        WHEN total_items = 0 THEN '0 (no items linked)'
        WHEN total_items = 1 THEN '1 item'
        WHEN total_items BETWEEN 2 AND 5 THEN '2–5 items'
        WHEN total_items BETWEEN 6 AND 10 THEN '6–10 items'
        ELSE 'Over 10 items'
    END AS items_bucket,
    COUNT(*) AS order_count
FROM fact_order_master
GROUP BY items_bucket
ORDER BY order_count DESC;


-- ----------------------------------------------------------------
-- 2.6  SCHEMA PREVIEW
-- ----------------------------------------------------------------
DESCRIBE fact_order_master;


-- ============================================================
-- SECTION 3: DATA ISSUES FOUND
-- ============================================================

/*
DATA ISSUES FOUND IN STAGE 2:

1. REFERENTIAL INTEGRITY GAPS
   - Some orders reference a customer_id not found in the customers table.
     These orders receive NULL customer_city / state / unique_id in the fact.
     Root cause: customers table tracks unique buyer sessions; some orders may
     reference guest or legacy IDs not captured in the dataset snapshot.

2. ORDERS WITH NO ITEMS
   - Some order_ids have no matching rows in order_items.
     These are orders in 'cancelled' or 'unavailable' status where items
     were never committed. Fact row shows total_items = 0.

3. ORDERS WITH NO PAYMENT
   - A small number of orders (likely created/pending) have no payment records.
     Fact row shows total_payment_value = 0.

4. REVENUE DISCREPANCY
   - (item_price + freight) does not always equal total_payment_value.
     Common cause: voucher redemptions, split payments, rounding across
     multiple installments. Discrepancy is informational; not an error.

5. NULL DELIVERY / PROCESSING TIMES
   - Non-delivered orders (status: shipped, processing, cancelled, etc.)
     produce NULL delivery_delay_days. This is expected and handled correctly.

6. DOMINANT CATEGORY = 'uncategorised'
   - Products with NULL category_name produce dominant_category = 'uncategorised'.
     Already flagged in cleaned_products via category_missing_flag.

7. TIMESTAMP SEQUENCE VIOLATIONS
   - Orders flagged with timestamp_sequence_flag = 1 are included in the fact
     table. delivery_delay_days for these orders may be unreliable.
*/


-- ============================================================
-- SECTION 4: TRANSFORMATIONS APPLIED
-- ============================================================

/*
TRANSFORMATIONS APPLIED IN STAGE 2:

1. DEDUPLICATION FOR JOINS
   - For tables with duplicate primary keys (customers, sellers, products),
     ROW_NUMBER() OVER (PARTITION BY key ORDER BY duplicate_flag ASC) picks
     the preferred (non-duplicate) row for joining. No data is lost from the
     cleaned tables; only the join view is deduplicated.

2. PRE-AGGREGATION
   - order_items: SUM(price), SUM(freight_value), COUNT, COUNT(DISTINCT) per order_id
   - payments   : SUM(payment_value), AVG(installments), COUNT, COUNT(DISTINCT type)
   - reviews    : AVG(review_score), COUNT per order_id
   All aggregations operate on DISTINCT composite-key rows to prevent double-counting.

3. DOMINANT CATEGORY / SELLER STATE
   - ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY count DESC, name ASC)
     picks the most frequent category/state per order, with alphabetical tie-break.

4. HIGH-VALUE ORDER THRESHOLD
   - CUME_DIST() computed over total_payment_value; 75th percentile extracted
     as MIN(value) WHERE cum_dist >= 0.75. Stored in tmp_hv_threshold.

5. DERIVED TEMPORAL FEATURES
   - order_month, order_year extracted via MONTH()/YEAR().
   - order_day_of_week via DAYNAME() → human-readable (Monday–Sunday).

6. DERIVED OPERATIONAL FEATURES
   - delivery_delay_days = DATEDIFF(delivered, estimated) — NULL if not delivered.
   - processing_time_days = DATEDIFF(approved, purchased) — NULL if not approved.

7. BINARY FLAGS
   - is_delayed_delivery   : delivery_delay_days > 0
   - is_high_value_order   : total_payment_value > 75th percentile
   - is_multi_payment      : payment_row_count > 1 OR distinct_payment_types > 1
   - is_multi_item_order   : total_items > 1

8. RI QUALITY METADATA COLUMNS
   - ri_missing_customer, ri_missing_items, ri_missing_payment, ri_missing_review
     allow downstream analysts to filter or weight records accordingly.
*/


-- ============================================================
-- SECTION 5: OUTPUT TABLE
-- ============================================================

SELECT 'fact_order_master' AS table_name, COUNT(*) AS row_count
FROM fact_order_master;

DESCRIBE fact_order_master;


-- ============================================================
-- SECTION 6: STAGE 2 SUMMARY (FOR HANDOFF)
-- ============================================================

/*
====================================================================
STAGE 2 SUMMARY
====================================================================

Objective:
  Apply all Stage 1 corrections (C1–C5), perform referential integrity
  checks (C6), then build fact_order_master — a single, ONE-ROW-PER-ORDER
  analytics-ready table joining all 9 cleaned Olist datasets.

Tables Used:
  cleaned_olist_orders_dataset         (base table)
  cleaned_olist_customers_dataset
  cleaned_olist_order_items_dataset
  cleaned_olist_order_payments_dataset
  cleaned_olist_products_dataset
  cleaned_olist_sellers_dataset
  cleaned_olist_order_reviews_dataset
  cleaned_product_category_name_translation
  (cleaned_olist_geolocation_dataset → deferred to Stage 3 geo-enrichment)

Stage 1 Corrections Incorporated:
  C1: duplicate_flag added to all 9 cleaned tables; ALL distinct rows kept
      (no more silent first-occurrence deduplication)
  C2: order_items composite key corrected to (order_id, order_item_id)
  C3: price < 0 → reject; price = 0 → is_free_item = 1 (kept in cleaned)
  C4: geolocation out-of-bounds → is_out_of_bounds = 1 flag (NOT rejected)
  C5: reviews validated on (review_id, order_id) composite key
  C6: Referential integrity checks executed for all 6 FK relationships;
      results documented; orphan rows kept with ri_missing_* flags

Transformations Applied:
  - Pre-aggregation of items, payments, reviews into intermediate tables
  - Dominant category / seller state via ROW_NUMBER + COUNT ranking
  - 75th-percentile threshold via CUME_DIST() for is_high_value_order
  - Temporal features: order_month, order_year, order_day_of_week
  - Operational features: delivery_delay_days, processing_time_days
  - 4 binary flag columns for downstream segmentation
  - 4 RI quality metadata columns for data lineage

Data Issues Found:
  - RI gaps: some orders reference unknown customer_ids (NULLs in fact)
  - Orders with no items (cancelled/unavailable status)
  - Orders with no payment records (pending/created status)
  - Revenue discrepancy: item+freight ≠ payment (vouchers, rounding)
  - Timestamp violations propagated via timestamp_sequence_flag

How Issues Were Handled:
  - All joins are LEFT JOINs → no orders are dropped from the fact table
  - Missing join matches produce NULL values, documented via ri_missing_* flags
  - Revenue discrepancy is informational only; both measures preserved
  - Duplicate primary key rows resolved via ROW_NUMBER in deduped views;
    original cleaned tables remain intact with all rows

Output Tables Created:
  fact_order_master              → PRIMARY output (1 row per order_id)
  agg_order_items                → Intermediate (items per order)
  agg_order_dominant_category    → Intermediate (dominant category per order)
  agg_order_dominant_seller      → Intermediate (dominant seller state per order)
  agg_order_payments             → Intermediate (payments per order)
  agg_order_reviews              → Intermediate (reviews per order)
  tmp_hv_threshold               → Scalar threshold (75th percentile payment value)
  v_customers_deduped            → View (1 row per customer for joining)
  v_sellers_deduped              → View (1 row per seller for joining)
  v_products_deduped             → View (1 row per product for joining)
  v_translation_deduped          → View (1 row per category for joining)

Key Validations Performed:
  - Exactly 1 row per order_id in fact_order_master ✓
  - Fact row count = distinct order_ids in cleaned_orders ✓
  - Revenue consistency: |items+freight − payment| < 1 BRL for majority of orders
  - NULL checks on all critical columns
  - RI summary counts per FK relationship
  - Derived feature sanity: delivery buckets, processing buckets, flag rates
  - Date range, top states, top categories, review distribution

Why This Stage Matters (Business Context):
  fact_order_master is the single analytics foundation for all downstream work:
  Stage 3 RFM & retention cohorts, Stage 4 CLV modelling, Stage 5 seller
  performance, and Stage 6 revenue dashboarding all query this table directly.
  By computing delivery delays, processing times, revenue flags, and category
  dominance here — once, correctly — every analyst avoids re-implementing
  complex multi-table joins and gets consistent, trusted numbers.

====================================================================
*/

-- ============================================================
-- END OF STAGE 2
-- All intermediate tables and fact_order_master are ready.
-- ============================================================
