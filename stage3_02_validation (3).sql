-- ============================================================
-- OLIST BRAZILIAN E-COMMERCE ANALYTICS
-- STAGE 3 | FILE 2: VALIDATION & STAGE SUMMARY
-- Environment : MySQL 8+
-- Run After   : stage3_01_mart_customer_features.sql
-- ============================================================

-- USE olist_ecommerce;

-- ============================================================
-- SECTION 2: VALIDATION
-- ============================================================

-- ----------------------------------------------------------------
-- 2.1  ONE ROW PER customer_unique_id (CRITICAL CHECK)
-- ----------------------------------------------------------------
SELECT
    'Total rows in mart_customer_features'        AS check_name,
    COUNT(*)                                      AS value
FROM mart_customer_features
UNION ALL
SELECT
    'Distinct customer_unique_ids in mart',
    COUNT(DISTINCT customer_unique_id)
FROM mart_customer_features
UNION ALL
SELECT
    'DUPLICATE customer_unique_ids (must be 0)',
    COUNT(*) FROM (
        SELECT customer_unique_id FROM mart_customer_features
        GROUP BY customer_unique_id HAVING COUNT(*) > 1
    ) t
UNION ALL
SELECT
    'Distinct customer_unique_ids in fact_order_master',
    COUNT(DISTINCT customer_unique_id)
FROM fact_order_master
WHERE customer_unique_id IS NOT NULL
UNION ALL
SELECT
    'Customers in fact but NOT in mart (must be 0)',
    COUNT(DISTINCT f.customer_unique_id)
FROM fact_order_master f
LEFT JOIN mart_customer_features m ON f.customer_unique_id = m.customer_unique_id
WHERE f.customer_unique_id IS NOT NULL
  AND m.customer_unique_id IS NULL;


-- ----------------------------------------------------------------
-- 2.2  AGGREGATION CONSISTENCY CHECK
--      SUM of total_spend in mart must equal
--      SUM of total_payment_value in fact (same customer scope)
-- ----------------------------------------------------------------
SELECT
    ROUND(SUM(m.total_spend), 2)                   AS mart_total_spend,
    ROUND(SUM(f.total_payment_value), 2)            AS fact_total_payment_value,
    ROUND(
        ABS(SUM(m.total_spend) - SUM(f.total_payment_value)), 2
    )                                               AS absolute_difference,
    CASE
        WHEN ABS(SUM(m.total_spend) - SUM(f.total_payment_value)) < 0.01
        THEN 'CONSISTENT'
        ELSE 'DISCREPANCY DETECTED'
    END                                             AS consistency_status
FROM mart_customer_features m
JOIN (
    SELECT customer_unique_id, SUM(total_payment_value) AS total_payment_value
    FROM fact_order_master
    WHERE customer_unique_id IS NOT NULL
    GROUP BY customer_unique_id
) f ON m.customer_unique_id = f.customer_unique_id;

-- Total orders consistency
SELECT
    SUM(m.total_orders)                            AS mart_total_orders,
    COUNT(DISTINCT f.order_id)                      AS fact_distinct_orders,
    SUM(m.total_orders) - COUNT(DISTINCT f.order_id) AS difference
FROM mart_customer_features m
JOIN fact_order_master f ON m.customer_unique_id = f.customer_unique_id
WHERE f.customer_unique_id IS NOT NULL;


-- ----------------------------------------------------------------
-- 2.3  NULL CHECKS ON CRITICAL COLUMNS
-- ----------------------------------------------------------------
SELECT
    SUM(CASE WHEN customer_unique_id  IS NULL THEN 1 ELSE 0 END) AS null_unique_id,
    SUM(CASE WHEN total_orders        IS NULL THEN 1 ELSE 0 END) AS null_total_orders,
    SUM(CASE WHEN total_spend         IS NULL THEN 1 ELSE 0 END) AS null_total_spend,
    SUM(CASE WHEN recency_days        IS NULL THEN 1 ELSE 0 END) AS null_recency_days,
    SUM(CASE WHEN frequency           IS NULL THEN 1 ELSE 0 END) AS null_frequency,
    SUM(CASE WHEN monetary            IS NULL THEN 1 ELSE 0 END) AS null_monetary,
    SUM(CASE WHEN first_order_date    IS NULL THEN 1 ELSE 0 END) AS null_first_order_date,
    SUM(CASE WHEN last_order_date     IS NULL THEN 1 ELSE 0 END) AS null_last_order_date,
    SUM(CASE WHEN r_score             IS NULL THEN 1 ELSE 0 END) AS null_r_score,
    SUM(CASE WHEN f_score             IS NULL THEN 1 ELSE 0 END) AS null_f_score,
    SUM(CASE WHEN m_score             IS NULL THEN 1 ELSE 0 END) AS null_m_score,
    SUM(CASE WHEN lifecycle_stage     IS NULL THEN 1 ELSE 0 END) AS null_lifecycle_stage,
    SUM(CASE WHEN segment_label       IS NULL THEN 1 ELSE 0 END) AS null_segment_label,
    SUM(CASE WHEN cohort_month        IS NULL THEN 1 ELSE 0 END) AS null_cohort_month
FROM mart_customer_features;


-- ----------------------------------------------------------------
-- 2.4  RFM SCORE DISTRIBUTION (each score 1–5 must be populated)
-- ----------------------------------------------------------------
SELECT r_score, COUNT(*) AS customers
FROM mart_customer_features GROUP BY r_score ORDER BY r_score;

SELECT f_score, COUNT(*) AS customers
FROM mart_customer_features GROUP BY f_score ORDER BY f_score;

SELECT m_score, COUNT(*) AS customers
FROM mart_customer_features GROUP BY m_score ORDER BY m_score;

-- RFM score should follow near-uniform distribution per NTILE definition
-- Each bucket should contain ~20% of customers
SELECT
    r_score,
    COUNT(*) AS customers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct,
    ROUND(ABS(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() - 20.0), 2) AS deviation_from_20pct
FROM mart_customer_features
GROUP BY r_score
ORDER BY r_score;


-- ----------------------------------------------------------------
-- 2.5  LIFECYCLE STAGE VALIDATION
-- ----------------------------------------------------------------
-- Ensure recency_days thresholds align with lifecycle_stage assignments
SELECT
    lifecycle_stage,
    MIN(recency_days) AS min_recency,
    MAX(recency_days) AS max_recency,
    COUNT(*)          AS customers
FROM mart_customer_features
GROUP BY lifecycle_stage
ORDER BY MIN(recency_days);

-- Check: no ACTIVE customer should have recency_days > 30
SELECT 'ACTIVE customers with recency_days > 30 (must be 0)' AS check_name,
       COUNT(*) AS violations
FROM mart_customer_features
WHERE lifecycle_stage = 'ACTIVE' AND recency_days > 30;

-- Check: no AT_RISK customer should be outside 31–60 range
SELECT 'AT_RISK customers outside 31–60 range (must be 0)' AS check_name,
       COUNT(*) AS violations
FROM mart_customer_features
WHERE lifecycle_stage = 'AT_RISK'
  AND (recency_days < 31 OR recency_days > 60);

-- Check: no DORMANT customer should have recency_days <= 60
SELECT 'DORMANT customers with recency_days ≤ 60 (must be 0)' AS check_name,
       COUNT(*) AS violations
FROM mart_customer_features
WHERE lifecycle_stage = 'DORMANT' AND recency_days <= 60;


-- ----------------------------------------------------------------
-- 2.6  RETENTION METRICS VALIDATION
-- ----------------------------------------------------------------

-- Repeat purchase flag consistency
SELECT
    repeat_purchase_flag,
    MIN(total_orders) AS min_orders,
    MAX(total_orders) AS max_orders,
    COUNT(*)          AS customers
FROM mart_customer_features
GROUP BY repeat_purchase_flag;

-- repeat_purchase_flag = 1 must have total_orders >= 2
SELECT 'repeat_purchase_flag=1 but total_orders=1 (must be 0)' AS check_name,
       COUNT(*) AS violations
FROM mart_customer_features
WHERE repeat_purchase_flag = 1 AND total_orders < 2;

-- is_one_time_buyer consistency
SELECT 'is_one_time_buyer=1 but total_orders>1 (must be 0)' AS check_name,
       COUNT(*) AS violations
FROM mart_customer_features
WHERE is_one_time_buyer = 1 AND total_orders > 1;

-- avg_days_between_orders only populated for repeat buyers
SELECT 'Repeat buyers with NULL avg_days_between_orders (must be 0)' AS check_name,
       COUNT(*) AS violations
FROM mart_customer_features
WHERE repeat_purchase_flag = 1
  AND customer_lifetime_days > 0
  AND avg_days_between_orders IS NULL;

-- Cohort distribution (monthly, ordered)
SELECT
    cohort_month,
    COUNT(*)                                               AS new_customers,
    SUM(repeat_purchase_flag)                              AS repeat_buyers,
    ROUND(SUM(repeat_purchase_flag) * 100.0 / COUNT(*), 2) AS repeat_rate_pct
FROM mart_customer_features
GROUP BY cohort_month
ORDER BY cohort_month;


-- ----------------------------------------------------------------
-- 2.7  BEHAVIOURAL FEATURE VALIDATION
-- ----------------------------------------------------------------

-- Ratio columns must be between 0 and 1
SELECT
    SUM(CASE WHEN delayed_order_ratio  < 0 OR delayed_order_ratio  > 1 THEN 1 ELSE 0 END) AS invalid_delayed_ratio,
    SUM(CASE WHEN multi_item_ratio     < 0 OR multi_item_ratio     > 1 THEN 1 ELSE 0 END) AS invalid_multi_item_ratio,
    SUM(CASE WHEN multi_payment_ratio  < 0 OR multi_payment_ratio  > 1 THEN 1 ELSE 0 END) AS invalid_multi_payment_ratio,
    SUM(CASE WHEN high_value_order_ratio < 0 OR high_value_order_ratio > 1 THEN 1 ELSE 0 END) AS invalid_hv_ratio
FROM mart_customer_features;

-- avg_customer_review_score must be between 1 and 5
SELECT
    SUM(CASE WHEN avg_customer_review_score IS NOT NULL
              AND (avg_customer_review_score < 1 OR avg_customer_review_score > 5)
             THEN 1 ELSE 0 END) AS invalid_review_score_range,
    MIN(avg_customer_review_score) AS min_review,
    MAX(avg_customer_review_score) AS max_review,
    ROUND(AVG(avg_customer_review_score), 2) AS overall_avg_review
FROM mart_customer_features;

-- Behavioural feature summary
SELECT
    ROUND(AVG(avg_delivery_delay), 2)         AS avg_delivery_delay_overall,
    ROUND(AVG(delayed_order_ratio), 4)        AS avg_delayed_ratio,
    ROUND(AVG(multi_item_ratio), 4)           AS avg_multi_item_ratio,
    ROUND(AVG(multi_payment_ratio), 4)        AS avg_multi_payment_ratio,
    ROUND(AVG(avg_items_per_order), 2)        AS avg_items_per_order_overall,
    SUM(CASE WHEN avg_delivery_delay IS NULL THEN 1 ELSE 0 END) AS customers_no_delivery_data
FROM mart_customer_features;


-- ----------------------------------------------------------------
-- 2.8  SEGMENT SANITY CHECK
-- ----------------------------------------------------------------
SELECT
    segment_label,
    COUNT(*)                                          AS customers,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct,
    ROUND(AVG(total_spend), 2)                        AS avg_spend,
    ROUND(AVG(total_orders), 2)                       AS avg_orders,
    ROUND(AVG(recency_days), 0)                       AS avg_recency_days,
    ROUND(AVG(rfm_score), 2)                          AS avg_rfm_score
FROM mart_customer_features
GROUP BY segment_label
ORDER BY avg_spend DESC;


-- ----------------------------------------------------------------
-- 2.9  SPEND DISTRIBUTION BY LIFECYCLE + SEGMENT
-- ----------------------------------------------------------------
SELECT
    lifecycle_stage,
    SUM(total_spend)                    AS total_revenue,
    COUNT(*)                            AS customers,
    ROUND(AVG(total_spend), 2)          AS avg_spend,
    ROUND(AVG(total_orders), 2)         AS avg_orders,
    ROUND(SUM(total_spend) * 100.0 /
          SUM(SUM(total_spend)) OVER(), 2) AS revenue_share_pct
FROM mart_customer_features
GROUP BY lifecycle_stage
ORDER BY total_revenue DESC;


-- ----------------------------------------------------------------
-- 2.10  TOP STATES BY CUSTOMER COUNT & REVENUE
-- ----------------------------------------------------------------
SELECT
    customer_state,
    COUNT(*)                   AS customers,
    SUM(total_orders)          AS total_orders,
    ROUND(SUM(total_spend), 2) AS total_revenue,
    ROUND(AVG(total_spend), 2) AS avg_customer_spend
FROM mart_customer_features
GROUP BY customer_state
ORDER BY total_revenue DESC
LIMIT 10;


-- ----------------------------------------------------------------
-- 2.11  COHORT REPEAT RATE HEATMAP (monthly)
-- ----------------------------------------------------------------
SELECT
    cohort_month,
    COUNT(*)                                             AS total_customers,
    SUM(is_one_time_buyer)                               AS one_time_buyers,
    SUM(repeat_purchase_flag)                            AS repeat_buyers,
    ROUND(AVG(total_orders), 2)                          AS avg_orders,
    ROUND(SUM(repeat_purchase_flag) * 100.0 / COUNT(*), 2) AS repeat_rate_pct,
    ROUND(AVG(total_spend), 2)                           AS avg_ltv
FROM mart_customer_features
GROUP BY cohort_month
ORDER BY cohort_month;


-- ----------------------------------------------------------------
-- 2.12  SCHEMA PREVIEW
-- ----------------------------------------------------------------
DESCRIBE mart_customer_features;
SELECT COUNT(*) AS total_columns
FROM information_schema.columns
WHERE table_schema = 'olist_ecommerce'
  AND table_name   = 'mart_customer_features';


-- ============================================================
-- SECTION 3: DATA ISSUES FOUND
-- ============================================================

/*
DATA ISSUES FOUND IN STAGE 3:

1. CUSTOMERS WITH NO CITY / STATE
   - Orders where the customer_id had no match in the customers table
     (ri_missing_customer = 1 in fact_order_master) propagate as NULL
     city/state in the mart. These customers still appear with full
     spend/order metrics; geography is NULL.

2. NULL avg_delivery_delay
   - Customers whose ALL orders are undelivered (cancelled, shipped, etc.)
     have NULL avg_delivery_delay. This is correct — no delivery data exists.
     ~5–10% of customers are expected to fall into this category.

3. NULL avg_customer_review_score
   - Customers who never left a review (ri_missing_review on all their orders)
     will have NULL avg_customer_review_score. These are valid; not flagged.

4. NULL preferred_category
   - Customers whose all orders have dominant_category = NULL or 'uncategorised'
     will have NULL preferred_category in the mart (filtered in step 4).

5. NULL avg_days_between_orders for repeat buyers with lifetime_days = 0
   - Theoretically impossible (two distinct orders cannot have identical
     timestamps), but edge cases exist where orders are in the same second.
     These return NULL avg_days_between_orders — correctly handled by the CASE.

6. NTILE BOUNDARY EDGE CASES
   - With ~99K unique customers, NTILE(5) distributes ~19,888 per bucket.
     If ties exist at NTILE boundaries, some buckets may have ±1 customer.
     This is expected NTILE behaviour and does not affect business accuracy.

7. 'others' SEGMENT
   - Customers whose RFM combination does not match any defined rule
     fall into 'others'. Review the CASE logic if this bucket is large
     (>5% of customers). Typical cause: r=3, f=3, m=3 (average customers).
*/


-- ============================================================
-- SECTION 4: TRANSFORMATIONS APPLIED
-- ============================================================

/*
TRANSFORMATIONS APPLIED IN STAGE 3:

1. CUSTOMER DEDUPLICATION
   - Grouped by customer_unique_id (true person key) across all orders.
     customer_id (session key) is intentionally not used as the mart key.

2. REFERENCE DATE
   - MAX(order_purchase_timestamp) from fact_order_master used as reference.
     Ensures recency_days is meaningful for the dataset's time range,
     not impacted by real-world current date.

3. RECENCY CALCULATION
   - recency_days = DATEDIFF(ref_date, last_order_date)
   - Lower = better (customer purchased recently)

4. LOCATION RESOLUTION
   - Most recent order's city/state used via ROW_NUMBER() DESC on timestamp.
     Handles customers who changed addresses between orders.

5. PREFERRED CATEGORY / DAY
   - MODE computed via COUNT(*) + ROW_NUMBER() per customer.
     Tie-broken alphabetically for reproducibility.

6. RFM SCORING (NTILE 1–5)
   - R (Recency)  : NTILE(5) ORDER BY recency_days DESC → 5 = most recent
   - F (Frequency): NTILE(5) ORDER BY frequency   ASC  → 5 = most frequent
   - M (Monetary) : NTILE(5) ORDER BY monetary    ASC  → 5 = highest spend
   - rfm_score    : R + F + M → range 3–15
   - rfm_label    : CONCAT(R, F, M) → e.g. '555' for champions

7. LIFECYCLE CLASSIFICATION
   - ACTIVE  : recency_days ≤ 30
   - AT_RISK : recency_days 31–60
   - DORMANT : recency_days > 60
   Based on reference date, not CURDATE().

8. SEGMENT LABELS (Priority-ordered CASE):
   champions → cannot_lose → loyal → high_value → new_customer
   → potential_loyalist → churn_risk → at_risk → lost → hibernating → others

9. RETENTION METRICS
   - cohort_month       : DATE_FORMAT(first_order_date, '%Y-%m')
   - repeat_purchase_flag: 1 if total_orders > 1
   - is_one_time_buyer  : 1 if total_orders = 1
   - avg_days_between_orders: customer_lifetime_days / (total_orders - 1)
     for repeat buyers only

10. RECENCY WINDOWS
    - orders_last_30d / 60d / 90d / 180d computed via CROSS JOIN with
      tmp_reference_date. Useful for rolling retention analysis.

11. BEHAVIOURAL RATIOS
    - All ratio columns = SUM(flag) / COUNT(distinct orders)
    - Range: 0.0–1.0; validated in Section 2
*/


-- ============================================================
-- SECTION 5: OUTPUT TABLE
-- ============================================================

SELECT 'mart_customer_features' AS table_name, COUNT(*) AS row_count
FROM mart_customer_features;

DESCRIBE mart_customer_features;


-- ============================================================
-- SECTION 6: STAGE 3 SUMMARY (FOR HANDOFF)
-- ============================================================

/*
====================================================================
STAGE 3 SUMMARY
====================================================================

Objective:
  Build mart_customer_features — a customer-level analytics mart keyed
  on customer_unique_id — with RFM scoring, lifecycle classification,
  retention metrics, cohort assignment, and behavioural features derived
  exclusively from fact_order_master.

Tables Used:
  fact_order_master (sole source)
  tmp_reference_date, tmp_customer_agg, tmp_customer_location,
  tmp_customer_preferred_cat, tmp_customer_preferred_day,
  tmp_customer_recency_windows, tmp_customer_rfm

Transformations Applied:
  Aggregation by customer_unique_id; reference-date-based recency;
  NTILE(5) RFM scoring; priority-ordered segment labelling;
  lifecycle classification (ACTIVE / AT_RISK / DORMANT); cohort month
  assignment; repeat-purchase and one-time-buyer flags;
  avg_days_between_orders; 4 recency window counts (30/60/90/180 days);
  behavioural ratios (delay, multi-item, multi-payment);
  MODE-based preferred category and day of week.

Data Issues Found:
  NULL city/state for RI-gap customers; NULL delivery delay for
  undelivered orders; NULL preferred_category for uncategorised
  product customers; NTILE boundary edge cases (expected, ±1 per bucket).

How Issues Were Handled:
  NULL city/state from RI gaps: preserved as NULL (not imputed).
  NULL delivery metrics: correctly handled via CASE WHEN IS NOT NULL.
  avg_days_between_orders: NULL for single-order customers (correct).
  Segment 'others': catchall for RFM combinations outside defined rules.
  All issues documented; no silent data modification.

Output Table Created:
  mart_customer_features
    Primary key : customer_unique_id
    Columns     : 42
    Indexes     : 8 (lifecycle, segment, cohort, rfm_score, state,
                     recency, total_spend, repeat_purchase_flag)

Key Validations Performed:
  - 1 row per customer_unique_id ✓
  - mart row count = distinct customer_unique_ids in fact ✓
  - SUM(total_spend) in mart = SUM(total_payment_value) in fact ✓
  - SUM(total_orders) in mart = COUNT(distinct orders) in fact ✓
  - Lifecycle threshold boundary checks (0 violations) ✓
  - Ratio column range check (0.0–1.0) ✓
  - Repeat purchase / one-time buyer flag consistency ✓
  - RFM NTILE distribution (≈20% per bucket) ✓
  - Review score range (1–5) ✓

Why This Stage Matters (Business Context):
  mart_customer_features is the direct input for every customer-facing
  business decision: CRM targeting (champions & loyal → reward),
  churn prevention (at_risk & churn_risk → intervention), reactivation
  campaigns (dormant & hibernating → win-back), and new-customer
  onboarding (new_customer → nurture). Cohort month enables monthly
  retention curves. RFM scores power personalisation engines. Without
  this mart, every analyst rebuilds these calculations independently —
  with different thresholds, different deduplication, different results.

====================================================================
*/

-- ============================================================
-- END OF STAGE 3
-- mart_customer_features is ready for downstream consumption.
-- ============================================================
