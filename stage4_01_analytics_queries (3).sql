-- ============================================================
-- OLIST BRAZILIAN E-COMMERCE ANALYTICS
-- STAGE 4 | FILE 1: ANALYTICS QUERIES
-- Source  : mart_customer_features + fact_order_master ONLY
-- Environment : MySQL 8+
-- Run After   : stage3_01_mart_customer_features.sql
-- ============================================================
-- COVERAGE:
--   Block 1  : Executive KPIs
--   Block 2  : Pareto / Value Concentration
--   Block 3  : Cumulative Revenue Distribution
--   Block 4  : Customer Segmentation Deep Dive
--   Block 5  : Lifecycle Analysis
--   Block 6  : Cohort Retention Matrix
--   Block 7  : Retention Trends Over Time
--   Block 8  : Delivery Risk & Behavioral Analysis
--   Block 9  : Temporal Order Trends (MoM)
--   Block 10 : Geographic Distribution
--   Block 11 : One-Time vs Repeat Buyer Comparison
--   Block 12 : Segment × Lifecycle Cross-Matrix
-- ============================================================

USE olist_ecommerce;

-- ============================================================
-- BLOCK 1: EXECUTIVE KPI LAYER
-- ============================================================

-- 1.1  Top-Level KPI Summary Card
-- ─────────────────────────────────────────────────────────────
SELECT
    COUNT(DISTINCT customer_unique_id)                              AS total_unique_customers,
    ROUND(SUM(total_spend), 2)                                      AS total_revenue_BRL,
    ROUND(AVG(avg_order_value), 2)                                  AS overall_avg_order_value,
    ROUND(MEDIAN_WORKAROUND.median_order, 2)                        AS median_order_value,
    ROUND(SUM(total_orders), 0)                                     AS total_orders_placed,
    ROUND(SUM(total_spend) / NULLIF(SUM(total_orders), 0), 2)                  AS revenue_per_order,

    -- Engagement
    ROUND(SUM(repeat_purchase_flag)   * 100.0 / NULLIF(COUNT(*), 0), 2)        AS repeat_customer_pct,
    ROUND(SUM(is_one_time_buyer)      * 100.0 / NULLIF(COUNT(*), 0), 2)        AS one_time_buyer_pct,

    -- Lifecycle mix
    ROUND(SUM(CASE WHEN lifecycle_stage = 'ACTIVE'   THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS active_pct,
    ROUND(SUM(CASE WHEN lifecycle_stage = 'AT_RISK'  THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS at_risk_pct,
    ROUND(SUM(CASE WHEN lifecycle_stage = 'DORMANT'  THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS dormant_pct,

    -- Quality
    ROUND(AVG(avg_customer_review_score), 2)                        AS avg_review_score,
    ROUND(AVG(avg_delivery_delay), 2)                               AS avg_delivery_delay_days,
    ROUND(AVG(delayed_order_ratio)     * 100, 2)                    AS avg_delay_rate_pct,

    -- Cohort span
    MIN(cohort_month)                                               AS first_customer_cohort,
    MAX(cohort_month)                                               AS last_customer_cohort

FROM mart_customer_features
CROSS JOIN (
    -- Median workaround: midpoint of sorted spend
    SELECT AVG(avg_order_value) AS median_order
    FROM (
        SELECT avg_order_value,
               ROW_NUMBER() OVER (ORDER BY avg_order_value)  AS rn,
               COUNT(*) OVER ()                              AS total_cnt
        FROM mart_customer_features
        WHERE avg_order_value IS NOT NULL
    ) t
    WHERE rn IN (FLOOR((total_cnt + 1) / NULLIF(2, 0)), CEIL((total_cnt + 1) / NULLIF(2, 0)))
) MEDIAN_WORKAROUND;


-- 1.2  Revenue Concentration — at a glance
-- ─────────────────────────────────────────────────────────────
WITH decile_spend AS (
    SELECT
        NTILE(10) OVER (ORDER BY total_spend DESC) AS spend_decile,
        total_spend
    FROM mart_customer_features
    WHERE total_spend > 0
)
SELECT
    spend_decile                                                         AS customer_decile,
    COUNT(*)                                                             AS customer_count,
    ROUND(SUM(total_spend), 2)                                           AS decile_revenue,
    ROUND(SUM(total_spend) * 100.0 / NULLIF(SUM(SUM(total_spend)) OVER (), 0), 2)  AS revenue_share_pct,
    ROUND(SUM(SUM(total_spend)) OVER (ORDER BY spend_decile
          ROWS UNBOUNDED PRECEDING) * 100.0
          / NULLIF(SUM(SUM(total_spend)) OVER (), 0), 2)                            AS cumulative_revenue_pct
FROM decile_spend
GROUP BY spend_decile
ORDER BY spend_decile;


-- ============================================================
-- BLOCK 2: PARETO ANALYSIS — TOP 20% CUSTOMER CONTRIBUTION
-- ============================================================

-- 2.1  Customer-level spend rank with cumulative revenue %
-- ─────────────────────────────────────────────────────────────
WITH ranked_customers AS (
    SELECT
        customer_unique_id,
        segment_label,
        lifecycle_stage,
        total_spend,
        total_orders,
        ROW_NUMBER()  OVER (ORDER BY total_spend DESC)   AS spend_rank,
        COUNT(*)      OVER ()                            AS total_customers,
        SUM(total_spend) OVER (
            ORDER BY total_spend DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )                                                AS cumulative_spend,
        SUM(total_spend) OVER ()                         AS grand_total
    FROM mart_customer_features
    WHERE total_spend > 0
),
pareto AS (
    SELECT *,
        ROUND(spend_rank * 100.0 / NULLIF(total_customers, 0), 4)      AS customer_pct,
        ROUND(cumulative_spend * 100.0 / NULLIF(grand_total, 0),  2)   AS cumulative_revenue_pct
    FROM ranked_customers
)
-- Pareto breakpoints — how much revenue from top N% of customers
SELECT
    CONCAT('Top ', ROUND(customer_pct, 0), '%')   AS customer_band,
    MAX(spend_rank)                               AS max_customer_rank,
    ROUND(MAX(cumulative_revenue_pct), 2)         AS cumulative_revenue_pct,
    COUNT(*)                                      AS customers_in_band
FROM pareto
WHERE ROUND(customer_pct, 0) IN (1, 5, 10, 15, 20, 25, 30, 40, 50, 75, 100)
GROUP BY ROUND(customer_pct, 0)
ORDER BY ROUND(customer_pct, 0);


-- 2.2  Find the exact "80% revenue threshold" customer count
-- ─────────────────────────────────────────────────────────────
WITH ranked AS (
    SELECT
        spend_rank,
        total_customers,
        cumulative_revenue_pct,
        customer_pct
    FROM (
        SELECT
            ROW_NUMBER() OVER (ORDER BY total_spend DESC)   AS spend_rank,
            COUNT(*)     OVER ()                            AS total_customers,
            ROUND(
                SUM(total_spend) OVER (ORDER BY total_spend DESC
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                * 100.0 / NULLIF(SUM(total_spend) OVER (), 0), 2
            )                                               AS cumulative_revenue_pct,
            ROUND(ROW_NUMBER() OVER (ORDER BY total_spend DESC) * 100.0
                  / NULLIF(COUNT(*) OVER (), 0), 2)                    AS customer_pct
        FROM mart_customer_features
        WHERE total_spend > 0
    ) t
)
SELECT
    MIN(spend_rank)                       AS customers_needed_for_80pct_revenue,
    ROUND(MIN(customer_pct), 2)           AS customer_percentile_that_drives_80pct,
    MIN(cumulative_revenue_pct)           AS actual_cumulative_revenue_pct
FROM ranked
WHERE cumulative_revenue_pct >= 80
LIMIT 1;


-- ============================================================
-- BLOCK 3: CUSTOMER SEGMENTATION DEEP DIVE
-- ============================================================

-- 3.1  Segment-level metrics (counts, revenue, behaviour)
-- ─────────────────────────────────────────────────────────────
SELECT
    segment_label,
    COUNT(*)                                                              AS customer_count,
    ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER (), 0), 2)                   AS customer_share_pct,
    ROUND(SUM(total_spend), 2)                                            AS segment_revenue,
    ROUND(SUM(total_spend) * 100.0 / NULLIF(SUM(SUM(total_spend)) OVER (), 0), 2)   AS revenue_share_pct,
    ROUND(AVG(avg_order_value), 2)                                        AS avg_order_value,
    ROUND(AVG(total_orders), 2)                                           AS avg_orders_per_customer,
    ROUND(AVG(recency_days), 0)                                           AS avg_recency_days,
    ROUND(AVG(rfm_score), 2)                                              AS avg_rfm_score,
    ROUND(SUM(repeat_purchase_flag) * 100.0 / NULLIF(COUNT(*), 0), 2)               AS repeat_rate_pct,
    ROUND(AVG(avg_customer_review_score), 2)                              AS avg_review_score,
    ROUND(AVG(delayed_order_ratio) * 100, 2)                             AS avg_delay_rate_pct,
    ROUND(AVG(customer_lifetime_days), 0)                                 AS avg_lifetime_days
FROM mart_customer_features
GROUP BY segment_label
ORDER BY segment_revenue DESC;


-- 3.2  Segment revenue ranking with share gap
-- ─────────────────────────────────────────────────────────────
WITH seg AS (
    SELECT
        segment_label,
        SUM(total_spend)                                                    AS revenue,
        COUNT(*)                                                            AS customers,
        ROUND(SUM(total_spend) * 100.0 / NULLIF(SUM(SUM(total_spend)) OVER(), 0), 2)  AS revenue_pct,
        ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER(), 0), 2)                  AS customer_pct
    FROM mart_customer_features
    GROUP BY segment_label
)
SELECT
    segment_label,
    ROUND(revenue, 2)                                                AS segment_revenue,
    customers,
    revenue_pct,
    customer_pct,
    ROUND(revenue_pct - customer_pct, 2)                             AS revenue_customer_gap,
    DENSE_RANK() OVER (ORDER BY revenue DESC)                        AS revenue_rank,
    ROUND(SUM(revenue_pct) OVER (ORDER BY revenue DESC
          ROWS UNBOUNDED PRECEDING), 2)                              AS cumulative_revenue_pct
FROM seg
ORDER BY revenue DESC;


-- 3.3  Top 5 customers per segment (by spend)
-- ─────────────────────────────────────────────────────────────
SELECT segment_label, customer_unique_id, total_spend, total_orders,
       recency_days, rfm_label, lifecycle_stage
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY segment_label ORDER BY total_spend DESC) AS rn
    FROM mart_customer_features
) t
WHERE rn <= 5
ORDER BY segment_label, total_spend DESC;


-- ============================================================
-- BLOCK 4: LIFECYCLE ANALYSIS
-- ============================================================

-- 4.1  Lifecycle distribution + revenue share
-- ─────────────────────────────────────────────────────────────
SELECT
    lifecycle_stage,
    COUNT(*)                                                              AS customer_count,
    ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER (), 0), 2)                   AS customer_share_pct,
    ROUND(SUM(total_spend), 2)                                            AS lifecycle_revenue,
    ROUND(SUM(total_spend) * 100.0 / NULLIF(SUM(SUM(total_spend)) OVER (), 0), 2)   AS revenue_share_pct,
    ROUND(AVG(total_spend), 2)                                            AS avg_customer_value,
    ROUND(AVG(total_orders), 2)                                           AS avg_orders,
    ROUND(AVG(recency_days), 0)                                           AS avg_recency_days,
    ROUND(SUM(repeat_purchase_flag) * 100.0 / NULLIF(COUNT(*), 0), 2)               AS repeat_rate_pct,
    ROUND(AVG(avg_customer_review_score), 2)                              AS avg_review_score
FROM mart_customer_features
GROUP BY lifecycle_stage
ORDER BY FIELD(lifecycle_stage, 'ACTIVE', 'AT_RISK', 'DORMANT');


-- 4.2  Revenue at risk — AT_RISK customers' 12-month potential
-- Assumes AT_RISK customers would spend their historic avg if re-activated
-- ─────────────────────────────────────────────────────────────
SELECT
    'AT_RISK customers'                                  AS category,
    COUNT(*)                                             AS customer_count,
    ROUND(SUM(total_spend), 2)                           AS historic_spend,
    ROUND(AVG(avg_order_value), 2)                       AS avg_order_value,
    ROUND(AVG(total_orders / NULLIF(GREATEST(customer_lifetime_days / NULLIF(30, 0), 1), 0)), 2)
                                                         AS avg_orders_per_month,
    ROUND(COUNT(*) * AVG(avg_order_value)
          * AVG(total_orders / NULLIF(GREATEST(customer_lifetime_days / NULLIF(30, 0), 1), 0)) * 12, 2)
                                                         AS projected_12mo_revenue_if_retained
FROM mart_customer_features
WHERE lifecycle_stage = 'AT_RISK';


-- 4.3  Dormant revenue recovery potential
-- ─────────────────────────────────────────────────────────────
SELECT
    CASE
        WHEN recency_days BETWEEN 61  AND 120 THEN '61–120 days (recoverable)'
        WHEN recency_days BETWEEN 121 AND 180 THEN '121–180 days (difficult)'
        WHEN recency_days BETWEEN 181 AND 365 THEN '181–365 days (unlikely)'
        ELSE 'Over 365 days (lost)'
    END                                                  AS dormancy_bucket,
    COUNT(*)                                             AS dormant_customers,
    ROUND(SUM(total_spend), 2)                           AS historic_total_spend,
    ROUND(AVG(avg_order_value), 2)                       AS avg_last_order_value,
    ROUND(AVG(total_orders), 2)                          AS avg_lifetime_orders
FROM mart_customer_features
WHERE lifecycle_stage = 'DORMANT'
GROUP BY dormancy_bucket
ORDER BY MIN(recency_days);


-- ============================================================
-- BLOCK 5: COHORT RETENTION MATRIX
-- ============================================================

-- 5.1  Cohort sizes + baseline repeat rate
-- ─────────────────────────────────────────────────────────────
SELECT
    cohort_month,
    cohort_year,
    COUNT(*)                                                              AS cohort_size,
    SUM(repeat_purchase_flag)                                             AS repeat_buyers,
    ROUND(SUM(repeat_purchase_flag) * 100.0 / NULLIF(COUNT(*), 0), 2)               AS repeat_rate_pct,
    ROUND(AVG(total_spend), 2)                                            AS avg_ltv,
    ROUND(AVG(avg_days_between_orders), 1)                                AS avg_days_between_orders,
    ROUND(SUM(total_spend), 2)                                            AS cohort_total_revenue
FROM mart_customer_features
GROUP BY cohort_month, cohort_year
ORDER BY cohort_month;


-- 5.2  Full cohort × period retention matrix
-- Rows: cohort_month | Columns: period 0, 1, 2 ... 11 months
-- ─────────────────────────────────────────────────────────────
WITH order_periods AS (
    SELECT
        f.customer_unique_id,
        m.cohort_month,
        TIMESTAMPDIFF(MONTH,
            STR_TO_DATE(CONCAT(m.cohort_month, '-01'), '%Y-%m-%d'),
            f.order_purchase_timestamp
        ) AS period_num
    FROM fact_order_master f
    JOIN mart_customer_features m ON f.customer_unique_id = m.customer_unique_id
    WHERE f.customer_unique_id IS NOT NULL
      AND f.order_purchase_timestamp IS NOT NULL
),
cohort_activity AS (
    SELECT cohort_month, period_num,
           COUNT(DISTINCT customer_unique_id) AS active_customers
    FROM order_periods
    WHERE period_num BETWEEN 0 AND 11
    GROUP BY cohort_month, period_num
),
cohort_size AS (
    SELECT cohort_month, COUNT(*) AS cohort_size
    FROM mart_customer_features
    GROUP BY cohort_month
)
SELECT
    ca.cohort_month,
    cs.cohort_size,
    ca.period_num,
    ca.active_customers,
    ROUND(ca.active_customers * 100.0 / NULLIF(cs.cohort_size, 0), 2) AS retention_pct,
    -- Period label for readability
    CASE ca.period_num
        WHEN 0 THEN 'Month 0 (acquisition)'
        ELSE CONCAT('Month ', ca.period_num)
    END AS period_label
FROM cohort_activity  ca
JOIN cohort_size      cs ON ca.cohort_month = cs.cohort_month
ORDER BY ca.cohort_month, ca.period_num;


-- 5.3  Average retention rate by period (across all cohorts)
-- ─────────────────────────────────────────────────────────────
WITH order_periods AS (
    SELECT
        f.customer_unique_id,
        m.cohort_month,
        TIMESTAMPDIFF(MONTH,
            STR_TO_DATE(CONCAT(m.cohort_month, '-01'), '%Y-%m-%d'),
            f.order_purchase_timestamp
        ) AS period_num
    FROM fact_order_master f
    JOIN mart_customer_features m ON f.customer_unique_id = m.customer_unique_id
    WHERE f.customer_unique_id IS NOT NULL
),
cohort_size AS (
    SELECT cohort_month, COUNT(*) AS cohort_size
    FROM mart_customer_features GROUP BY cohort_month
),
retention_rates AS (
    SELECT
        op.cohort_month,
        op.period_num,
        ROUND(COUNT(DISTINCT op.customer_unique_id) * 100.0 / NULLIF(cs.cohort_size, 0), 2) AS retention_pct
    FROM order_periods op
    JOIN cohort_size cs ON op.cohort_month = cs.cohort_month
    WHERE op.period_num BETWEEN 0 AND 11
    GROUP BY op.cohort_month, op.period_num
)
SELECT
    period_num,
    ROUND(AVG(retention_pct), 2)  AS avg_retention_pct,
    ROUND(MIN(retention_pct), 2)  AS min_retention_pct,
    ROUND(MAX(retention_pct), 2)  AS max_retention_pct,
    COUNT(DISTINCT cohort_month)  AS cohorts_measured
FROM retention_rates
GROUP BY period_num
ORDER BY period_num;


-- ============================================================
-- BLOCK 6: RETENTION TRENDS OVER TIME
-- ============================================================

-- 6.1  Monthly new vs returning customer count
-- ─────────────────────────────────────────────────────────────
WITH monthly_orders AS (
    SELECT
        f.customer_unique_id,
        DATE_FORMAT(f.order_purchase_timestamp, '%Y-%m') AS order_month,
        m.first_order_date,
        CASE
            WHEN DATE_FORMAT(f.order_purchase_timestamp, '%Y-%m')
               = DATE_FORMAT(m.first_order_date, '%Y-%m')
            THEN 'new'
            ELSE 'returning'
        END AS customer_type
    FROM fact_order_master f
    JOIN mart_customer_features m ON f.customer_unique_id = m.customer_unique_id
    WHERE f.customer_unique_id IS NOT NULL
      AND f.order_purchase_timestamp IS NOT NULL
)
SELECT
    order_month,
    COUNT(DISTINCT CASE WHEN customer_type = 'new'       THEN customer_unique_id END) AS new_customers,
    COUNT(DISTINCT CASE WHEN customer_type = 'returning' THEN customer_unique_id END) AS returning_customers,
    COUNT(DISTINCT customer_unique_id)                                                 AS total_active_customers,
    ROUND(
        COUNT(DISTINCT CASE WHEN customer_type = 'returning' THEN customer_unique_id END)
        * 100.0
        / NULLIF(COUNT(DISTINCT customer_unique_id), 0), 2
    )                                                                                  AS returning_customer_pct
FROM monthly_orders
GROUP BY order_month
ORDER BY order_month;


-- 6.2  30-day rolling repeat purchase rate trend
-- ─────────────────────────────────────────────────────────────
WITH monthly_cohort_repeat AS (
    SELECT
        cohort_month,
        COUNT(*)                                          AS cohort_size,
        SUM(repeat_purchase_flag)                         AS repeat_buyers,
        ROUND(SUM(repeat_purchase_flag) * 100.0 / NULLIF(COUNT(*), 0), 2) AS repeat_rate_pct
    FROM mart_customer_features
    GROUP BY cohort_month
)
SELECT
    cohort_month,
    cohort_size,
    repeat_buyers,
    repeat_rate_pct,
    ROUND(AVG(repeat_rate_pct) OVER (
        ORDER BY cohort_month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2)                                                 AS rolling_3mo_avg_repeat_rate,
    LAG(repeat_rate_pct, 1) OVER (ORDER BY cohort_month)  AS prev_month_repeat_rate,
    ROUND(repeat_rate_pct
          - LAG(repeat_rate_pct, 1) OVER (ORDER BY cohort_month), 2) AS mom_change_pct_points
FROM monthly_cohort_repeat
ORDER BY cohort_month;


-- ============================================================
-- BLOCK 7: DELIVERY RISK & BEHAVIORAL ANALYSIS
-- ============================================================

-- 7.1  Delayed order ratio vs review score impact
-- ─────────────────────────────────────────────────────────────
SELECT
    CASE
        WHEN delayed_order_ratio = 0                       THEN '0% – No delays'
        WHEN delayed_order_ratio BETWEEN 0.01 AND 0.25    THEN '1–25% delayed'
        WHEN delayed_order_ratio BETWEEN 0.26 AND 0.50    THEN '26–50% delayed'
        WHEN delayed_order_ratio BETWEEN 0.51 AND 0.75    THEN '51–75% delayed'
        ELSE                                                    '76–100% delayed'
    END                                                    AS delay_bucket,
    COUNT(*)                                               AS customers,
    ROUND(AVG(avg_customer_review_score), 2)               AS avg_review_score,
    ROUND(AVG(total_spend), 2)                             AS avg_lifetime_spend,
    ROUND(AVG(total_orders), 2)                            AS avg_orders,
    ROUND(SUM(repeat_purchase_flag) * 100.0 / NULLIF(COUNT(*), 0), 2) AS repeat_rate_pct,
    ROUND(AVG(avg_delivery_delay), 1)                      AS avg_delay_days
FROM mart_customer_features
WHERE delayed_order_ratio IS NOT NULL
GROUP BY delay_bucket
ORDER BY MIN(delayed_order_ratio);


-- 7.2  High-value order ratio segmentation (risk / quality proxy)
-- ─────────────────────────────────────────────────────────────
SELECT
    CASE
        WHEN high_value_order_ratio = 1.0               THEN 'Always High-Value (100%)'
        WHEN high_value_order_ratio >= 0.75             THEN 'Mostly High-Value (75–99%)'
        WHEN high_value_order_ratio >= 0.50             THEN 'Often High-Value (50–74%)'
        WHEN high_value_order_ratio >= 0.25             THEN 'Sometimes High-Value (25–49%)'
        WHEN high_value_order_ratio > 0                 THEN 'Rarely High-Value (1–24%)'
        ELSE                                                 'Never High-Value (0%)'
    END                                                   AS hv_category,
    COUNT(*)                                              AS customers,
    ROUND(AVG(total_spend), 2)                            AS avg_lifetime_spend,
    ROUND(AVG(avg_order_value), 2)                        AS avg_order_value,
    ROUND(AVG(cancelled_orders * 1.0 / NULLIF(total_orders,0)) * 100, 2)
                                                          AS avg_cancellation_pct,
    ROUND(AVG(avg_customer_review_score), 2)              AS avg_review_score,
    ROUND(AVG(delayed_order_ratio) * 100, 2)             AS avg_delay_pct
FROM mart_customer_features
GROUP BY hv_category
ORDER BY AVG(total_spend) DESC;


-- 7.3  Delivery delay vs customer retention relationship
-- ─────────────────────────────────────────────────────────────
SELECT
    ROUND(avg_delivery_delay / NULLIF(5, 0)) * 5                     AS delay_day_bucket_start,
    COUNT(*)                                              AS customers,
    ROUND(AVG(avg_customer_review_score), 2)              AS avg_review,
    ROUND(SUM(repeat_purchase_flag) * 100.0 / NULLIF(COUNT(*), 0), 2) AS repeat_rate_pct,
    ROUND(AVG(total_spend), 2)                            AS avg_ltv
FROM mart_customer_features
WHERE avg_delivery_delay IS NOT NULL
GROUP BY ROUND(avg_delivery_delay / NULLIF(5, 0)) * 5
ORDER BY delay_day_bucket_start;


-- ============================================================
-- BLOCK 8: TEMPORAL ORDER TRENDS
-- ============================================================

-- 8.1  Monthly revenue, orders, new customers trend
-- ─────────────────────────────────────────────────────────────
WITH monthly_fact AS (
    SELECT
        DATE_FORMAT(order_purchase_timestamp, '%Y-%m')  AS order_month,
        YEAR(order_purchase_timestamp)                  AS order_year,
        MONTH(order_purchase_timestamp)                 AS month_num,
        COUNT(DISTINCT order_id)                        AS orders,
        ROUND(SUM(total_payment_value), 2)              AS revenue,
        COUNT(DISTINCT customer_unique_id)              AS active_customers,
        ROUND(AVG(avg_review_score), 2)                 AS avg_review
    FROM fact_order_master
    WHERE order_purchase_timestamp IS NOT NULL
    GROUP BY DATE_FORMAT(order_purchase_timestamp, '%Y-%m'),
             YEAR(order_purchase_timestamp),
             MONTH(order_purchase_timestamp)
)
SELECT
    order_month,
    orders,
    revenue,
    active_customers,
    avg_review,
    LAG(revenue, 1)  OVER (ORDER BY order_month)                    AS prev_month_revenue,
    ROUND(revenue - LAG(revenue,  1) OVER (ORDER BY order_month), 2) AS mom_revenue_change,
    ROUND((revenue - LAG(revenue, 1) OVER (ORDER BY order_month))
          * 100.0
          / NULLIF(LAG(revenue, 1) OVER (ORDER BY order_month), 0), 2) AS mom_growth_pct,
    ROUND(AVG(revenue) OVER (
        ORDER BY order_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2)                                                            AS rolling_3mo_avg_revenue,
    ROUND(SUM(revenue) OVER (
        PARTITION BY order_year ORDER BY order_month
        ROWS UNBOUNDED PRECEDING
    ), 2)                                                            AS ytd_revenue
FROM monthly_fact
ORDER BY order_month;


-- 8.2  Day-of-week revenue distribution
-- ─────────────────────────────────────────────────────────────
SELECT
    order_day_of_week,
    COUNT(DISTINCT order_id)               AS total_orders,
    ROUND(SUM(total_payment_value), 2)     AS total_revenue,
    ROUND(AVG(total_payment_value), 2)     AS avg_order_value,
    ROUND(COUNT(DISTINCT order_id) * 100.0
          / NULLIF(SUM(COUNT(DISTINCT order_id)) OVER(), 0), 2) AS orders_share_pct
FROM fact_order_master
WHERE order_day_of_week IS NOT NULL
GROUP BY order_day_of_week
ORDER BY FIELD(order_day_of_week,
    'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday');


-- ============================================================
-- BLOCK 9: GEOGRAPHIC DISTRIBUTION
-- ============================================================

-- 9.1  Top 15 states by revenue + customer metrics
-- ─────────────────────────────────────────────────────────────
WITH state_metrics AS (
    SELECT
        customer_state,
        COUNT(*)                                                              AS total_customers,
        ROUND(SUM(total_spend), 2)                                            AS total_revenue,
        ROUND(AVG(total_spend), 2)                                            AS avg_customer_ltv,
        ROUND(AVG(avg_order_value), 2)                                        AS avg_order_value,
        ROUND(SUM(repeat_purchase_flag) * 100.0 / NULLIF(COUNT(*), 0), 2)               AS repeat_rate_pct,
        ROUND(SUM(CASE WHEN lifecycle_stage='ACTIVE'  THEN 1 ELSE 0 END)
              * 100.0 / NULLIF(COUNT(*), 0), 2)                                          AS active_pct,
        ROUND(SUM(CASE WHEN lifecycle_stage='DORMANT' THEN 1 ELSE 0 END)
              * 100.0 / NULLIF(COUNT(*), 0), 2)                                          AS dormant_pct,
        ROUND(AVG(avg_customer_review_score), 2)                              AS avg_review
    FROM mart_customer_features
    WHERE customer_state IS NOT NULL
    GROUP BY customer_state
)
SELECT
    customer_state,
    total_customers,
    DENSE_RANK() OVER (ORDER BY total_revenue DESC)                       AS revenue_rank,
    total_revenue,
    ROUND(total_revenue * 100.0 / NULLIF(SUM(total_revenue) OVER(), 0), 2)          AS revenue_share_pct,
    avg_customer_ltv,
    avg_order_value,
    repeat_rate_pct,
    active_pct,
    dormant_pct,
    avg_review
FROM state_metrics
ORDER BY total_revenue DESC
LIMIT 15;


-- ============================================================
-- BLOCK 10: ONE-TIME vs REPEAT BUYER COMPARISON
-- ============================================================

-- 10.1  Side-by-side comparison
-- ─────────────────────────────────────────────────────────────
SELECT
    CASE WHEN repeat_purchase_flag = 1 THEN 'Repeat Buyer' ELSE 'One-Time Buyer' END AS buyer_type,
    COUNT(*)                                                 AS customers,
    ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER(), 0), 2)       AS customer_share_pct,
    ROUND(SUM(total_spend), 2)                               AS total_revenue,
    ROUND(SUM(total_spend) * 100.0 / NULLIF(SUM(SUM(total_spend)) OVER(), 0), 2) AS revenue_share_pct,
    ROUND(AVG(total_spend), 2)                               AS avg_ltv,
    ROUND(AVG(avg_order_value), 2)                           AS avg_order_value,
    ROUND(AVG(total_orders), 2)                              AS avg_orders,
    ROUND(AVG(customer_lifetime_days), 0)                    AS avg_lifetime_days,
    ROUND(AVG(avg_customer_review_score), 2)                 AS avg_review_score,
    ROUND(AVG(delayed_order_ratio) * 100, 2)                AS avg_delay_pct,
    ROUND(AVG(total_categories_purchased), 2)                AS avg_categories_explored
FROM mart_customer_features
GROUP BY repeat_purchase_flag
ORDER BY repeat_purchase_flag DESC;


-- 10.2  Time-to-second-purchase distribution for repeat buyers
-- ─────────────────────────────────────────────────────────────
SELECT
    CASE
        WHEN avg_days_between_orders BETWEEN 0   AND 7   THEN '0–7 days'
        WHEN avg_days_between_orders BETWEEN 8   AND 30  THEN '8–30 days'
        WHEN avg_days_between_orders BETWEEN 31  AND 90  THEN '31–90 days'
        WHEN avg_days_between_orders BETWEEN 91  AND 180 THEN '91–180 days'
        ELSE                                                   '180+ days'
    END                                                        AS repurchase_interval,
    COUNT(*)                                                   AS repeat_customers,
    ROUND(AVG(total_spend), 2)                                 AS avg_ltv,
    ROUND(AVG(total_orders), 2)                                AS avg_total_orders,
    ROUND(AVG(avg_customer_review_score), 2)                   AS avg_review
FROM mart_customer_features
WHERE repeat_purchase_flag = 1
  AND avg_days_between_orders IS NOT NULL
GROUP BY repurchase_interval
ORDER BY MIN(avg_days_between_orders);


-- ============================================================
-- BLOCK 11: SEGMENT × LIFECYCLE CROSS-MATRIX
-- ============================================================

-- 11.1  Revenue at each intersection (heat-map data)
-- ─────────────────────────────────────────────────────────────
SELECT
    segment_label,
    lifecycle_stage,
    COUNT(*)                                                              AS customer_count,
    ROUND(SUM(total_spend), 2)                                            AS revenue,
    ROUND(AVG(avg_order_value), 2)                                        AS avg_order_value,
    ROUND(AVG(rfm_score), 2)                                              AS avg_rfm_score,
    ROUND(SUM(total_spend) * 100.0 / NULLIF(SUM(SUM(total_spend)) OVER(), 0), 2)    AS revenue_pct_of_total
FROM mart_customer_features
GROUP BY segment_label, lifecycle_stage
ORDER BY segment_label, FIELD(lifecycle_stage, 'ACTIVE', 'AT_RISK', 'DORMANT');


-- 11.2  Top preferred categories by segment
-- ─────────────────────────────────────────────────────────────
SELECT segment_label, preferred_category, customer_count,
       ROUND(revenue, 2) AS segment_category_revenue
FROM (
    SELECT
        segment_label,
        preferred_category,
        COUNT(*)          AS customer_count,
        SUM(total_spend)  AS revenue,
        ROW_NUMBER() OVER (PARTITION BY segment_label ORDER BY SUM(total_spend) DESC) AS rn
    FROM mart_customer_features
    WHERE preferred_category IS NOT NULL
    GROUP BY segment_label, preferred_category
) t
WHERE rn <= 3
ORDER BY segment_label, revenue DESC;


-- ============================================================
-- END OF FILE 1
-- Next step: Run stage4_02_powerbi_views.sql
-- ============================================================
