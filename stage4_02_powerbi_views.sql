-- ============================================================
-- OLIST BRAZILIAN E-COMMERCE ANALYTICS
-- STAGE 4 | FILE 2: POWER BI VIEWS + KPI LAYER
-- Environment : MySQL 8+
-- Run After   : stage4_01_analytics_queries.sql
-- ============================================================
-- PURPOSE:
--   Create persistent MySQL VIEWs that Power BI (or any BI tool)
--   can connect to directly as data sources.
--   Each view corresponds to one chart or page in the dashboard.
-- ============================================================

USE olist_ecommerce;

-- ============================================================
-- VIEW 1: vw_kpi_executive
-- Power BI: KPI Cards Page — top-level business metrics
-- ============================================================

DROP VIEW IF EXISTS vw_kpi_executive;
CREATE VIEW vw_kpi_executive AS
SELECT
    COUNT(DISTINCT customer_unique_id)                                          AS total_unique_customers,
    ROUND(SUM(total_spend), 2)                                                  AS total_revenue_BRL,
    ROUND(AVG(avg_order_value), 2)                                              AS avg_order_value_BRL,
    ROUND(SUM(total_orders), 0)                                                 AS total_orders_placed,
    ROUND(SUM(total_spend) / NULLIF(SUM(total_orders), 0), 2)                  AS revenue_per_order,
    ROUND(SUM(repeat_purchase_flag)   * 100.0 / NULLIF(COUNT(*), 0), 2)                    AS repeat_customer_pct,
    ROUND(SUM(is_one_time_buyer)      * 100.0 / NULLIF(COUNT(*), 0), 2)                    AS one_time_buyer_pct,
    ROUND(SUM(CASE WHEN lifecycle_stage = 'ACTIVE'  THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS active_pct,
    ROUND(SUM(CASE WHEN lifecycle_stage = 'AT_RISK' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS at_risk_pct,
    ROUND(SUM(CASE WHEN lifecycle_stage = 'DORMANT' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS dormant_pct,
    ROUND(AVG(avg_customer_review_score), 2)                                    AS avg_review_score,
    ROUND(AVG(avg_delivery_delay), 2)                                           AS avg_delivery_delay_days,
    ROUND(AVG(delayed_order_ratio) * 100, 2)                                    AS avg_delay_rate_pct,
    -- Revenue at risk: AT_RISK + DORMANT customer historic spend
    ROUND(SUM(CASE WHEN lifecycle_stage IN ('AT_RISK','DORMANT') THEN total_spend ELSE 0 END), 2)
                                                                                AS revenue_at_risk_BRL,
    MIN(cohort_month)                                                           AS first_cohort_month,
    MAX(cohort_month)                                                           AS last_cohort_month
FROM mart_customer_features;


-- ============================================================
-- VIEW 2: vw_pareto_revenue
-- Power BI: Pareto Chart — top 20% customer contribution
-- ============================================================

DROP VIEW IF EXISTS vw_pareto_revenue;
CREATE VIEW vw_pareto_revenue AS
SELECT
    customer_unique_id,
    total_spend,
    segment_label,
    lifecycle_stage,
    spend_rank,
    total_customers,
    ROUND(spend_rank * 100.0 / total_customers, 2)       AS customer_percentile,
    ROUND(cumulative_spend * 100.0 / grand_total, 2)     AS cumulative_revenue_pct,
    spend_decile,
    CASE
        WHEN spend_rank * 100.0 / total_customers <= 20 THEN 'Top 20%'
        WHEN spend_rank * 100.0 / total_customers <= 50 THEN 'Middle 30%'
        ELSE                                                  'Bottom 50%'
    END AS customer_tier
FROM (
    SELECT
        customer_unique_id,
        total_spend,
        segment_label,
        lifecycle_stage,
        ROW_NUMBER()  OVER (ORDER BY total_spend DESC)     AS spend_rank,
        COUNT(*)      OVER ()                              AS total_customers,
        SUM(total_spend) OVER (ORDER BY total_spend DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_spend,
        SUM(total_spend) OVER ()                           AS grand_total,
        NTILE(10)     OVER (ORDER BY total_spend DESC)     AS spend_decile
    FROM mart_customer_features
    WHERE total_spend > 0
) t;


-- ============================================================
-- VIEW 3: vw_segment_summary
-- Power BI: Segment Breakdown (bar charts, revenue share)
-- ============================================================

DROP VIEW IF EXISTS vw_segment_summary;
CREATE VIEW vw_segment_summary AS
SELECT
    segment_label,
    COUNT(*)                                                              AS customer_count,
    ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER (), 0), 2)                   AS customer_share_pct,
    ROUND(SUM(total_spend), 2)                                            AS total_revenue,
    ROUND(SUM(total_spend) * 100.0 / SUM(SUM(total_spend)) OVER (), 2)   AS revenue_share_pct,
    ROUND(AVG(avg_order_value), 2)                                        AS avg_order_value,
    ROUND(AVG(total_orders), 2)                                           AS avg_orders,
    ROUND(AVG(recency_days), 0)                                           AS avg_recency_days,
    ROUND(AVG(rfm_score), 2)                                              AS avg_rfm_score,
    ROUND(SUM(repeat_purchase_flag) * 100.0 / NULLIF(COUNT(*), 0), 2)               AS repeat_rate_pct,
    ROUND(AVG(avg_customer_review_score), 2)                              AS avg_review_score,
    ROUND(AVG(delayed_order_ratio) * 100, 2)                             AS avg_delay_pct,
    ROUND(AVG(customer_lifetime_days), 0)                                 AS avg_lifetime_days,
    -- Segment priority for Power BI sort order
    CASE segment_label
        WHEN 'champions'          THEN 1
        WHEN 'loyal'              THEN 2
        WHEN 'high_value'         THEN 3
        WHEN 'cannot_lose'        THEN 4
        WHEN 'potential_loyalist' THEN 5
        WHEN 'new_customer'       THEN 6
        WHEN 'churn_risk'         THEN 7
        WHEN 'at_risk'            THEN 8
        WHEN 'hibernating'        THEN 9
        WHEN 'lost'               THEN 10
        ELSE                           11
    END                                                                   AS segment_sort_order
FROM mart_customer_features
GROUP BY segment_label;


-- ============================================================
-- VIEW 4: vw_lifecycle_summary
-- Power BI: Lifecycle Donut + Revenue Waterfall
-- ============================================================

DROP VIEW IF EXISTS vw_lifecycle_summary;
CREATE VIEW vw_lifecycle_summary AS
SELECT
    lifecycle_stage,
    COUNT(*)                                                               AS customer_count,
    ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER (), 0), 2)                    AS customer_share_pct,
    ROUND(SUM(total_spend), 2)                                             AS lifecycle_revenue,
    ROUND(SUM(total_spend) * 100.0 / SUM(SUM(total_spend)) OVER (), 2)    AS revenue_share_pct,
    ROUND(AVG(total_spend), 2)                                             AS avg_customer_ltv,
    ROUND(AVG(avg_order_value), 2)                                         AS avg_order_value,
    ROUND(AVG(recency_days), 0)                                            AS avg_recency_days,
    ROUND(SUM(repeat_purchase_flag) * 100.0 / NULLIF(COUNT(*), 0), 2)                AS repeat_rate_pct,
    ROUND(AVG(avg_customer_review_score), 2)                               AS avg_review_score,
    ROUND(AVG(delayed_order_ratio) * 100, 2)                              AS avg_delay_pct,
    CASE lifecycle_stage
        WHEN 'ACTIVE'   THEN 1
        WHEN 'AT_RISK'  THEN 2
        WHEN 'DORMANT'  THEN 3
    END                                                                    AS stage_sort_order
FROM mart_customer_features
GROUP BY lifecycle_stage;


-- ============================================================
-- VIEW 5: vw_cohort_retention
-- Power BI: Cohort Retention Matrix (heat map) + Repeat Rate Line
-- ============================================================

DROP VIEW IF EXISTS vw_cohort_retention;
CREATE VIEW vw_cohort_retention AS
SELECT
    ca.cohort_month,
    cs.cohort_size                               AS initial_customers,
    ca.period_num                                AS months_after_first_order,
    ca.active_customers                          AS returning_customers,
    ROUND(ca.active_customers * 100.0 / cs.cohort_size, 2) AS retention_pct,
    CASE
        WHEN ROUND(ca.active_customers * 100.0 / cs.cohort_size, 2) >= 20 THEN 'High'
        WHEN ROUND(ca.active_customers * 100.0 / cs.cohort_size, 2) >= 10 THEN 'Medium'
        ELSE 'Low'
    END                                           AS retention_band
FROM (
    SELECT
        m.cohort_month,
        TIMESTAMPDIFF(MONTH,
            STR_TO_DATE(CONCAT(m.cohort_month, '-01'), '%Y-%m-%d'),
            f.order_purchase_timestamp
        ) AS period_num,
        COUNT(DISTINCT f.customer_unique_id) AS active_customers
    FROM fact_order_master f
    JOIN mart_customer_features m ON f.customer_unique_id = m.customer_unique_id
    WHERE f.customer_unique_id IS NOT NULL
      AND f.order_purchase_timestamp IS NOT NULL
    GROUP BY m.cohort_month,
             TIMESTAMPDIFF(MONTH,
                 STR_TO_DATE(CONCAT(m.cohort_month, '-01'), '%Y-%m-%d'),
                 f.order_purchase_timestamp
             )
) ca
JOIN (
    SELECT cohort_month, COUNT(*) AS cohort_size
    FROM mart_customer_features
    GROUP BY cohort_month
) cs ON ca.cohort_month = cs.cohort_month
WHERE ca.period_num BETWEEN 0 AND 11;


-- ============================================================
-- VIEW 6: vw_monthly_revenue_trend
-- Power BI: Revenue Line Chart + MoM Growth
-- ============================================================

DROP VIEW IF EXISTS vw_monthly_revenue_trend;
CREATE VIEW vw_monthly_revenue_trend AS
WITH monthly AS (
    SELECT
        DATE_FORMAT(order_purchase_timestamp, '%Y-%m')   AS order_month,
        YEAR(order_purchase_timestamp)                   AS order_year,
        MONTH(order_purchase_timestamp)                  AS order_month_num,
        COUNT(DISTINCT order_id)                         AS order_count,
        COUNT(DISTINCT customer_unique_id)               AS unique_customers,
        ROUND(SUM(total_payment_value), 2)               AS revenue,
        ROUND(SUM(total_order_value), 2)                 AS items_revenue,
        ROUND(SUM(total_freight_value), 2)               AS freight_revenue,
        ROUND(AVG(total_payment_value), 2)               AS avg_order_value,
        ROUND(AVG(avg_review_score), 2)                  AS avg_review_score,
        SUM(is_delayed_delivery)                         AS delayed_orders,
        ROUND(SUM(is_delayed_delivery) * 100.0
              / COUNT(DISTINCT order_id), 2)             AS delay_rate_pct
    FROM fact_order_master
    WHERE order_purchase_timestamp IS NOT NULL
    GROUP BY DATE_FORMAT(order_purchase_timestamp, '%Y-%m'),
             YEAR(order_purchase_timestamp),
             MONTH(order_purchase_timestamp)
)
SELECT
    order_month,
    order_year,
    order_month_num,
    order_count,
    unique_customers,
    revenue,
    items_revenue,
    freight_revenue,
    avg_order_value,
    avg_review_score,
    delayed_orders,
    delay_rate_pct,
    LAG(revenue,  1) OVER (ORDER BY order_month)  AS prev_month_revenue,
    ROUND((revenue - LAG(revenue, 1) OVER (ORDER BY order_month))
          * 100.0 / NULLIF(LAG(revenue, 1) OVER (ORDER BY order_month), 0), 2)
                                                  AS mom_growth_pct,
    ROUND(AVG(revenue) OVER (
        ORDER BY order_month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 2)                                         AS rolling_3mo_avg,
    ROUND(SUM(revenue) OVER (
        PARTITION BY order_year ORDER BY order_month ROWS UNBOUNDED PRECEDING
    ), 2)                                         AS ytd_revenue
FROM monthly
ORDER BY order_month;


-- ============================================================
-- VIEW 7: vw_risk_behavior
-- Power BI: Behavioral Scatter + Risk Proxy Bar Charts
-- ============================================================

DROP VIEW IF EXISTS vw_risk_behavior;
CREATE VIEW vw_risk_behavior AS
SELECT
    customer_unique_id,
    lifecycle_stage,
    segment_label,
    delayed_order_ratio,
    multi_item_ratio,
    multi_payment_ratio,
    high_value_order_ratio,
    avg_delivery_delay,
    avg_customer_review_score,
    total_spend,
    total_orders,
    cancelled_orders,
    ROUND(cancelled_orders * 100.0 / NULLIF(total_orders, 0), 2) AS cancellation_rate_pct,
    CASE
        WHEN delayed_order_ratio >= 0.5
         AND avg_customer_review_score < 3
         AND cancelled_orders >= 1
        THEN 'High Risk'
        WHEN delayed_order_ratio >= 0.25
          OR avg_customer_review_score < 3
        THEN 'Medium Risk'
        ELSE 'Low Risk'
    END AS customer_risk_tier
FROM mart_customer_features;


-- ============================================================
-- VIEW 8: vw_geographic_summary
-- Power BI: Brazil Map + State Revenue Bar
-- ============================================================

DROP VIEW IF EXISTS vw_geographic_summary;
CREATE VIEW vw_geographic_summary AS
SELECT
    customer_state,
    COUNT(*)                                                               AS total_customers,
    ROUND(SUM(total_spend), 2)                                             AS total_revenue,
    ROUND(SUM(total_spend) * 100.0 / SUM(SUM(total_spend)) OVER(), 2)     AS revenue_share_pct,
    ROUND(AVG(total_spend), 2)                                             AS avg_customer_ltv,
    ROUND(AVG(avg_order_value), 2)                                         AS avg_order_value,
    ROUND(SUM(repeat_purchase_flag) * 100.0 / NULLIF(COUNT(*), 0), 2)                AS repeat_rate_pct,
    ROUND(SUM(CASE WHEN lifecycle_stage='ACTIVE'  THEN 1 ELSE 0 END)
          * 100.0 / NULLIF(COUNT(*), 0), 2)                                           AS active_pct,
    ROUND(SUM(CASE WHEN lifecycle_stage='DORMANT' THEN 1 ELSE 0 END)
          * 100.0 / NULLIF(COUNT(*), 0), 2)                                           AS dormant_pct,
    ROUND(AVG(avg_customer_review_score), 2)                               AS avg_review,
    DENSE_RANK() OVER (ORDER BY SUM(total_spend) DESC)                     AS revenue_rank
FROM mart_customer_features
WHERE customer_state IS NOT NULL
GROUP BY customer_state;


-- ============================================================
-- VIEW 9: vw_customer_profile_distribution
-- Power BI: RFM Histogram + Score Distribution
-- ============================================================

DROP VIEW IF EXISTS vw_customer_profile_distribution;
CREATE VIEW vw_customer_profile_distribution AS
SELECT
    rfm_score,
    rfm_label,
    r_score,
    f_score,
    m_score,
    COUNT(*)                                                               AS customer_count,
    ROUND(COUNT(*) * 100.0 / NULLIF(SUM(COUNT(*)) OVER(), 0), 2)                     AS pct_of_customers,
    ROUND(SUM(total_spend), 2)                                             AS total_revenue,
    ROUND(AVG(total_spend), 2)                                             AS avg_ltv,
    ROUND(AVG(avg_order_value), 2)                                         AS avg_order_value,
    ROUND(AVG(recency_days), 0)                                            AS avg_recency_days
FROM mart_customer_features
GROUP BY rfm_score, rfm_label, r_score, f_score, m_score
ORDER BY rfm_score DESC;


-- ============================================================
-- VERIFY ALL VIEWS CREATED
-- ============================================================
SELECT
    table_name AS view_name,
    'VIEW' AS object_type
FROM information_schema.views
WHERE table_schema = 'olist_ecommerce'
ORDER BY table_name;

-- ============================================================
-- END OF FILE 2
-- ============================================================
