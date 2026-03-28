-- ============================================================
-- OLAP VIEWS
--
-- Purpose:
--   This layer provides analytical views for reporting and business insights.
--
-- Description:
--   OLAP views aggregate and analyze data from fact views and dimension views.
--   They implement advanced analytical techniques such as:
--
--   - GROUP BY ROLLUP / CUBE (multi-dimensional aggregation)
--   - window functions (cumulative metrics, period comparison)
--   - derived KPIs (percentages, ratios, retention metrics)
--
--   These views are designed to answer business questions such as:
--   - retention and churn analysis
--   - engagement vs revenue analysis
--   - time-based performance trends
--
--   OLAP views represent the final analytical layer of the model.
--
-- Notes:
--   - Built on top of fact-like consolidation views (ROLAP approach).
--   - Use dimension views for descriptive context (e.g. user, tier).
--   - Results may reflect limited data volume due to upstream access layer constraints.
-- ============================================================
SET SQLBLANKLINES ON
SET DEFINE OFF

-- ADINA
--
-- Required consolidation views:
--   V_CONS_USERS
--   V_CONS_SUBSCRIPTIONS
--   V_CONS_SUBSCRIPTION_TIERS
--   V_CONS_USER_SUBSCRIPTION
--   V_CONS_SUB_COHORT
--   V_CONS_USER_ACTIVITY
--   V_CONS_PG_ORDERS
--   V_CONS_USER_ORDERS
--
-- Required dimension views:
--   V_DIM_USERS
--   V_DIM_SUBSCRIPTION_TIERS

-- 01. V_OLAP_COHORT_RETENTION
--
-- Query 1:
--   Cohort retention / churn by subscription tier
--
-- Description:
--   Aggregates subscription lifecycle and retention data by cohort month
--   and subscription tier in order to measure user retention and churn.
--   The view exposes how many users started in each cohort, how many
--   were retained after 30 and 90 days, how many churned, and the
--   corresponding percentage metrics.
--
--   This OLAP view uses:
--   - V_CONS_SUB_COHORT as the fact-like source
--   - V_DIM_SUBSCRIPTION_TIERS as the descriptive dimension source
--
-- Technique:
--   GROUP BY ROLLUP
--   - detailed rows by cohort month and subscription tier
--   - subtotals by cohort month
--   - grand total
CREATE OR REPLACE VIEW FDBO.V_CONS_SUB_COHORT AS
SELECT
    us.user_id,
    us.email                AS user_email,
    us.full_name            AS user_full_name,
    us.country_code         AS user_country_code,
    us.city                 AS user_city,
    us.sub_id               AS subscription_id,
    us.tier_id,
    us.tier_name,
    us.sub_status           AS subscription_status,
    us.billing_cycle,
    us.started_at,
    us.current_period_start,
    us.current_period_end,
    us.cancelled_at,
    us.cancel_reason,
    us.monthly_price_usd,
    TRUNC(CAST(us.started_at AS DATE), 'MM')        AS cohort_month,
    CASE
        WHEN us.cancelled_at IS NOT NULL
            THEN CAST(us.cancelled_at AS DATE) - CAST(us.started_at AS DATE)
        WHEN us.current_period_end IS NOT NULL
            THEN CAST(us.current_period_end AS DATE) - CAST(us.started_at AS DATE)
        ELSE SYSDATE - CAST(us.started_at AS DATE)
    END AS days_active,
    CASE WHEN NVL(CAST(us.cancelled_at AS DATE), CAST(us.current_period_end AS DATE))
              >= CAST(us.started_at AS DATE) + 30 THEN 1 ELSE 0 END AS retained_30d,
    CASE WHEN NVL(CAST(us.cancelled_at AS DATE), CAST(us.current_period_end AS DATE))
              >= CAST(us.started_at AS DATE) + 90 THEN 1 ELSE 0 END AS retained_90d,
    CASE WHEN us.cancelled_at IS NOT NULL THEN 1 ELSE 0 END AS churned_flag
FROM FDBO.V_CONS_USER_SUBSCRIPTION us;
-- 02. V_OLAP_ENGAGEMENT_REVENUE_MONTHLY
--
-- Query 2:
--   User engagement and order revenue analysis by month and country
--
-- Description:
--   Aggregates monthly behavioural activity and commercial order data
--   in order to analyse engagement intensity, purchase-related activity,
--   order volume, quantity sold, and revenue by month and user country.
--
--   The view combines:
--   - behavioural activity from V_CONS_USER_ACTIVITY
--   - order transactions from V_CONS_USER_ORDERS
--   - descriptive user geography from V_DIM_USERS
--
-- Technique:
--   GROUP BY ROLLUP
--   - detailed rows by month and country
--   - subtotals by month
--   - grand total
--
--   WINDOW FUNCTIONS
--   - cumulative revenue over time
--   - cumulative orders over time
--   - previous-month revenue comparison
CREATE OR REPLACE VIEW FDBO.V_OLAP_ENGAGEMENT_REVENUE_MONTHLY AS
WITH activity_monthly AS (
    SELECT
        TRUNC(CAST(ua.occurred_at AS DATE), 'MM')   AS activity_month,
        ua.user_country_code,                        
        COUNT(*)                                     AS total_events,
        SUM(CASE WHEN ua.event_type = 'page_view'       THEN 1 ELSE 0 END) AS total_page_views,
        SUM(CASE WHEN ua.event_type = 'product_view'    THEN 1 ELSE 0 END) AS total_product_views,
        SUM(CASE WHEN ua.event_type = 'search'          THEN 1 ELSE 0 END) AS total_searches,
        SUM(CASE WHEN ua.event_type = 'add_to_cart'     THEN 1 ELSE 0 END) AS total_add_to_cart,
        SUM(CASE WHEN ua.event_type = 'checkout_start'  THEN 1 ELSE 0 END) AS total_checkout_starts,
        SUM(CASE WHEN ua.event_type = 'purchase'        THEN 1 ELSE 0 END) AS total_purchase_events
    FROM FDBO.V_CONS_USER_ACTIVITY ua
    GROUP BY
        TRUNC(CAST(ua.occurred_at AS DATE), 'MM'),
        ua.user_country_code
),
orders_monthly AS (
    SELECT
        TRUNC(TO_DATE(SUBSTR(uo.order_created_at, 1, 10), 'YYYY-MM-DD'), 'MM') AS order_month, 
        uo.country_code                              AS user_country_code, 
        COUNT(DISTINCT uo.order_id)                  AS total_orders,
        SUM(NVL(uo.quantity, 0))                     AS total_quantity,
        SUM(NVL(uo.line_total_usd, 0))               AS total_revenue_usd
    FROM FDBO.V_CONS_USER_ORDERS uo
    GROUP BY
        TRUNC(TO_DATE(SUBSTR(uo.order_created_at, 1, 10), 'YYYY-MM-DD'), 'MM'),
        uo.country_code
),
combined AS (
    SELECT
        a.activity_month,
        a.user_country_code,
        a.total_events,
        a.total_page_views,
        a.total_product_views,
        a.total_searches,
        a.total_add_to_cart,
        a.total_checkout_starts,
        a.total_purchase_events,
        NVL(o.total_orders, 0)      AS total_orders,
        NVL(o.total_quantity, 0)    AS total_quantity,
        NVL(o.total_revenue_usd, 0) AS total_revenue_usd
    FROM activity_monthly a
    LEFT JOIN orders_monthly o
        ON  o.order_month = a.activity_month
        AND NVL(o.user_country_code, '##NULL##') = NVL(a.user_country_code, '##NULL##')
),
base_rollup AS (
    SELECT
        activity_month,
        user_country_code,
        SUM(total_events)           AS total_events,
        SUM(total_page_views)       AS total_page_views,
        SUM(total_product_views)    AS total_product_views,
        SUM(total_searches)         AS total_searches,
        SUM(total_add_to_cart)      AS total_add_to_cart,
        SUM(total_checkout_starts)  AS total_checkout_starts,
        SUM(total_purchase_events)  AS total_purchase_events,
        SUM(total_orders)           AS total_orders,
        SUM(total_quantity)         AS total_quantity,
        SUM(total_revenue_usd)      AS total_revenue_usd,
        GROUPING(activity_month)    AS grp_month,
        GROUPING(user_country_code) AS grp_country
    FROM combined
    GROUP BY ROLLUP(activity_month, user_country_code)
),
monthly_totals AS (
    SELECT
        activity_month,
        total_revenue_usd,
        total_orders,
        SUM(total_revenue_usd) OVER (
            ORDER BY activity_month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_revenue_usd,
        SUM(total_orders) OVER (
            ORDER BY activity_month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_orders,
        LAG(total_revenue_usd) OVER (
            ORDER BY activity_month
        ) AS prev_month_revenue_usd
    FROM base_rollup
    WHERE activity_month IS NOT NULL
      AND user_country_code IS NULL
)
SELECT
    b.activity_month,
    b.user_country_code,
    b.total_events,
    b.total_page_views,
    b.total_product_views,
    b.total_searches,
    b.total_add_to_cart,
    b.total_checkout_starts,
    b.total_purchase_events,
    b.total_orders,
    b.total_quantity,
    b.total_revenue_usd,
    m.cumulative_revenue_usd,
    m.cumulative_orders,
    m.prev_month_revenue_usd,
    b.grp_month,
    b.grp_country
FROM base_rollup b
LEFT JOIN monthly_totals m
    ON m.activity_month = b.activity_month;
--







-- ============================================================
-- ============================================================
-- ============================================================
-- 5. V_OLAP_FUNNEL_CONVERSION_BY_TIER
--
-- Purpose:
--   Analyzes the customer journey funnel (Awareness -> Consideration -> Conversion)
--   segmented by subscription tier and product category.
--   Provides granular conversion rates and purchase rates using multidimensional 
--   grouping sets for flexible reporting.
--
-- OLAP feature: GROUPING SETS (Tier, Stage, Product, Month)
-- Source views: V_CONS_USER_ACTIVITY, V_CONS_USER_SUBSCRIPTION
--
-- Output columns:
--   tier_name          - User's current subscription level
--   funnel_stage       - Categorized intent (Awareness/Consideration/Conversion)
--   product_type       - The type of product interacted with
--   event_month        - The month the activity occurred
--   total_events       - Raw count of all activities in that group
--   unique_users       - Distinct user count (Reach)
--   purchase_rate_pct   - Percentage of total events that were purchases
--   conversion_rate_pct - Percentage of events that were "lower-funnel" actions
-- ============================================================
-- ============================================================
-- ============================================================
CREATE OR REPLACE VIEW FDBO.V_OLAP_FUNNEL_CONVERSION_BY_TIER AS
WITH funnel_base AS (
    SELECT
        ua.user_id,
        TRUNC(CAST(ua.occurred_at AS DATE), 'MM') AS event_month,
        us.tier_name,
        ua.product_type,
        ua.event_type,
        CASE
            WHEN ua.event_type IN ('page_view','search')
                THEN 'awareness'
            WHEN ua.event_type IN ('product_view','add_to_cart')
                THEN 'consideration'
            WHEN ua.event_type IN ('checkout_start','purchase')
                THEN 'conversion'
            ELSE 'other'
        END                             AS funnel_stage,
        CASE
            WHEN ua.event_type IN (
                'add_to_cart','checkout_start','purchase'
            ) THEN 1 ELSE 0
        END                             AS is_conversion_event,
        CASE
            WHEN ua.event_type = 'purchase' THEN 1
            ELSE 0
        END                             AS is_purchase
    FROM FDBO.V_CONS_USER_ACTIVITY ua
    LEFT JOIN (
        SELECT user_id, tier_name
        FROM (
            SELECT user_id, tier_name,
                   ROW_NUMBER() OVER (
                       PARTITION BY user_id
                       ORDER BY started_at DESC
                   ) AS rn
            FROM FDBO.V_CONS_USER_SUBSCRIPTION
        ) WHERE rn = 1
    ) us ON us.user_id = ua.user_id
)
SELECT
    tier_name,
    funnel_stage,
    product_type,
    event_month,
    COUNT(*)                            AS total_events,
    SUM(is_conversion_event)            AS conversion_events,
    SUM(is_purchase)                    AS purchase_events,
    COUNT(DISTINCT user_id)             AS unique_users,
    ROUND(
        100 * SUM(is_purchase)
        / NULLIF(COUNT(*), 0), 2
    )                                   AS purchase_rate_pct,
    ROUND(
        100 * SUM(is_conversion_event)
        / NULLIF(COUNT(*), 0), 2
    )                                   AS conversion_rate_pct,
    GROUPING(tier_name)                 AS grp_tier,
    GROUPING(funnel_stage)              AS grp_funnel,
    GROUPING(product_type)              AS grp_product_type,
    GROUPING(event_month)               AS grp_month
FROM funnel_base
GROUP BY GROUPING SETS (
    (tier_name, funnel_stage),
    (tier_name, funnel_stage, product_type),
    (tier_name, funnel_stage, event_month),
    ()
);
-- ============================================================
-- ============================================================
-- ============================================================
-- 6. V_OLAP_SUBSCRIPTION_BILLING_TREND
--
-- Purpose:
--   Advanced financial time-series analysis for subscription revenue.
--   Calculates growth metrics, cumulative totals, and year-over-year (YoY)
--   comparisons using window functions.
--
-- OLAP feature: Window Functions (LAG, LEAD, SUM OVER, FIRST_VALUE, LAST_VALUE)
-- Source views: SUBSCRIPTION_INVOICES, SUBSCRIPTIONS, SUBSCRIPTION_TIERS
--
-- Output columns:
--   billing_month            - The first day of the billing month
--   tier_name                - The subscription plan name
--   total_billed_usd         - Current month revenue
--   cumulative_revenue_usd   - Running total of revenue per tier
--   prev_month_revenue_usd   - Prior month's revenue (for MoM calculation)
--   mom_growth_pct           - Month-over-Month growth percentage
--   same_month_prior_year_usd- Revenue from 12 months ago (for YoY calculation)
--   last_month_revenue_usd   - The revenue from the most recent month in the dataset
-- ============================================================
-- ============================================================
-- ============================================================
CREATE OR REPLACE VIEW FDBO.V_OLAP_SUBSCRIPTION_BILLING_TREND AS
WITH monthly_billing AS (
    SELECT
        TRUNC(si.billing_period_start, 'MM')    AS billing_month,
        st.name                                 AS tier_name,
        s.billing_cycle,
        COUNT(DISTINCT si.id)                   AS invoice_count,
        SUM(si.total_usd)                       AS total_billed_usd,
        SUM(si.discount_usd)                    AS total_discount_usd,
        SUM(si.tax_usd)                         AS total_tax_usd,
        COUNT(DISTINCT si.user_id)              AS unique_users_billed
    FROM FDBO.SUBSCRIPTION_INVOICES si
    JOIN FDBO.SUBSCRIPTIONS s
        ON s.id = si.subscription_id
    JOIN FDBO.SUBSCRIPTION_TIERS st
        ON st.id = s.tier_id
    WHERE si.status = 'paid'
    GROUP BY
        TRUNC(si.billing_period_start, 'MM'),
        st.name,
        s.billing_cycle
)
SELECT
    billing_month,
    tier_name,
    billing_cycle,
    invoice_count,
    total_billed_usd,
    total_discount_usd,
    total_tax_usd,
    unique_users_billed,
    SUM(total_billed_usd) OVER (
        PARTITION BY tier_name
        ORDER BY billing_month
        ROWS UNBOUNDED PRECEDING
    )                                           AS cumulative_revenue_usd,
    LAG(total_billed_usd, 1) OVER (
        PARTITION BY tier_name
        ORDER BY billing_month
    )                                           AS prev_month_revenue_usd,
    LAG(total_billed_usd, 12) OVER (
        PARTITION BY tier_name
        ORDER BY billing_month
    )                                           AS same_month_prior_year_usd,
    LEAD(total_billed_usd, 1) OVER (
        PARTITION BY tier_name
        ORDER BY billing_month
    )                                           AS next_month_revenue_usd,
    FIRST_VALUE(total_billed_usd) OVER (
        PARTITION BY tier_name
        ORDER BY billing_month
        ROWS UNBOUNDED PRECEDING
    )                                           AS first_month_revenue_usd,
    LAST_VALUE(total_billed_usd) OVER (
        PARTITION BY tier_name
        ORDER BY billing_month
        ROWS BETWEEN UNBOUNDED PRECEDING
            AND UNBOUNDED FOLLOWING
    )                                           AS last_month_revenue_usd,
    ROUND(
        100 * (
            total_billed_usd -
            LAG(total_billed_usd, 1) OVER (
                PARTITION BY tier_name
                ORDER BY billing_month
            )
        ) / NULLIF(
            LAG(total_billed_usd, 1) OVER (
                PARTITION BY tier_name
                ORDER BY billing_month
            ), 0
        ), 2
    )                                           AS mom_growth_pct
FROM monthly_billing
ORDER BY tier_name, billing_month;