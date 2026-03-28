-- ============================================================
-- FACT VIEWS (ROLAP)
--
-- Purpose:
--   This layer exposes measurable events and transactions used for OLAP.
--
-- Description:
--   Fact views are implemented as relational views (ROLAP) over the
--   consolidation layer. They contain business measures such as counts,
--   quantities, revenue, and activity metrics, together with keys that
--   link to dimension views.
--
--   These views act as fact tables in the analytical model, without
--   requiring physical data warehouse tables.
--
--   Examples of measures:
--   - total_events
--   - order_count
--   - total_revenue_usd
--   - quantity
--   - retention flags
--
-- Notes:
--   - Fact views are derived from consolidation views.
--   - They are used directly by OLAP queries (ROLLUP, CUBE, window functions).
--   - The model follows a ROLAP (Relational OLAP) architecture.
-- ============================================================
-- 01. V_CONS_USER_SUBSCRIPTION
-- Description:
--   General user + subscription + tier + historical pricing view.
-- Can be used for:
--   - subscription overview per user
--   - active/cancelled subscriptions by tier
--   - user subscription timeline
--   - subscription lifecycle analysis
--   - retention/churn preparation
--   - subscription revenue analysis
CREATE OR REPLACE VIEW FDBO.V_CONS_USER_SUBSCRIPTION AS 
SELECT 
    u.id AS user_id,
    u.email, u.full_name,
    u.country_code,
    u.city,
    u.created_at AS user_created_at,
    u.last_login_at,
    u.is_active AS user_is_active,
    s.id AS sub_id,
    s.status AS sub_status,
    s.started_at,
    s.current_period_start,
    s.current_period_end,
    s.cancelled_at,
    s.cancel_reason, 
    s.billing_cycle,
    s.created_at AS sub_created_at,
    s.updated_at AS sub_updated_at, 
    s.tier_id, t.name AS tier_name,
    t.description AS tier_description, 
    p.id AS price_id,
    p.monthly_price_usd,
    p.valid_from AS price_valid_from,
    p.valid_to AS price_valid_to,
    p.is_active AS price_is_active 
FROM FDBO.USERS u 
JOIN FDBO.SUBSCRIPTIONS s 
    ON s.user_id = u.id 
LEFT JOIN FDBO.SUBSCRIPTION_TIERS t 
    ON t.id = s.tier_id 
LEFT JOIN FDBO.SUBSCRIPTION_TIER_PRICING p 
    ON p.tier_id = s.tier_id 
    AND s.started_at >= p.valid_from 
    AND (p.valid_to IS NULL OR s.started_at < p.valid_to);
-- 02. V_CONS_SUB_BILLING
-- Description:
--   Invoice headers + invoice lines for Oracle subscription billing.
-- Can be used for:
--   - billing detail analysis
--   - invoice breakdown analysis
--   - subscription monetisation analysis
CREATE OR REPLACE VIEW FDBO.V_CONS_SUB_BILLING AS
SELECT
    i.id                   AS invoice_id,   
    i.user_id,
    i.invoice_type,
    i.status               AS invoice_status,
    i.subtotal_usd,
    i.tax_usd,
    i.discount_usd,
    i.total_usd,
    i.subscription_id,                      
    i.billing_period_start,
    i.billing_period_end,
    i.paid_at,
    i.due_at,
    i.created_at           AS invoice_created_at,
    l.id                   AS line_item_id,
    l.product_id,
    l.description          AS line_description,
    l.quantity,
    l.unit_price_usd,
    l.line_total_usd,
    l.created_at           AS line_created_at
FROM FDBO.SUBSCRIPTION_INVOICES i
LEFT JOIN FDBO.SUBSCRIPTION_INVOICE_LINES l
    ON l.invoice_id = i.id;
-- 03. V_CONS_ORDER_TRANSACTIONS
-- Description:
--   Combines PostgreSQL order headers and order items.
-- Can be used for:
--   - full transactional sales analysis
--   - order value analysis
--   - line item reporting
CREATE OR REPLACE VIEW FDBO.V_CONS_ORDER_TRANSACTIONS AS
SELECT
    o.order_id,
    o.user_id,
    o.marketplace_invoice_id,
    o.order_status,
    o.shipping_country,
    o.order_created_at,
    i.order_item_id,
    i.product_id,
    i.quantity,
    i.unit_price_usd,
    i.line_total_usd
FROM FDBO.V_CONS_PG_ORDERS o
LEFT JOIN FDBO.V_CONS_PG_ORDER_ITEMS i
    ON i.order_id = o.order_id;
-- 04. V_CONS_USER_ORDERS
-- Description:
--   Oracle users + PostgreSQL transactions.
-- Can be used for:
--   - purchase behaviour per user
--   - buyers vs non-buyers
--   - user order history
--   - order funnel preparation
CREATE OR REPLACE VIEW FDBO.V_CONS_USER_ORDERS AS
SELECT
    u.id,
    u.email,
    u.full_name,
    u.country_code,
    u.city,
    o.id                AS order_id,
    o.status            AS order_status,
    o.shipping_country,
    o.created_at        AS order_created_at,
    mi.id               AS marketplace_invoice_id,
    mi.status           AS invoice_status,
    oi.product_id,
    oi.quantity,
    oi.unit_price_usd,
    oi.line_total_usd
FROM FDBO.USERS u
JOIN FDBO.MV_PG_ORDERS o
    ON o.user_id = u.id
JOIN FDBO.MV_PG_ORDER_ITEMS oi
    ON oi.order_id = o.id
LEFT JOIN FDBO.MV_PG_MKT_INVOICES mi
    ON o.invoice_id = mi.id;
-- 05. V_CONS_USER_ACTIVITY
-- Description:
--   Oracle users + Timescale events + Mongo products.
-- Can be used for:
--   - behavioural analysis
--   - product interaction analysis
--   - event-type trend analysis
--   - pre-purchase activity analysis
--   - funnel preparation
CREATE OR REPLACE VIEW FDBO.V_CONS_USER_ACTIVITY AS
SELECT
    u.user_id,
    u.user_email,
    u.user_full_name,
    u.user_country_code,
    u.user_city,
    u.user_created_at,
    u.user_last_login_at,
    u.user_is_active,
    e.id          AS event_id,
    e.event_type  AS event_type,
    e.product_id  AS product_id,
    e.metadata    AS metadata,
    TO_TIMESTAMP_TZ(e.occurred_at, 'YYYY-MM-DD"T"HH24:MI:SSTZH:TZM') AS occurred_at,
    p.name        AS product_name,
    p.product_type AS product_type,
    p.seller_id   AS seller_id,
    p.price_usd   AS product_price_usd,
    p.is_active   AS product_is_active
FROM FDBO.V_CONS_USERS u
JOIN FDBO.V_TS_EVENTS e
    ON e.user_id = u.user_id
LEFT JOIN FDBO.V_MG_PRODUCTS p
    ON p.product_id = e.product_id;
-- 06. V_CONS_SUB_COHORT 
-- Description:
--   cohort retention / churn by subscription tier.
-- Can be used for:
--   - cohort grouping
--   - retention flags
--   - churn flags
--   - days active analysis
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
    TRUNC(us.started_at, 'MM') AS cohort_month,
    CASE
        WHEN us.cancelled_at IS NOT NULL        THEN us.cancelled_at - us.started_at
        WHEN us.current_period_end IS NOT NULL  THEN us.current_period_end - us.started_at
        ELSE SYSDATE - us.started_at
    END AS days_active,
    CASE WHEN NVL(us.cancelled_at, us.current_period_end) >= us.started_at + 30 THEN 1 ELSE 0 END AS retained_30d,
    CASE WHEN NVL(us.cancelled_at, us.current_period_end) >= us.started_at + 90 THEN 1 ELSE 0 END AS retained_90d,
    CASE WHEN us.cancelled_at IS NOT NULL THEN 1 ELSE 0 END AS churned_flag
FROM FDBO.V_CONS_USER_SUBSCRIPTION us;
-- 07. V_CONS_USER_ENGAGEMENT_SALES
-- Description:
--   Monthly cross-source user engagement and sales view.
--   Combines Oracle user dimensions, TimescaleDB behavioural activity,
--   and PostgreSQL transactional order data at user-month level.
-- Can be used for:
--   - monthly engagement analysis per user and country
--   - activity vs sales correlation analysis
--   - event intensity vs order volume analysis
--   - cross-source behavioural and commercial reporting
--   - OLAP preparation for rollup, cube, and window-based aggregations
CREATE OR REPLACE VIEW FDBO.V_CONS_USER_ENGAGEMENT_SALES AS
WITH activity_monthly AS (
    SELECT
        ua.user_id,
        ua.user_country_code,
        TRUNC(CAST(ua.occurred_at AS DATE), 'MM')        AS activity_month,
        COUNT(*)                                        AS total_events,
        SUM(CASE WHEN ua.event_type = 'page_view'      THEN 1 ELSE 0 END) AS page_views,
        SUM(CASE WHEN ua.event_type = 'product_view'   THEN 1 ELSE 0 END) AS product_views,
        SUM(CASE WHEN ua.event_type = 'search'         THEN 1 ELSE 0 END) AS searches,
        SUM(CASE WHEN ua.event_type = 'add_to_cart'    THEN 1 ELSE 0 END) AS add_to_cart_events,
        SUM(CASE WHEN ua.event_type = 'checkout_start' THEN 1 ELSE 0 END) AS checkout_starts,
        SUM(CASE WHEN ua.event_type = 'purchase'       THEN 1 ELSE 0 END) AS purchase_events
    FROM FDBO.V_CONS_USER_ACTIVITY ua
    GROUP BY
        ua.user_id,
        ua.user_country_code,
        TRUNC(CAST(ua.occurred_at AS DATE), 'MM')
),
orders_monthly AS (
    SELECT
        o.user_id,
        TRUNC(TO_DATE(SUBSTR(o.created_at, 1, 10), 'YYYY-MM-DD'), 'MM') AS order_month,
        COUNT(DISTINCT o.id)                            AS order_count,
        SUM(NVL(oi.quantity, 0))                        AS total_quantity,
        SUM(NVL(oi.line_total_usd, 0))                  AS total_revenue_usd
    FROM FDBO.V_PG_ORDERS o
    JOIN FDBO.V_PG_ORDER_ITEMS oi
        ON oi.order_id = o.id
    GROUP BY
        o.user_id,
        TRUNC(TO_DATE(SUBSTR(o.created_at, 1, 10), 'YYYY-MM-DD'), 'MM')
)
SELECT
    a.user_id,
    a.user_country_code,
    a.activity_month,
    a.total_events,
    a.page_views,
    a.product_views,
    a.searches,
    a.add_to_cart_events,
    a.checkout_starts,
    a.purchase_events,
    NVL(o.order_count, 0)       AS order_count,
    NVL(o.total_quantity, 0)    AS total_quantity,
    NVL(o.total_revenue_usd, 0) AS total_revenue_usd
FROM activity_monthly a
LEFT JOIN orders_monthly o
    ON  o.user_id     = a.user_id
    AND o.order_month = a.activity_month;