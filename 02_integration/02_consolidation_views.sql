-- ============================================================
-- CONSOLIDATION VIEWS
--
-- Purpose:
--   This layer integrates and normalizes data from multiple sources,
--   including Oracle, PostgreSQL, TimescaleDB, MongoDB, and Neo4j.
--
-- Description:
--   Consolidation views (V_CONS_*) provide a unified relational model
--   over heterogeneous data sources. They standardize structure, naming,
--   and data types in order to support downstream analytical processing.
--
--   This layer contains:
--   - descriptive master data (e.g. users, products, tiers)
--   - normalized transactional data (e.g. orders, invoices)
--   - integrated cross-source views used as the foundation for analytics
--
-- Notes:
--   - Some views are limited to 10000 rows due to the external access layer.
--   - These views are not final analytical objects, but serve as a base
--     for fact views and dimension views.
-- ============================================================
-- 1. V_CONS_USER_ACTIVITY
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
    u.id,
    u.email,
    u.full_name,
    u.country_code,
    u.city,
    u.created_at,
    u.is_active,
    e.id AS event_id,
    e.event_type AS event_type,
    e.product_id AS product_id,
    e.metadata AS metadata,
    TO_TIMESTAMP_TZ(e.occurred_at, 'YYYY-MM-DD"T"HH24:MI:SSTZH:TZM') AS occurred_at,
    p.name AS product_name,
    p.product_type AS product_type,
    p.seller_id AS seller_id,
    p.price_usd AS product_price_usd,
    p.is_active AS product_is_active
FROM FDBO.USERS u
JOIN FDBO.V_TS_EVENTS e
    ON e.user_id = u.id
LEFT JOIN FDBO.V_MG_PRODUCTS p
    ON p.product_id = e.product_id;
/
-- 2. V_CONS_PRODUCT_AFFINITY
-- Description:
--   Product co-purchase relationships from Neo4j enriched with
--   product metadata from the MongoDB product catalog.
-- Can be used for:
--   - product recommendation analysis
--   - cross-sell opportunity discovery
--   - product affinity analysis
--   - recommendation system experiments
CREATE OR REPLACE VIEW FDBO.V_CONS_PRODUCT_AFFINITY AS
SELECT
    bw.product_1_id,
    p1.name AS product_1_name,
    p1.product_type AS product_1_type,
    p1.price_usd AS product_1_price,
    p1.seller_id AS product_1_seller_id,
    bw.product_2_id,
    p2.name AS product_2_name,
    p2.product_type AS product_2_type,
    p2.price_usd AS product_2_price,
    p2.seller_id AS product_2_seller_id,
    bw.co_purchase_count
FROM FDBO.V_NEO4J_BOUGHT_WITH bw
LEFT JOIN FDBO.MV_MG_PRODUCTS p1
    ON p1.product_id = bw.product_1_id
LEFT JOIN FDBO.MV_MG_PRODUCTS p2
    ON p2.product_id = bw.product_2_id;
-- 3. V_CONS_SELLER_ORDER_REVENUE
-- Description:
--   Seller profiles joined with product catalog and marketplace orders.
-- Can be used for:
--   - seller revenue analysis
--   - seller product performance
--   - seller sales ranking
--   - marketplace seller metrics
CREATE OR REPLACE VIEW FDBO.V_CONS_SELLER_ORDER_REVENUE AS
SELECT
    s.user_id as seller_id,
    s.display_name,
    s.country_code,
    s.is_verified,
    p.product_id,
    p.name,
    p.product_type,
    o.id as order_id,
    o.user_id AS buyer_user_id,
    o.status as order_status,
    o.shipping_country,
    o.created_at,
    oi.id as order_item_id,
    oi.quantity,
    oi.unit_price_usd,
    oi.line_total_usd
FROM FDBO.EXT_SELLER_PROFILES s
JOIN FDBO.MV_MG_PRODUCTS p
    ON p.seller_id = s.user_id
JOIN FDBO.V_PG_ORDER_ITEMS oi
    ON oi.product_id = p.product_id
JOIN FDBO.V_PG_ORDERS o
    ON o.id = oi.order_id;
-- ============================================================
-- 4. V_CONS_INVOICES
-- Description:
--    Consolidated billing view joining subscription-based 
--    invoices with marketplace-specific invoices and user metadata.
-- Can be used for:
--    - Unified customer billing history
--    - AR (Accounts Receivable) aging and status tracking
--    - Comparing subscription vs. marketplace revenue per user
--    - Financial auditing and payment reconciliation
-- ============================================================
CREATE OR REPLACE VIEW FDBO.V_CONS_INVOICES AS
SELECT 
    si.ID AS invoice_id,
    si.User_ID AS user_id,
    u.FULL_NAME as full_name,
    si.INVOICE_TYPE,
    si.Status AS invoice_status,
    si.TOTAL_USD AS amount_usd,
    si.BILLING_PERIOD_START,
    si.BILLING_PERIOD_END,
    si.PAID_AT,
    si.DUE_AT,
    u.ID AS user_id_ref
FROM FDBO.SUBSCRIPTION_INVOICES si
INNER JOIN FDBO.USERS u ON si.USER_ID = u.ID
UNION ALL
SELECT 
    sm.ID AS invoice_id,
    sm.USER_ID AS user_id,
    u.FULL_NAME as full_name,
    NULL AS invoice_type,
    sm.STATUS AS invoice_status,
    sm.TOTAL_USD AS amount_usd,
    NULL AS billing_period_start,
    NULL AS billing_period_end,
    NULL AS paid_at,
    NULL AS due_at,
    u.ID AS user_id_ref
FROM FDBO.V_PG_MKT_INVOICES sm
INNER JOIN FDBO.USERS u ON sm.USER_ID = u.ID;