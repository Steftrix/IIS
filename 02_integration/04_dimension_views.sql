-- ============================================================
-- DIMENSION VIEWS
--
-- Purpose:
--   This layer provides descriptive attributes used to analyze fact data.
--
-- Description:
--   Dimension views (V_DIM_*) expose clean, reusable business attributes
--   such as user information, time hierarchies, status values, and
--   subscription tiers.
--
--   They are used to:
--   - group and filter fact data
--   - provide business context for analytical queries
--   - support slice-and-dice operations in OLAP
--
--   Typical dimensions include:
--   - user (country, city, status)
--   - time (year, month, quarter)
--   - subscription tier
--   - order, invoice, and subscription status
--
-- Notes:
--   - Dimension views are derived from consolidation views.
--   - They do not contain measures or aggregations.
--   - They are designed for reuse across multiple analytical queries.
-- ============================================================
-- 1. V_DIM_TIME
-- Description:
--   Time dimension derived from multiple date sources.
--   Enables analysis by year, month, quarter.
-- ============================================================
CREATE OR REPLACE VIEW FDBO.V_DIM_TIME AS
SELECT DISTINCT
    TRUNC(dt, 'DD') AS date_key,
    EXTRACT(YEAR FROM dt) AS year_no,
    EXTRACT(MONTH FROM dt) AS month_no,
    TO_CHAR(dt, 'YYYY-MM') AS year_month,
    TO_CHAR(dt, 'MON') AS month_name,
    TO_CHAR(dt, 'Q') AS quarter_no
FROM (
    SELECT CAST(user_created_at AS DATE) AS dt FROM FDBO.V_CONS_USERS
    UNION
    SELECT CAST(started_at AS DATE) AS dt FROM FDBO.V_CONS_SUBSCRIPTIONS
    UNION
    SELECT CAST(invoice_created_at AS DATE) AS dt FROM FDBO.V_CONS_SUB_INVOICES
    UNION
    SELECT CAST(order_created_at AS DATE) AS dt FROM FDBO.V_CONS_PG_ORDERS
);
-- ============================================================
-- 2. V_DIM_PRODUCT
-- Description:
--    Product dimension with price-based segmentation.
--    Categorizes products into budget, mid-range, premium, or luxury bands.
-- Can be used for:
--    - Price sensitivity analysis
--    - Catalog distribution reporting
--    - Revenue breakdown by price category
-- ============================================================
CREATE OR REPLACE VIEW FDBO.V_DIM_PRODUCT AS
SELECT
    p.product_id,
    p.name                  AS product_name,
    p.slug,
    p.product_type,
    p.price_usd,
    p.currency,
    p.is_active,
    p.seller_id,
    p.created_at            AS product_created_at,
    CASE
        WHEN p.price_usd < 20  THEN 'budget'
        WHEN p.price_usd < 60  THEN 'mid_range'
        WHEN p.price_usd < 120 THEN 'premium'
        ELSE 'luxury'
    END                     AS price_band
FROM FDBO.MV_MG_PRODUCTS p;
-- ============================================================
-- 3. V_DIM_SELLER
-- Description:
--    Seller dimension with maturity tracking based on tenure.
--    Handles ISO 8601 timestamp conversion from external CSV sources.
-- Can be used for:
--    - Seller lifecycle analysis (New vs. Veteran)
--    - Payout and verification status monitoring
--    - Cohort analysis by seller registration date
-- ============================================================
CREATE OR REPLACE VIEW FDBO.V_DIM_SELLER AS
WITH formatted_sellers AS (
    SELECT 
        s.*,
        CAST(TO_TIMESTAMP_TZ(
            TRIM(BOTH '"' FROM s.created_at), 
            'YYYY-MM-DD"T"HH24:MI:SSTZH:TZM'
        ) AS DATE) as created_at_dt
    FROM FDBO.EXT_SELLER_PROFILES s
)
SELECT
    user_id,
    display_name,
    legal_name,
    payout_email,
    country_code,
    created_at_dt           AS created_at,
    is_verified,
    CASE
        WHEN created_at_dt >= ADD_MONTHS(SYSDATE, -6)  THEN 'new'
        WHEN created_at_dt >= ADD_MONTHS(SYSDATE, -24) THEN 'established'
        ELSE 'veteran'
    END                     AS seller_maturity
FROM formatted_sellers;
-- ============================================================
-- 4. V_DIM_PRODUCT_AFFINITY_TYPE
-- Description:
--    Graph-derived dimension mapping product co-purchase relationships.
--    Identifies if paired products share types or sellers.
-- Can be used for:
--    - Market basket analysis (Frequently Bought Together)
--    - Cross-selling and up-selling strategy
--    - Analyzing seller loyalty vs. product type preference
-- ============================================================
CREATE OR REPLACE VIEW FDBO.V_DIM_PRODUCT_AFFINITY_TYPE AS
SELECT
    nb.product_1_id,
    nb.product_2_id,
    nb.co_purchase_count,
    p1.product_type         AS product_1_type,
    p2.product_type         AS product_2_type,
    p1.seller_id            AS product_1_seller_id,
    p2.seller_id            AS product_2_seller_id,
    CASE
        WHEN p1.product_type = p2.product_type THEN 'same_type'
        ELSE 'cross_type'
    END                     AS affinity_type_category,
    CASE
        WHEN p1.seller_id = p2.seller_id THEN 'same_seller'
        ELSE 'cross_seller'
    END                     AS affinity_seller_category
FROM FDBO.V_NEO4J_BOUGHT_WITH nb
LEFT JOIN FDBO.MV_MG_PRODUCTS p1
    ON p1.product_id = nb.product_1_id
LEFT JOIN FDBO.MV_MG_PRODUCTS p2
    ON p2.product_id = nb.product_2_id;