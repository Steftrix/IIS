SET SQLBLANKLINES ON
SET DEFINE OFF
-- Script 04: Access Views
-- Run as: FDBO
-- Database: XEPDB1
-- Purpose:
--   Creates Oracle access views over external REST endpoints exposed by:
--   - PostgreSQL via PostgREST
--   - TimescaleDB via PostgREST
--   - MongoDB via RestHeart
--   - DONT FORGET NEO4J!!!

-- ============================================================
-- 01. V_PG_ORDERS
-- Source:
--   PostgreSQL orders via PostgREST
-- ============================================================
CREATE OR REPLACE VIEW FDBO.V_PG_ORDERS AS
SELECT jt.*
FROM (
    SELECT UTL_HTTP.REQUEST(
        'http://postgrest-pg:3000/orders?limit=100'
    ) AS json_data
    FROM dual
),
JSON_TABLE(
    json_data,
    '$[*]'
    COLUMNS (
        id               VARCHAR2(36)   PATH '$.id',
        user_id          VARCHAR2(36)   PATH '$.user_id',
        invoice_id       VARCHAR2(36)   PATH '$.invoice_id',
        status           VARCHAR2(20)   PATH '$.status',
        shipping_country VARCHAR2(10)   PATH '$.shipping_country',
        created_at       VARCHAR2(50)   PATH '$.created_at'
    )
) jt;


-- ============================================================
-- 02. V_PG_ORDER_ITEMS
-- Source:
--   PostgreSQL order_items via PostgREST
-- ============================================================
CREATE OR REPLACE VIEW FDBO.V_PG_ORDER_ITEMS AS
SELECT jt.*
FROM (
    SELECT UTL_HTTP.REQUEST(
        'http://postgrest-pg:3000/order_items?limit=100'
    ) AS json_data
    FROM dual
),
JSON_TABLE(
    json_data,
    '$[*]'
    COLUMNS (
        id              VARCHAR2(36)   PATH '$.id',
        order_id        VARCHAR2(36)   PATH '$.order_id',
        product_id      VARCHAR2(36)   PATH '$.product_id',
        quantity        NUMBER         PATH '$.quantity',
        unit_price_usd  NUMBER(10,2)   PATH '$.unit_price_usd',
        line_total_usd  NUMBER(10,2)   PATH '$.line_total_usd'
    )
) jt;


-- ============================================================
-- 03. V_PG_MKT_INVOICES
-- Source:
--   PostgreSQL marketplace_invoices via PostgREST
-- ============================================================
CREATE OR REPLACE VIEW FDBO.V_PG_MKT_INVOICES AS
SELECT jt.*
FROM (
    SELECT UTL_HTTP.REQUEST(
        'http://postgrest-pg:3000/marketplace_invoices?limit=100'
    ) AS json_data
    FROM dual
),
JSON_TABLE(
    json_data,
    '$[*]'
    COLUMNS (
        id          VARCHAR2(36)   PATH '$.id',
        user_id     VARCHAR2(36)   PATH '$.user_id',
        status      VARCHAR2(20)   PATH '$.status',
        total_usd   NUMBER(10,2)   PATH '$.total_usd',
        created_at  VARCHAR2(50)   PATH '$.created_at'
    )
) jt;


-- ============================================================
-- 04. V_TS_EVENTS
-- Source:
--   TimescaleDB events via PostgREST
-- ============================================================
CREATE OR REPLACE VIEW FDBO.V_TS_EVENTS AS
SELECT jt.*
FROM (
    SELECT UTL_HTTP.REQUEST(
        'http://postgrest-ts:3000/events?limit=100'
    ) AS json_data
    FROM dual
),
JSON_TABLE(
    json_data,
    '$[*]'
    COLUMNS (
        id          VARCHAR2(36)   PATH '$.id',
        user_id     VARCHAR2(36)   PATH '$.user_id',
        event_type  VARCHAR2(100)  PATH '$.event_type',
        product_id  VARCHAR2(36)   PATH '$.product_id',
        metadata    CLOB           PATH '$.metadata',
        occurred_at VARCHAR2(50)   PATH '$.occurred_at'
    )
) jt;


-- ============================================================
-- 05. V_MG_PRODUCTS
-- Source:
--   MongoDB products via RestHeart
-- ============================================================
CREATE OR REPLACE VIEW FDBO.V_MG_PRODUCTS AS
SELECT jt.*
FROM (
    SELECT UTL_HTTP.REQUEST(
        'http://restheart:8080/products?pagesize=1'
    ) AS json_data
    FROM dual
),
JSON_TABLE(
    json_data,
    '$[*]'
    COLUMNS (
        id            VARCHAR2(36)   PATH '$.id',
        name          VARCHAR2(255)  PATH '$.name',
        product_type  VARCHAR2(100)  PATH '$.product_type',
        price_usd     NUMBER(10,2)   PATH '$.price_usd',
        seller_id     VARCHAR2(36)   PATH '$.seller_id',
        is_active     VARCHAR2(20)   PATH '$.is_active'
    )
) jt;

