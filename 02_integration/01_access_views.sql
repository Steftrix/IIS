SET SQLBLANKLINES ON
SET DEFINE OFF
-- Access Views
-- Run as: FDBO
-- Database: XEPDB1
-- Purpose:
--   Creates Oracle access views over external REST endpoints exposed by:
--   - PostgreSQL via PostgREST
--   - TimescaleDB via PostgREST
--   - MongoDB via RestHeart
--   - Neo4j

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
-- Due to the massive size of the mongodb, we use a function to return the response to CLOB instead of VARCHAR2
CREATE OR REPLACE FUNCTION get_products_json
RETURN CLOB
IS
    req   UTL_HTTP.req;
    resp  UTL_HTTP.resp;
    buf   VARCHAR2(32767);
    cl    CLOB;
BEGIN
    req := UTL_HTTP.begin_request(
        'http://restheart:8080/products?pagesize=1000'
    );

    resp := UTL_HTTP.get_response(req);

    DBMS_LOB.createtemporary(cl, TRUE);

    LOOP
        UTL_HTTP.read_text(resp, buf, 32767);
        DBMS_LOB.writeappend(cl, LENGTH(buf), buf);
    END LOOP;

EXCEPTION
    WHEN UTL_HTTP.end_of_body THEN
        UTL_HTTP.end_response(resp);
        RETURN cl;
END;
/
-- The View is here
CREATE OR REPLACE VIEW FDBO.V_MG_PRODUCTS AS
SELECT jt.id, jt.seller_id, jt.name, jt.slug, jt.product_type, jt.description, jt.price_usd, jt.currency, jt.is_active,
    DATE '1970-01-01' + (jt.created_at_ms/1000)/86400  AS created_at,
    DATE '1970-01-01' + (jt.updated_at_ms/1000)/86400  AS updated_at
FROM (
    SELECT get_products_json() doc
    FROM dual
),
JSON_TABLE(
    doc,
    '$[*]'
    COLUMNS (
        id              VARCHAR2(36)   PATH '$._id."$oid"',
        seller_id       VARCHAR2(36)   PATH '$.seller_id."$binary"."base64"',
        name            VARCHAR2(255)  PATH '$.name',
        slug            VARCHAR2(255)  PATH '$.slug',
        product_type    VARCHAR2(100)  PATH '$.product_type',
        description     VARCHAR2(4000) PATH '$.description',
        price_usd       NUMBER(10,2)   PATH '$.price_usd',
        currency        VARCHAR2(10)   PATH '$.currency',
        is_active       VARCHAR2(5)    PATH '$.is_active',
        created_at_ms   NUMBER         PATH '$.created_at."$date"',
        updated_at_ms   NUMBER         PATH '$.updated_at."$date"'

    )
) jt;
-- ============================================================
-- 06. V_NEO4J_BOUGHT_WITH
-- Source:
-- Neo4j -- Products bought together
-- ============================================================
CREATE OR REPLACE VIEW V_NEO4J_BOUGHT_WITH AS
WITH json AS (
    SELECT query_neo4j_rest_graph_data(
        'http://neo4j:7474/db/neo4j/query/v2',
        'MATCH (p1:Product)-[r:BOUGHT_WITH]->(p2:Product) RETURN p1.id, p1.name, p2.id, p2.name, r.co_purchase_count ORDER BY r.co_purchase_count DESC',
        'neo4j', 'neo4j_admin') doc
    FROM dual
)
SELECT
    product_1_id,
    product_1_name,
    product_2_id,
    product_2_name,
    co_purchase_count
FROM JSON_TABLE( (SELECT doc FROM json), '$.data.values[*]'
    COLUMNS (
        product_1_id      VARCHAR2(100) PATH '$[0]' NULL ON ERROR,
        product_1_name    VARCHAR2(255) PATH '$[1]' NULL ON ERROR,
        product_2_id      VARCHAR2(100) PATH '$[2]' NULL ON ERROR,
        product_2_name    VARCHAR2(255) PATH '$[3]' NULL ON ERROR,
        co_purchase_count NUMBER        PATH '$[4]' NULL ON ERROR
    )
);

Select * from V_NEO4J_BOUGHT_WITH