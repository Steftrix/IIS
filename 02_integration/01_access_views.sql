-- ============================================================
-- ACCESS VIEWS
--
-- Purpose:
--   This layer provides access to external data sources through REST APIs.
--
-- Description:
--   Access views expose data from heterogeneous systems such as:
--   - PostgreSQL (via PostgREST)
--   - TimescaleDB (via PostgREST)
--   - MongoDB (via RestHeart)
--   - Neo4j (via REST API)
--
--   Data is retrieved using HTTP calls (UTL_HTTP) and transformed into
--   relational format using JSON_TABLE.
--
--   These views act as virtual tables over external systems and allow
--   Oracle to query remote data using standard SQL.
--
-- Notes:
--   - Data volume is limited (e.g. 10000 rows) to avoid large payloads.
--   - These views are not optimized for heavy analytics.
--   - They are used only as source inputs for the consolidation layer.
-- ============================================================
SET SQLBLANKLINES ON
SET DEFINE OFF
--- ============================================================
-- Helper functions for large JSON payloads over HTTP
-- ============================================================
CREATE OR REPLACE FUNCTION FDBO.GET_PG_ORDERS_JSON
RETURN CLOB
IS
    req   UTL_HTTP.req;
    resp  UTL_HTTP.resp;
    buf   VARCHAR2(32767);
    cl    CLOB;
BEGIN
    req := UTL_HTTP.begin_request(
        'http://postgrest-pg:3000/orders?select=id,user_id,invoice_id,status,shipping_country,created_at&limit=10000&order=created_at.desc'
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
SHOW ERRORS;



CREATE OR REPLACE FUNCTION FDBO.GET_PG_ORDER_ITEMS_JSON
RETURN CLOB
IS
    req   UTL_HTTP.req;
    resp  UTL_HTTP.resp;
    buf   VARCHAR2(32767);
    cl    CLOB;
BEGIN
    req := UTL_HTTP.begin_request(
        'http://postgrest-pg:3000/order_items?select=id,order_id,product_id,quantity,unit_price_usd,line_total_usd&limit=10000&order=id.asc'
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
SHOW ERRORS;



CREATE OR REPLACE FUNCTION FDBO.GET_PG_MKT_INVOICES_JSON
RETURN CLOB
IS
    req   UTL_HTTP.req;
    resp  UTL_HTTP.resp;
    buf   VARCHAR2(32767);
    cl    CLOB;
BEGIN
    req := UTL_HTTP.begin_request(
        'http://postgrest-pg:3000/marketplace_invoices?select=id,user_id,status,total_usd,created_at&limit=10000&order=created_at.desc'
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
SHOW ERRORS;



CREATE OR REPLACE FUNCTION FDBO.GET_TS_EVENTS_JSON
RETURN CLOB
IS
    req   UTL_HTTP.req;
    resp  UTL_HTTP.resp;
    buf   VARCHAR2(32767);
    cl    CLOB;
BEGIN
    req := UTL_HTTP.begin_request(
        'http://postgrest-ts:3000/events?select=id,user_id,event_type,product_id,metadata,occurred_at&limit=10000&order=occurred_at.desc'
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
SHOW ERRORS;



CREATE OR REPLACE FUNCTION FDBO.GET_MG_PRODUCTS_JSON
RETURN CLOB
IS
    req   UTL_HTTP.req;
    resp  UTL_HTTP.resp;
    buf   VARCHAR2(32767);
    cl    CLOB;
BEGIN
    req := UTL_HTTP.begin_request(
        'http://restheart:8080/products?pagesize=50000'
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



CREATE OR REPLACE FUNCTION FDBO.BASE64_TO_UUID(p_base64 IN VARCHAR2)
RETURN VARCHAR2 IS
    l_raw RAW(16);
    l_hex VARCHAR2(32);
BEGIN
    IF p_base64 IS NULL OR LENGTH(TRIM(p_base64)) = 0 THEN
        RETURN NULL;
    END IF;
    l_raw := UTL_ENCODE.BASE64_DECODE(UTL_RAW.CAST_TO_RAW(p_base64));
    l_hex := RAWTOHEX(l_raw);
    RETURN LOWER(
        SUBSTR(l_hex,  1, 8) || '-' ||
        SUBSTR(l_hex,  9, 4) || '-' ||
        SUBSTR(l_hex, 13, 4) || '-' ||
        SUBSTR(l_hex, 17, 4) || '-' ||
        SUBSTR(l_hex, 21, 12)
    );
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL; 
END;
/
-- ============================================================
-- ============================================================
-- ============================================================
-- 01. V_PG_ORDERS
-- Source:
--   PostgreSQL orders via PostgREST
-- ============================================================
CREATE OR REPLACE VIEW FDBO.V_PG_ORDERS AS
SELECT jt.*
FROM (
    SELECT FDBO.GET_PG_ORDERS_JSON() AS json_data
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
/
SHOW ERRORS;
select * from V_PG_ORDERS
-- ============================================================
-- 02. V_PG_ORDER_ITEMS
-- Source:
--   PostgreSQL order_items via PostgREST
-- ============================================================
CREATE OR REPLACE VIEW FDBO.V_PG_ORDER_ITEMS AS
SELECT jt.*
FROM (
    SELECT FDBO.GET_PG_ORDER_ITEMS_JSON() AS json_data
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
/
SHOW ERRORS;
select * from V_PG_ORDER_ITEMS

-- ============================================================
-- 03. V_PG_MKT_INVOICES
-- Source:
--   PostgreSQL marketplace_invoices via PostgREST
-- ============================================================
-- ============================================================
-- Staging tables
-- ============================================================
CREATE TABLE FDBO.JSON_STAGING_PG_ORDERS (
    id          NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    json_doc    CLOB,
    loaded_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE FDBO.JSON_STAGING_PG_ORDER_ITEMS (
    id          NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    json_doc    CLOB,
    loaded_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE FDBO.JSON_STAGING_PG_MKT_INVOICES (
    id          NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    json_doc    CLOB,
    loaded_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- Materialized views
-- ============================================================
CREATE MATERIALIZED VIEW FDBO.MV_PG_ORDERS
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT jt.*
FROM FDBO.JSON_STAGING_PG_ORDERS s,
JSON_TABLE(s.json_doc, '$[*]'
    COLUMNS (
        id               VARCHAR2(36)  PATH '$.id',
        user_id          VARCHAR2(36)  PATH '$.user_id',
        invoice_id       VARCHAR2(36)  PATH '$.invoice_id',
        status           VARCHAR2(20)  PATH '$.status',
        shipping_country VARCHAR2(10)  PATH '$.shipping_country',
        created_at       VARCHAR2(50)  PATH '$.created_at'
    )
) jt;

CREATE MATERIALIZED VIEW FDBO.MV_PG_ORDER_ITEMS
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT jt.*
FROM FDBO.JSON_STAGING_PG_ORDER_ITEMS s,
JSON_TABLE(s.json_doc, '$[*]'
    COLUMNS (
        id              VARCHAR2(36)  PATH '$.id',
        order_id        VARCHAR2(36)  PATH '$.order_id',
        product_id      VARCHAR2(36)  PATH '$.product_id',
        quantity        NUMBER        PATH '$.quantity',
        unit_price_usd  NUMBER(10,2)  PATH '$.unit_price_usd',
        line_total_usd  NUMBER(10,2)  PATH '$.line_total_usd'
    )
) jt;

CREATE MATERIALIZED VIEW FDBO.MV_PG_MKT_INVOICES
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT jt.*
FROM FDBO.JSON_STAGING_PG_MKT_INVOICES s,
JSON_TABLE(s.json_doc, '$[*]'
    COLUMNS (
        id          VARCHAR2(36)  PATH '$.id',
        user_id     VARCHAR2(36)  PATH '$.user_id',
        status      VARCHAR2(20)  PATH '$.status',
        total_usd   NUMBER(10,2)  PATH '$.total_usd',
        created_at  VARCHAR2(50)  PATH '$.created_at'
    )
) jt;

-- ============================================================
-- Indexes
-- ============================================================
CREATE INDEX IDX_MV_ORD_USER     ON FDBO.MV_PG_ORDERS(user_id);
CREATE INDEX IDX_MV_ORD_INV      ON FDBO.MV_PG_ORDERS(invoice_id);
CREATE INDEX IDX_MV_OI_ORDER     ON FDBO.MV_PG_ORDER_ITEMS(order_id);
CREATE INDEX IDX_MV_OI_PRODUCT   ON FDBO.MV_PG_ORDER_ITEMS(product_id);
CREATE INDEX IDX_MV_INV_ID       ON FDBO.MV_PG_MKT_INVOICES(id);

-- ============================================================
-- Refresh procedure (run this on a schedule)
-- ============================================================
TRUNCATE TABLE FDBO.JSON_STAGING_PG_ORDERS;
INSERT INTO FDBO.JSON_STAGING_PG_ORDERS (json_doc) VALUES (FDBO.GET_PG_ORDERS_JSON());
        
TRUNCATE TABLE FDBO.JSON_STAGING_PG_ORDER_ITEMS;
INSERT INTO FDBO.JSON_STAGING_PG_ORDER_ITEMS (json_doc) VALUES (FDBO.GET_PG_ORDER_ITEMS_JSON());
        
TRUNCATE TABLE FDBO.JSON_STAGING_PG_MKT_INVOICES;
INSERT INTO FDBO.JSON_STAGING_PG_MKT_INVOICES (json_doc) VALUES (FDBO.GET_PG_MKT_INVOICES_JSON());
        
COMMIT;
BEGIN       
    DBMS_MVIEW.REFRESH('FDBO.MV_PG_ORDERS',       method => 'C');
    DBMS_MVIEW.REFRESH('FDBO.MV_PG_ORDER_ITEMS',  method => 'C');
    DBMS_MVIEW.REFRESH('FDBO.MV_PG_MKT_INVOICES', method => 'C');
END;
-- ============================================================
-- ============================================================
-- ============================================================

CREATE OR REPLACE VIEW FDBO.V_PG_MKT_INVOICES AS
SELECT jt.*
FROM (
    SELECT FDBO.GET_PG_MKT_INVOICES_JSON() AS json_data
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
/
SHOW ERRORS;
select * from V_PG_MKT_INVOICES
-- ============================================================
-- 04. V_TS_EVENTS
-- Source:
--   TimescaleDB events via PostgREST
-- ============================================================
CREATE OR REPLACE VIEW FDBO.V_TS_EVENTS AS
SELECT jt.*
FROM (
    SELECT FDBO.GET_TS_EVENTS_JSON() AS json_data
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
        metadata    CLOB           FORMAT JSON PATH '$.metadata',
        occurred_at VARCHAR2(50)   PATH '$.occurred_at'
    )
) jt;
/
SHOW ERRORS;
SELECT count(*) FROM V_MG_PRODUCTS 
WHERE product_id = '4c9aae02-d249-4b84-a24d-e96f91ac6944';


select * from 
-- ============================================================
-- 05.a V_MG_PRODUCTS
-- Source:
--   MongoDB products via RestHeart
-- ============================================================
CREATE OR REPLACE VIEW FDBO.V_MG_PRODUCTS AS
SELECT
    jt.id_raw                                    AS product_id_b64,
    FDBO.BASE64_TO_UUID(jt.id_raw)               AS product_id,
    jt.seller_id_raw                             AS seller_id_b64,
    FDBO.BASE64_TO_UUID(jt.seller_id_raw)        AS seller_id,
    jt.name,
    jt.slug,
    jt.product_type,
    jt.description,
    jt.price_usd,
    jt.currency,
    jt.is_active,
    DATE '1970-01-01' + (jt.created_at_ms/1000)/86400 AS created_at,
    DATE '1970-01-01' + (jt.updated_at_ms/1000)/86400 AS updated_at
FROM (
    SELECT GET_MG_PRODUCTS_JSON() doc FROM dual
),
JSON_TABLE(
    doc, '$[*]'
    COLUMNS (
        id_raw          VARCHAR2(36)   PATH '$.id."$binary"."base64"',
        seller_id_raw   VARCHAR2(36)   PATH '$.seller_id."$binary"."base64"',
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
-- 05.b MV_MG_PRODUCTS
-- ============================================================
CREATE TABLE FDBO.JSON_STAGING (
    id NUMBER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    json_doc CLOB,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE MATERIALIZED VIEW FDBO.MV_MG_PRODUCTS
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
AS
SELECT
    jt.id_raw                                         AS product_id_b64,
    FDBO.BASE64_TO_UUID(jt.id_raw)                    AS product_id,
    jt.seller_id_raw                                  AS seller_id_b64,
    FDBO.BASE64_TO_UUID(jt.seller_id_raw)             AS seller_id,
    jt.name,
    jt.slug,
    jt.product_type,
    jt.description,
    jt.price_usd,
    jt.currency,
    jt.is_active,
    DATE '1970-01-01' + (jt.created_at_ms/1000)/86400 AS created_at,
    DATE '1970-01-01' + (jt.updated_at_ms/1000)/86400 AS updated_at
FROM FDBO.JSON_STAGING s,
JSON_TABLE(s.json_doc, '$[*]'
    COLUMNS (
        id_raw          VARCHAR2(36)   PATH '$.id."$binary"."base64"',
        seller_id_raw   VARCHAR2(36)   PATH '$.seller_id."$binary"."base64"',
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

-- 1. Wipe old data from the staging table
TRUNCATE TABLE FDBO.JSON_STAGING;

-- 2. Call your function and insert the result into the table
INSERT INTO FDBO.JSON_STAGING (json_doc) 
VALUES (FDBO.GET_MG_PRODUCTS_JSON());

COMMIT;

-- 3. Tell Oracle to refresh the Materialized View from the table
BEGIN
    DBMS_MVIEW.REFRESH('FDBO.MV_MG_PRODUCTS', method => 'C');
END;
/

CREATE INDEX IDX_MV_PROD_UUID ON FDBO.MV_MG_PRODUCTS(product_id);
CREATE INDEX IDX_MV_SELLER_ID ON FDBO.MV_MG_PRODUCTS(seller_id);
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