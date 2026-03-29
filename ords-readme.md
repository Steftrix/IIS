# Oracle REST Data Services (ORDS) Setup and Configuration

## 1. Download and Install ORDS

Run the following commands to download, extract, and begin the installation of ORDS:

```bash
# Download the latest ORDS release
curl -o ords-latest.zip https://download.oracle.com/otn_software/java/ords/ords-latest.zip

# Create setup directory and extract
mkdir ords-setup
tar -xf ords-latest.zip -C ords-setup
cd ords-setup

# Run the installer
# Basic install credentials: localhost, port 1521, service XEPDB1, USER SYS, PW Oracle21c
.\ords --config ords-setup/config install
```

## 2. Start the ORDS Server

Once installed, you can serve the ORDS API on port `8181`:

```bash
./ords-setup/bin/ords --config ./ords-setup/bin/ords-setup/config serve --port 8181
```

Access the ORDS interface at: [http://localhost:8181/ords/](http://localhost:8181/ords/)

---

## 3. Database Configuration (Run as `SYS` Admin)

Connect to your Oracle database as `SYS` and execute the following configuration steps for the `FDBO` user:

```sql
GRANT INHERIT PRIVILEGES ON USER SYS TO ORDS_METADATA;

SELECT SYS_CONTEXT('USERENV', 'CON_NAME') AS con_name FROM DUAL;

GRANT CONNECT, RESOURCE TO FDBO;
GRANT CREATE VIEW TO FDBO;
GRANT CREATE SYNONYM TO FDBO;
GRANT CREATE DATABASE LINK TO FDBO;
GRANT EXECUTE ON UTL_HTTP TO FDBO;

ALTER USER FDBO QUOTA UNLIMITED ON USERS;

GRANT INHERIT PRIVILEGES ON USER SYS TO ORDS_METADATA;

BEGIN
  ORDS.ENABLE_SCHEMA(
    p_enabled             => TRUE,
    p_schema              => 'FDBO',
    p_url_mapping_type    => 'BASE_PATH',
    p_url_mapping_pattern => 'fdbo',
    p_auto_rest_auth      => FALSE
  );
  COMMIT;
END;
/
```

---

## 4. Define Analytics Module (Run as `FDBO`)

Connect to the database as `FDBO` (password: `fdbo_pass`) and define the REST module:

```sql
BEGIN
  ORDS.DEFINE_MODULE(
    p_module_name    => 'analytics',
    p_base_path      => '/analytics/',
    p_items_per_page => 100,
    p_status         => 'PUBLISHED',
    p_comments       => 'IIS Analytics REST API'
  );
  COMMIT;
END;
/
```

---

## 5. Define Endpoints (Run as `FDBO`)

Use the following template to define your 8 endpoints. Run this block once per query, changing the `v_pattern` and `v_sql` variables accordingly.

```sql
DECLARE
  v_module  VARCHAR2(100) := 'analytics';
  v_pattern VARCHAR2(100) := 'your-endpoint-name';   -- e.g. 'sales-by-region'
  v_sql     CLOB :=
    'SELECT col1, col2, col3
     FROM   your_fdbo_view_or_olap_query
     ORDER  BY col1';
BEGIN
  ORDS.DEFINE_TEMPLATE(
    p_module_name => v_module,
    p_pattern     => v_pattern
  );
  
  ORDS.DEFINE_HANDLER(
    p_module_name    => v_module,
    p_pattern        => v_pattern,
    p_method         => 'GET',
    p_source_type    => ORDS.source_type_collection_feed,
    p_source         => v_sql,
    p_items_per_page => 1000
  );
  COMMIT;
END;
/
```

---

## 6. Validation

To verify that your REST modules are active, run the following query as `FDBO`:

```sql
SELECT module_name, uri_prefix, status 
FROM ords_metadata.user_ords_modules;
```

---

## 7. Custom Endpoints

```sql
BEGIN
  -------------------------------------------------------------
  -- 1. V_CONS_SUB_COHORT (Cohort Retention)
  -- Endpoint: /ords/fdbo/analytics/sub-cohort
  -------------------------------------------------------------
  DECLARE
    v_module  VARCHAR2(100) := 'analytics';
    v_pattern VARCHAR2(100) := 'sub-cohort';
    v_sql     CLOB :=
      'SELECT user_id, user_email, user_full_name, user_country_code, user_city,
              subscription_id, tier_id, tier_name, subscription_status, billing_cycle,
              started_at, current_period_start, current_period_end, cancelled_at,
              cancel_reason, monthly_price_usd, cohort_month, days_active,
              retained_30d, retained_90d, churned_flag
       FROM FDBO.V_CONS_SUB_COHORT
       ORDER BY cohort_month DESC, user_id';
  BEGIN
    ORDS.DEFINE_TEMPLATE(p_module_name => v_module, p_pattern => v_pattern);
    ORDS.DEFINE_HANDLER(
      p_module_name => v_module, p_pattern => v_pattern, p_method => 'GET',
      p_source_type => ORDS.source_type_collection_feed, p_source => v_sql, p_items_per_page => 1000
    );
  END;


  -------------------------------------------------------------
  -- 2. V_OLAP_ENGAGEMENT_REVENUE_MONTHLY
  -- Endpoint: /ords/fdbo/analytics/engagement-revenue
  -------------------------------------------------------------
  DECLARE
    v_module  VARCHAR2(100) := 'analytics';
    v_pattern VARCHAR2(100) := 'engagement-revenue';
    v_sql     CLOB :=
      'SELECT activity_month, country_code, total_events, total_page_views, 
              total_product_views, total_searches, total_add_to_cart, total_checkout_starts, 
              total_purchase_events, total_orders, total_quantity, total_revenue_usd, 
              cumulative_revenue_usd, cumulative_orders, prev_month_revenue_usd, 
              grp_month, grp_country
       FROM FDBO.V_OLAP_ENGAGEMENT_REVENUE_MONTHLY
       ORDER BY grp_month, activity_month DESC, grp_country, country_code';
  BEGIN
    ORDS.DEFINE_TEMPLATE(p_module_name => v_module, p_pattern => v_pattern);
    ORDS.DEFINE_HANDLER(
      p_module_name => v_module, p_pattern => v_pattern, p_method => 'GET',
      p_source_type => ORDS.source_type_collection_feed, p_source => v_sql, p_items_per_page => 1000
    );
  END;

  -------------------------------------------------------------
  -- 3. Geographic Market Density (From pure SELECT query)
  -- Endpoint: /ords/fdbo/analytics/geo-density
  -------------------------------------------------------------
  DECLARE
    v_module  VARCHAR2(100) := 'analytics';
    v_pattern VARCHAR2(100) := 'geo-density';
    v_sql     CLOB :=
      'WITH geo_banded AS (
           SELECT country_code, total_users, total_sellers, total_orders,
                  CASE
                      WHEN total_orders >= PERCENTILE_CONT(0.66) WITHIN GROUP (ORDER BY total_orders) OVER () THEN ''High''
                      WHEN total_orders >= PERCENTILE_CONT(0.33) WITHIN GROUP (ORDER BY total_orders) OVER () THEN ''Mid''
                      ELSE ''Low''
                  END AS activity_band
           FROM FDBO.V_DIM_GEOGRAPHY
           WHERE country_code IS NOT NULL
       ),
       rollup_agg AS (
           SELECT CASE GROUPING(activity_band) WHEN 1 THEN ''** ALL BANDS **'' ELSE activity_band END AS activity_band,
                  CASE GROUPING(country_code)  WHEN 1 THEN NULL               ELSE country_code  END AS country_code,
                  GROUPING(country_code)                                                             AS is_subtotal,
                  SUM(total_users)                                                                   AS total_users,
                  SUM(total_sellers)                                                                 AS total_sellers,
                  SUM(total_orders)                                                                  AS total_orders,
                  ROUND(SUM(total_users) / NULLIF(SUM(total_sellers), 0), 2)                        AS buyer_seller_ratio,
                  CASE WHEN GROUPING(activity_band)=1 AND GROUPING(country_code)=1 THEN ''Grand Total''
                       WHEN GROUPING(country_code)=1 THEN ''Activity Band Subtotal'' ELSE ''Country Detail'' END AS grouping_label
           FROM geo_banded
           GROUP BY ROLLUP(activity_band, country_code)
       ),
       detail_ranked AS (
           SELECT activity_band, country_code, is_subtotal, total_users, total_sellers, total_orders,
                  buyer_seller_ratio, grouping_label,
                  NTILE(4) OVER (ORDER BY total_orders DESC) AS order_volume_quartile,
                  RANK()   OVER (ORDER BY total_orders DESC) AS country_rank
           FROM rollup_agg WHERE is_subtotal = 0
       )
       SELECT activity_band, country_code, total_users, total_sellers, total_orders,
              buyer_seller_ratio, ROUND(RATIO_TO_REPORT(total_users) OVER () * 100, 2) AS user_share_pct,
              order_volume_quartile, country_rank, grouping_label
       FROM detail_ranked
       UNION ALL
       SELECT activity_band, country_code, total_users, total_sellers, total_orders,
              buyer_seller_ratio, NULL AS user_share_pct, NULL AS order_volume_quartile, NULL AS country_rank, grouping_label
       FROM rollup_agg WHERE is_subtotal = 1
       ORDER BY grouping_label DESC, total_orders DESC NULLS LAST';
  BEGIN
    ORDS.DEFINE_TEMPLATE(p_module_name => v_module, p_pattern => v_pattern);
    ORDS.DEFINE_HANDLER(
      p_module_name => v_module, p_pattern => v_pattern, p_method => 'GET',
      p_source_type => ORDS.source_type_collection_feed, p_source => v_sql, p_items_per_page => 1000
    );
  END;

  -------------------------------------------------------------
  -- 4. V_RPT_PRODUCT_AFFINITY_ANALYTICS
  -- Endpoint: /ords/fdbo/analytics/product-affinity
  -------------------------------------------------------------
  DECLARE
    v_module  VARCHAR2(100) := 'analytics';
    v_pattern VARCHAR2(100) := 'product-affinity';
    v_sql     CLOB :=
      'SELECT affinity_type_category, affinity_seller_category, product_1_id, product_2_id,
              pair_count, total_co_purchases, avg_co_purchases, rank_in_type_category, 
              running_co_purchases, vs_global_avg_flag, grouping_label
       FROM FDBO.V_RPT_PRODUCT_AFFINITY_ANALYTICS';
  BEGIN
    ORDS.DEFINE_TEMPLATE(p_module_name => v_module, p_pattern => v_pattern);
    ORDS.DEFINE_HANDLER(
      p_module_name => v_module, p_pattern => v_pattern, p_method => 'GET',
      p_source_type => ORDS.source_type_collection_feed, p_source => v_sql, p_items_per_page => 1000
    );
  END;

  -------------------------------------------------------------
  -- 5. V_OLAP_SUB_RETENTION_COHORT (Aggregated Analytics View)
  -- Endpoint: /ords/fdbo/analytics/sub-retention
  -------------------------------------------------------------
  DECLARE
    v_module  VARCHAR2(100) := 'analytics';
    v_pattern VARCHAR2(100) := 'sub-retention';
    v_sql     CLOB :=
      'SELECT tier_name, cohort_month, cohort_size, churned_this_month,
              cumulative_churned, survival_rate_pct, churn_rate_pct, 
              rolling_3m_churn_rate, next_month_churn_proj
       FROM FDBO.V_OLAP_SUB_RETENTION_COHORT
       ORDER BY tier_name, cohort_month DESC';
  BEGIN
    ORDS.DEFINE_TEMPLATE(p_module_name => v_module, p_pattern => v_pattern);
    ORDS.DEFINE_HANDLER(
      p_module_name => v_module, p_pattern => v_pattern, p_method => 'GET',
      p_source_type => ORDS.source_type_collection_feed, p_source => v_sql, p_items_per_page => 1000
    );
  END;

  -------------------------------------------------------------
  -- 6. V_OLAP_SUBSCRIPTION_BILLING_TREND
  -- Endpoint: /ords/fdbo/analytics/billing-trend
  -------------------------------------------------------------
  DECLARE
    v_module  VARCHAR2(100) := 'analytics';
    v_pattern VARCHAR2(100) := 'billing-trend';
    v_sql     CLOB :=
      'SELECT billing_month, tier_name, billing_cycle, invoice_count, 
              total_billed_usd, total_discount_usd, total_tax_usd, unique_users_billed,
              cumulative_revenue_usd, prev_month_revenue_usd, same_month_prior_year_usd,
              next_month_revenue_usd, first_month_revenue_usd, last_month_revenue_usd,
              mom_growth_pct
       FROM FDBO.V_OLAP_SUBSCRIPTION_BILLING_TREND
       ORDER BY tier_name, billing_month DESC';
  BEGIN
    ORDS.DEFINE_TEMPLATE(p_module_name => v_module, p_pattern => v_pattern);
    ORDS.DEFINE_HANDLER(
      p_module_name => v_module, p_pattern => v_pattern, p_method => 'GET',
      p_source_type => ORDS.source_type_collection_feed, p_source => v_sql, p_items_per_page => 1000
    );
  END;

  -------------------------------------------------------------
  -- 7. V_OLAP_FUNNEL_CONVERSION_BY_TIER
  -- Endpoint: /ords/fdbo/analytics/funnel-conversion
  -------------------------------------------------------------
  DECLARE
    v_module  VARCHAR2(100) := 'analytics';
    v_pattern VARCHAR2(100) := 'funnel-conversion';
    v_sql     CLOB :=
      'SELECT tier_name, funnel_stage, product_type, event_month,
              total_events, conversion_events, purchase_events, unique_users,
              purchase_rate_pct, conversion_rate_pct, 
              grp_tier, grp_funnel, grp_product_type, grp_month
       FROM FDBO.V_OLAP_FUNNEL_CONVERSION_BY_TIER';
  BEGIN
    ORDS.DEFINE_TEMPLATE(p_module_name => v_module, p_pattern => v_pattern);
    ORDS.DEFINE_HANDLER(
      p_module_name => v_module, p_pattern => v_pattern, p_method => 'GET',
      p_source_type => ORDS.source_type_collection_feed, p_source => v_sql, p_items_per_page => 1000
    );
  END;

  -------------------------------------------------------------
  -- 8. V_OLAP_INVOICE_PAYMENT_BEHAVIOUR
  -- Endpoint: /ords/fdbo/analytics/payment-behaviour
  -------------------------------------------------------------
  DECLARE
    v_module  VARCHAR2(100) := 'analytics';
    v_pattern VARCHAR2(100) := 'payment-behaviour';
    v_sql     CLOB :=
      'SELECT tier_name, billing_cycle, country_code, total_invoices,
              paid_count, overdue_count, discounted_count, payment_rate_pct,
              overdue_rate_pct, avg_invoice_usd, median_invoice_usd, stddev_invoice_usd,
              p25_invoice_usd, p75_invoice_usd, avg_discount_rate_pct, 
              median_days_to_pay, stddev_days_to_pay, discount_payment_correlation,
              invoice_size_payment_speed_corr, grouping_label, 
              grp_tier, grp_cycle, grp_country
       FROM FDBO.V_OLAP_INVOICE_PAYMENT_BEHAVIOUR';
  BEGIN
    ORDS.DEFINE_TEMPLATE(p_module_name => v_module, p_pattern => v_pattern);
    ORDS.DEFINE_HANDLER(
      p_module_name => v_module, p_pattern => v_pattern, p_method => 'GET',
      p_source_type => ORDS.source_type_collection_feed, p_source => v_sql, p_items_per_page => 1000
    );
  END;

  COMMIT;
END;
/
```

### Access Views

```sql
BEGIN
  -------------------------------------------------------------
  -- 1. V_PG_ORDERS / MV_PG_ORDERS
  -- Endpoint: /ords/fdbo/analytics/pg-orders
  -------------------------------------------------------------
  DECLARE
    v_module  VARCHAR2(100) := 'analytics';
    v_pattern VARCHAR2(100) := 'pg-orders';
    v_sql     CLOB :=
      'SELECT id, user_id, invoice_id, status, shipping_country, created_at
       FROM FDBO.MV_PG_ORDERS
       ORDER BY created_at DESC';
  BEGIN
    ORDS.DEFINE_TEMPLATE(p_module_name => v_module, p_pattern => v_pattern);
    ORDS.DEFINE_HANDLER(
      p_module_name => v_module, p_pattern => v_pattern, p_method => 'GET',
      p_source_type => ORDS.source_type_collection_feed, p_source => v_sql, p_items_per_page => 1000
    );
  END;

  -------------------------------------------------------------
  -- 2. V_PG_ORDER_ITEMS / MV_PG_ORDER_ITEMS
  -- Endpoint: /ords/fdbo/analytics/pg-order-items
  -------------------------------------------------------------
  DECLARE
    v_module  VARCHAR2(100) := 'analytics';
    v_pattern VARCHAR2(100) := 'pg-order-items';
    v_sql     CLOB :=
      'SELECT id, order_id, product_id, quantity, unit_price_usd, line_total_usd
       FROM FDBO.MV_PG_ORDER_ITEMS
       ORDER BY id ASC';
  BEGIN
    ORDS.DEFINE_TEMPLATE(p_module_name => v_module, p_pattern => v_pattern);
    ORDS.DEFINE_HANDLER(
      p_module_name => v_module, p_pattern => v_pattern, p_method => 'GET',
      p_source_type => ORDS.source_type_collection_feed, p_source => v_sql, p_items_per_page => 1000
    );
  END;

  -------------------------------------------------------------
  -- 3. V_PG_MKT_INVOICES / MV_PG_MKT_INVOICES
  -- Endpoint: /ords/fdbo/analytics/pg-mkt-invoices
  -------------------------------------------------------------
  DECLARE
    v_module  VARCHAR2(100) := 'analytics';
    v_pattern VARCHAR2(100) := 'pg-mkt-invoices';
    v_sql     CLOB :=
      'SELECT id, user_id, status, total_usd, created_at
       FROM FDBO.MV_PG_MKT_INVOICES
       ORDER BY created_at DESC';
  BEGIN
    ORDS.DEFINE_TEMPLATE(p_module_name => v_module, p_pattern => v_pattern);
    ORDS.DEFINE_HANDLER(
      p_module_name => v_module, p_pattern => v_pattern, p_method => 'GET',
      p_source_type => ORDS.source_type_collection_feed, p_source => v_sql, p_items_per_page => 1000
    );
  END;

  -------------------------------------------------------------
  -- 4. V_TS_EVENTS
  -- Endpoint: /ords/fdbo/analytics/ts-events
  -------------------------------------------------------------
  DECLARE
    v_module  VARCHAR2(100) := 'analytics';
    v_pattern VARCHAR2(100) := 'ts-events';
    v_sql     CLOB :=
      'SELECT id, user_id, event_type, product_id, metadata, occurred_at
       FROM FDBO.V_TS_EVENTS
       ORDER BY occurred_at DESC';
  BEGIN
    ORDS.DEFINE_TEMPLATE(p_module_name => v_module, p_pattern => v_pattern);
    ORDS.DEFINE_HANDLER(
      p_module_name => v_module, p_pattern => v_pattern, p_method => 'GET',
      p_source_type => ORDS.source_type_collection_feed, p_source => v_sql, p_items_per_page => 1000
    );
  END;

  -------------------------------------------------------------
  -- 5. MV_MG_PRODUCTS (Using MV for better ORDS performance)
  -- Endpoint: /ords/fdbo/analytics/mg-products
  -------------------------------------------------------------
  DECLARE
    v_module  VARCHAR2(100) := 'analytics';
    v_pattern VARCHAR2(100) := 'mg-products';
    v_sql     CLOB :=
      'SELECT product_id_b64, product_id, seller_id_b64, seller_id, name, slug,
              product_type, description, price_usd, currency, is_active,
              created_at, updated_at
       FROM FDBO.MV_MG_PRODUCTS
       ORDER BY created_at DESC';
  BEGIN
    ORDS.DEFINE_TEMPLATE(p_module_name => v_module, p_pattern => v_pattern);
    ORDS.DEFINE_HANDLER(
      p_module_name => v_module, p_pattern => v_pattern, p_method => 'GET',
      p_source_type => ORDS.source_type_collection_feed, p_source => v_sql, p_items_per_page => 1000
    );
  END;

  -------------------------------------------------------------
-- 6. V_NEO4J_BOUGHT_WITH
-- Endpoint: /ords/fdbo/analytics/neo4j-bought-with
-------------------------------------------------------------
 DECLARE
    v_module  VARCHAR2(100) := 'analytics';
    v_pattern VARCHAR2(100) := 'neo4j-bought-with';
    v_sql     CLOB :=
      'SELECT product_1_id, product_1_name, product_2_id, product_2_name, co_purchase_count
       FROM FDBO.V_NEO4J_BOUGHT_WITH
       ORDER BY co_purchase_count DESC';
  BEGIN
    ORDS.DEFINE_TEMPLATE(p_module_name => v_module, p_pattern => v_pattern);
    ORDS.DEFINE_HANDLER(
      p_module_name => v_module, p_pattern => v_pattern, p_method => 'GET',
      p_source_type => ORDS.source_type_collection_feed, p_source => v_sql, p_items_per_page => 1000
    );
  END;

  COMMIT;
END;
/
```