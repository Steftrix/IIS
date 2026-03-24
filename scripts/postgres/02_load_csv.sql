-- ============================================================
--  IIS Project — PostgreSQL 14  (DS_2)
--  Script 02: Load CSV data
--
--  Run as: iis_user, connected to iis_db
--  How to run:
--    Get-Content postgres/scripts/02_load_csv.sql | docker exec -i iis-postgres psql -U iis_user -d iis_db
--
--  Load order: invoices first (orders FK references them),
--  orders second, order_items last (FKs orders)
-- ============================================================

-- ── marketplace_invoices ──────────────────────────────────────
COPY marketplace_invoices (
    id, user_id, invoice_type, status,
    subtotal_usd, tax_usd, discount_usd, total_usd,
    subscription_id, billing_period_start, billing_period_end,
    paid_at, due_at, created_at
)
FROM '/csv/marketplace_invoices.csv'
WITH (FORMAT csv, HEADER true, NULL '');


-- ── marketplace_invoice_lines ─────────────────────────────────
COPY marketplace_invoice_lines (
    id, invoice_id, product_id, description,
    quantity, unit_price_usd, line_total_usd, created_at
)
FROM '/csv/marketplace_invoice_lines.csv'
WITH (FORMAT csv, HEADER true, NULL '');


-- ── orders ────────────────────────────────────────────────────
COPY orders (
    id, user_id, invoice_id, status,
    shipping_name, shipping_address, shipping_city,
    shipping_country, shipping_postal, created_at, updated_at
)
FROM '/csv/orders.csv'
WITH (FORMAT csv, HEADER true, NULL '');


-- ── order_items ───────────────────────────────────────────────
COPY order_items (
    id, order_id, product_id, quantity,
    unit_price_usd, line_total_usd, fulfilment_status, created_at
)
FROM '/csv/order_items.csv'
WITH (FORMAT csv, HEADER true, NULL '');


-- ── Verify row counts ─────────────────────────────────────────
SELECT 'marketplace_invoices'      AS table_name, COUNT(*) AS rows FROM marketplace_invoices
UNION ALL
SELECT 'marketplace_invoice_lines',               COUNT(*) FROM marketplace_invoice_lines
UNION ALL
SELECT 'orders',                                  COUNT(*) FROM orders
UNION ALL
SELECT 'order_items',                             COUNT(*) FROM order_items
ORDER BY table_name;
