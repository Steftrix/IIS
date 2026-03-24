-- ============================================================
--  IIS Project — PostgreSQL 14  (DS_2)
--  Script 01: Create schema for orders & commerce domain
--
--  Run as: iis_user, connected to iis_db
--  How to run:
--    Get-Content postgres\scripts\01_schema.sql | docker exec -i iis-postgres psql -U iis_user -d iis_db
--
--  Tables:
--    marketplace_invoices       (renamed from 'invoices' in original schema)
--    marketplace_invoice_lines  (renamed from 'invoice_lines')
--    orders
--    order_items
-- ============================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── marketplace_invoices ──────────────────────────────────────
CREATE TABLE marketplace_invoices (
    id               UUID DEFAULT uuid_generate_v4() NOT NULL PRIMARY KEY,
    user_id          UUID NOT NULL,
    invoice_type     VARCHAR(20)   NOT NULL,
    status           VARCHAR(20)   NOT NULL DEFAULT 'pending',
    subtotal_usd     NUMERIC(10,2) NOT NULL,
    tax_usd          NUMERIC(10,2) NOT NULL DEFAULT 0.00,
    discount_usd     NUMERIC(10,2) NOT NULL DEFAULT 0.00,
    total_usd        NUMERIC(10,2) NOT NULL,
    subscription_id  UUID,
    billing_period_start TIMESTAMPTZ,
    billing_period_end   TIMESTAMPTZ,
    paid_at          TIMESTAMPTZ,
    due_at           TIMESTAMPTZ NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_mkt_invoices_user_id    ON marketplace_invoices (user_id);
CREATE INDEX idx_mkt_invoices_status     ON marketplace_invoices (status);
CREATE INDEX idx_mkt_invoices_created_at ON marketplace_invoices (created_at);


-- ── marketplace_invoice_lines ─────────────────────────────────
CREATE TABLE marketplace_invoice_lines (
    id             UUID DEFAULT uuid_generate_v4() NOT NULL PRIMARY KEY,
    invoice_id     UUID          NOT NULL,
    product_id     UUID,
    description    VARCHAR(500)  NOT NULL,
    quantity       SMALLINT      NOT NULL DEFAULT 1,
    unit_price_usd NUMERIC(8,2)  NOT NULL,
    line_total_usd NUMERIC(10,2) NOT NULL,
    created_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT fk_mil_invoice FOREIGN KEY (invoice_id)
        REFERENCES marketplace_invoices(id) ON DELETE CASCADE
);

CREATE INDEX idx_mil_invoice_id  ON marketplace_invoice_lines (invoice_id);
CREATE INDEX idx_mil_product_id  ON marketplace_invoice_lines (product_id);


-- ── orders ────────────────────────────────────────────────────
CREATE TABLE orders (
    id               UUID DEFAULT uuid_generate_v4() NOT NULL PRIMARY KEY,
    user_id          UUID          NOT NULL,
    invoice_id       UUID          NOT NULL,
    status           VARCHAR(20)   NOT NULL DEFAULT 'pending',
    shipping_name    VARCHAR(255),
    shipping_address VARCHAR(500),
    shipping_city    VARCHAR(100),
    shipping_country CHAR(2),
    shipping_postal  VARCHAR(20),
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT fk_orders_invoice FOREIGN KEY (invoice_id)
        REFERENCES marketplace_invoices(id)
);

CREATE INDEX idx_orders_user_id    ON orders (user_id);
CREATE INDEX idx_orders_invoice_id ON orders (invoice_id);
CREATE INDEX idx_orders_status     ON orders (status);
CREATE INDEX idx_orders_created_at ON orders (created_at);


-- ── order_items ───────────────────────────────────────────────
CREATE TABLE order_items (
    id               UUID DEFAULT uuid_generate_v4() NOT NULL PRIMARY KEY,
    order_id         UUID          NOT NULL,
    product_id       UUID          NOT NULL,
    quantity         SMALLINT      NOT NULL DEFAULT 1,
    unit_price_usd   NUMERIC(8,2)  NOT NULL,
    line_total_usd   NUMERIC(10,2) NOT NULL,
    fulfilment_status VARCHAR(20)  NOT NULL DEFAULT 'pending',
    created_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT fk_oi_order FOREIGN KEY (order_id)
        REFERENCES orders(id) ON DELETE CASCADE
);

CREATE INDEX idx_oi_order_id   ON order_items (order_id);
CREATE INDEX idx_oi_product_id ON order_items (product_id);


-- ── Verify ────────────────────────────────────────────────────
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;
