-- ============================================================
--  IIS Project — Oracle XE 21c
--  Script 02: Create all tables in FDBO schema
--
--  Run as: FDBO user, connected to XEPDB1
--  Connection: localhost:1521/XEPDB1  user: FDBO  pass: fdbo_pass
--
--  Tables created:
--    1. SUBSCRIPTION_TIERS          (lookup, seeded by script 03)
--    2. SUBSCRIPTION_TIER_PRICING   (lookup, seeded by script 03)
--    3. USERS
--    4. SUBSCRIPTIONS
--    5. SUBSCRIPTION_INVOICES
--    6. SUBSCRIPTION_INVOICE_LINES
-- ============================================================


-- ── 1. SUBSCRIPTION_TIERS ────────────────────────────────────
-- Lookup table for Free / Pro / Business tiers.
-- Seeded with hardcoded INSERTs in script 03.
CREATE TABLE SUBSCRIPTION_TIERS (
    id          NUMBER          PRIMARY KEY,
    name        VARCHAR2(50)    NOT NULL,
    description VARCHAR2(500),
    features    CLOB            -- JSON blob: seats, api_access, apps etc.
);


-- ── 2. SUBSCRIPTION_TIER_PRICING ─────────────────────────────
-- Price history per tier. Pro and Business had a price increase
-- in June 2024 — old and new prices are both kept here.
CREATE TABLE SUBSCRIPTION_TIER_PRICING (
    id                  NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tier_id             NUMBER          NOT NULL,
    valid_from          TIMESTAMP WITH TIME ZONE    NOT NULL,
    valid_to            TIMESTAMP WITH TIME ZONE,   -- NULL means currently active
    monthly_price_usd   NUMBER(8,2)     NOT NULL,
    is_active           NUMBER(1)       DEFAULT 0   NOT NULL,   -- 1=true, 0=false
    CONSTRAINT fk_stp_tier FOREIGN KEY (tier_id)
        REFERENCES SUBSCRIPTION_TIERS(id)
);


-- ── 3. USERS ─────────────────────────────────────────────────
-- Source: users.csv
-- Columns: id, email, full_name, country_code, city,
--          created_at, last_login_at, is_active, preferences
CREATE TABLE USERS (
    id              VARCHAR2(36)    PRIMARY KEY,    -- UUID
    email           VARCHAR2(255)   NOT NULL,
    full_name       VARCHAR2(200),
    country_code    VARCHAR2(5),
    city            VARCHAR2(100),
    created_at      TIMESTAMP WITH TIME ZONE,
    last_login_at   TIMESTAMP WITH TIME ZONE,
    is_active       VARCHAR2(5),                   -- 'True' / 'False' from CSV
    preferences     CLOB                           -- JSON blob: theme, language, notifications etc.
);


-- ── 4. SUBSCRIPTIONS ─────────────────────────────────────────
-- Source: subscriptions.csv
-- Columns: id, user_id, tier_id, status, started_at,
--          current_period_start, current_period_end,
--          cancelled_at, cancel_reason, billing_cycle,
--          created_at, updated_at
CREATE TABLE SUBSCRIPTIONS (
    id                      VARCHAR2(36)    PRIMARY KEY,
    user_id                 VARCHAR2(36)    NOT NULL,
    tier_id                 NUMBER          NOT NULL,
    status                  VARCHAR2(20),           -- active, cancelled, expired
    started_at              TIMESTAMP WITH TIME ZONE,
    current_period_start    TIMESTAMP WITH TIME ZONE,
    current_period_end      TIMESTAMP WITH TIME ZONE,
    cancelled_at            TIMESTAMP WITH TIME ZONE,
    cancel_reason           VARCHAR2(500),
    billing_cycle           VARCHAR2(10),           -- monthly, annual
    created_at              TIMESTAMP WITH TIME ZONE,
    updated_at              TIMESTAMP WITH TIME ZONE,
    CONSTRAINT fk_sub_user FOREIGN KEY (user_id)
        REFERENCES USERS(id),
    CONSTRAINT fk_sub_tier FOREIGN KEY (tier_id)
        REFERENCES SUBSCRIPTION_TIERS(id)
);


-- ── 5. SUBSCRIPTION_INVOICES ─────────────────────────────────
-- Source: subscription_invoices.csv
-- Columns: id, user_id, invoice_type, status,
--          subtotal_usd, tax_usd, discount_usd, total_usd,
--          subscription_id, billing_period_start,
--          billing_period_end, paid_at, due_at, created_at
CREATE TABLE SUBSCRIPTION_INVOICES (
    id                      VARCHAR2(36)    PRIMARY KEY,
    user_id                 VARCHAR2(36)    NOT NULL,
    invoice_type            VARCHAR2(20),           -- 'subscription'
    status                  VARCHAR2(20),           -- paid, pending, overdue
    subtotal_usd            NUMBER(10,2),
    tax_usd                 NUMBER(10,2),
    discount_usd            NUMBER(10,2),
    total_usd               NUMBER(10,2),
    subscription_id         VARCHAR2(36),
    billing_period_start    TIMESTAMP WITH TIME ZONE,
    billing_period_end      TIMESTAMP WITH TIME ZONE,
    paid_at                 TIMESTAMP WITH TIME ZONE,
    due_at                  TIMESTAMP WITH TIME ZONE,
    created_at              TIMESTAMP WITH TIME ZONE,
    CONSTRAINT fk_si_user FOREIGN KEY (user_id)
        REFERENCES USERS(id),
    CONSTRAINT fk_si_sub FOREIGN KEY (subscription_id)
        REFERENCES SUBSCRIPTIONS(id)
);


-- ── 6. SUBSCRIPTION_INVOICE_LINES ────────────────────────────
-- Source: subscription_invoice_lines.csv
-- Columns: id, invoice_id, product_id, description,
--          quantity, unit_price_usd, line_total_usd, created_at
CREATE TABLE SUBSCRIPTION_INVOICE_LINES (
    id              VARCHAR2(36)    PRIMARY KEY,
    invoice_id      VARCHAR2(36)    NOT NULL,
    product_id      VARCHAR2(36),                  -- nullable (subscription lines have no product)
    description     VARCHAR2(500),
    quantity        NUMBER(10),
    unit_price_usd  NUMBER(10,2),
    line_total_usd  NUMBER(10,2),
    created_at      TIMESTAMP WITH TIME ZONE,
    CONSTRAINT fk_sil_invoice FOREIGN KEY (invoice_id)
        REFERENCES SUBSCRIPTION_INVOICES(id)
);


-- ── Verify all tables were created ───────────────────────────
SELECT table_name
FROM user_tables
ORDER BY table_name;\

-- ============================================================
--  IIS Project — Oracle XE 21c
--  Script 02b: Alter is_active to NUMBER(1) before CSV load
--
--  Run as: FDBO, connected to XEPDB1
--  Run this BEFORE the SQL*Loader control files.
--
--  Oracle has no native BOOLEAN column type (before 23c).
--  We use NUMBER(1) with a CHECK constraint: 1 = true, 0 = false.
--  The SQL*Loader control file will DECODE 'True'→1, 'False'→0.
-- ============================================================

-- Table is empty at this point so MODIFY is safe
ALTER TABLE USERS MODIFY (is_active NUMBER(1));

-- Add a check constraint to enforce only 0 or 1
ALTER TABLE USERS ADD CONSTRAINT chk_users_is_active
    CHECK (is_active IN (0, 1));

-- Verify
SELECT column_name, data_type, data_length
FROM user_tab_columns
WHERE table_name = 'USERS'
AND column_name = 'IS_ACTIVE';
