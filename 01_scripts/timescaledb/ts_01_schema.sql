-- ============================================================
--  IIS Project — TimescaleDB (DS_3)
--  Script 01: Create events hypertable
--
--  Run as: iis_user, connected to iis_events
--  How to run:
--    Get-Content timescaledb\scripts\ts_01_schema.sql | docker exec -i iis-timescaledb psql -U iis_user -d iis_events
-- ============================================================

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ── events ────────────────────────────────────────────────────
CREATE TABLE events (
    id           UUID        NOT NULL,
    user_id      UUID        NOT NULL,
    event_type   TEXT NOT NULL,
    product_id   UUID,
    session_id   TEXT,
    metadata     JSONB       NOT NULL DEFAULT '{}',
    occurred_at  TIMESTAMPTZ NOT NULL
);

-- Convert to hypertable partitioned on occurred_at.
-- TimescaleDB automatically manages time-based chunks
-- for fast range queries like:
--   WHERE occurred_at > now() - INTERVAL '7 days'
SELECT create_hypertable('events', 'occurred_at');

-- Indexes — same as original schema
CREATE INDEX idx_events_occurred_at  ON events (occurred_at DESC);
CREATE INDEX idx_events_user_time    ON events (user_id, occurred_at DESC);
CREATE INDEX idx_events_type         ON events (event_type);
CREATE INDEX idx_events_product_id   ON events (product_id);

-- ── Verify ────────────────────────────────────────────────────
SELECT hypertable_name, num_chunks
FROM timescaledb_information.hypertables;
