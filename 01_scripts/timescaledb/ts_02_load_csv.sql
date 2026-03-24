-- ============================================================
--  IIS Project — TimescaleDB (DS_3)
--  Script 02: Load events.csv into hypertable
--
--  Run as: iis_user, connected to iis_events
--  How to run:
--    docker exec -i iis-timescaledb psql -U iis_user -d iis_events < timescaledb/scripts/ts_02_load_csv.sql
-- ============================================================

COPY events (
    id, user_id, event_type, product_id,
    session_id, metadata, occurred_at
)
FROM '/csv/events.csv'
WITH (FORMAT csv, HEADER true, NULL '');

-- ── Verify ────────────────────────────────────────────────────
SELECT COUNT(*)                             AS total_events    FROM events;
SELECT COUNT(DISTINCT event_type)           AS distinct_types  FROM events;
SELECT MIN(occurred_at), MAX(occurred_at)   AS time_range      FROM events;
SELECT show_chunks('events');
