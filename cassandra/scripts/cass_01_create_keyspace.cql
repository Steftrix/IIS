-- ============================================================
--  IIS Project — Cassandra 4 (DS_6)
--  Script 01: Create keyspace and sessions table
--
--  Run via:
--    Get-Content cassandra\scripts\cass_01_create_keyspace.cql | docker exec -i iis-cassandra cqlsh -u cassandra -p cassandra
-- ============================================================

-- ── Keyspace ──────────────────────────────────────────────────
-- SimpleStrategy with replication_factor 1 is correct for a
-- single-node development cluster (which is what we have).
CREATE KEYSPACE IF NOT EXISTS iis
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    AND durable_writes = true;

USE iis;

-- ── sessions table ────────────────────────────────────────────
-- Partition key: id (session UUID string)
-- Lookup is always by session id — Cassandra is optimised
-- for exactly this pattern.
--
-- TTL is set per-row at INSERT time based on expires_at.
-- Cassandra automatically tombstones expired rows — no
-- cleanup job needed.
--
-- cart is stored as TEXT (JSON string) — Cassandra has no
-- native JSON array type, and we don't need to query into it.
CREATE TABLE IF NOT EXISTS iis.sessions (
    id              TEXT        PRIMARY KEY,
    user_id         UUID,
    cart            TEXT,
    ip_address      TEXT,
    user_agent      TEXT,
    created_at      TIMESTAMP,
    last_active_at  TIMESTAMP,
    expires_at      TIMESTAMP
);

-- ── Verify ────────────────────────────────────────────────────
DESCRIBE TABLE iis.sessions;
