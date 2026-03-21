-- ============================================================
--  In SQL modeler:
--  Hostname: localhost
--  Service name: XEPDB1
--  Username: sys
--  SYSDBA role from dropdown
--  Password: Oracle21c
-- ============================================================

-- ── Create the FDBO user ─────────────────────────────────────
-- FDBO = Federated Database Owner
-- This user owns all project tables, views, and external tables.
-- All team members connect as FDBO for day-to-day work.
CREATE USER FDBO IDENTIFIED BY fdbo_pass
    DEFAULT TABLESPACE USERS
    TEMPORARY TABLESPACE TEMP
    QUOTA UNLIMITED ON USERS;

-- ── Grant permissions ────────────────────────────────────────
GRANT CONNECT TO FDBO;
GRANT RESOURCE TO FDBO;
GRANT CREATE SESSION TO FDBO;
GRANT CREATE TABLE TO FDBO;
GRANT CREATE VIEW TO FDBO;
GRANT CREATE SEQUENCE TO FDBO;
GRANT CREATE PROCEDURE TO FDBO;
GRANT CREATE SYNONYM TO FDBO;

-- UTL_HTTP is needed for Oracle to call PostgREST, RestHeart,
-- and Stargate (L2 and L3 federation views)
GRANT EXECUTE ON SYS.UTL_HTTP TO FDBO;

-- Allow UTL_HTTP to reach external services (required in 21c)
BEGIN
    DBMS_NETWORK_ACL_ADMIN.APPEND_HOST_ACE(
        host => '*',
        ace  => xs$ace_type(
            privilege_list => xs$name_list('connect', 'resolve'),
            principal_name => 'FDBO',
            principal_type => xs_acl.ptype_db
        )
    );
END;
/

-- ── Directory object for CSV files (DS_5 external table) ─────
-- Points to the folder mounted at /opt/oracle/csv in docker-compose.yml
-- Used by the seller_profiles.csv external table and SQL*Loader
CREATE OR REPLACE DIRECTORY EXT_FILE_DS AS '/opt/oracle/csv';
GRANT READ, WRITE ON DIRECTORY EXT_FILE_DS TO FDBO;

-- ── Verify ───────────────────────────────────────────────────
SELECT username, account_status, default_tablespace
FROM dba_users
WHERE username = 'FDBO';
