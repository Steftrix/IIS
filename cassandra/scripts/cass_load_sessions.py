#!/usr/bin/env python3
"""
IIS Project — Cassandra (DS_6)
load_sessions.py: Loads sessions.csv into Cassandra iis.sessions

Why Python instead of COPY FROM:
  - COPY FROM doesn't support per-row TTL
  - We calculate TTL from expires_at - now() so expired sessions
    are automatically deleted by Cassandra (tombstoned)
  - Also handles the cart field which contains JSON with commas
    that confuse plain COPY FROM

Run inside the container:
  docker exec iis-cassandra mkdir -p /csv
  docker cp data\csv\sessions.csv iis-cassandra:/csv/sessions.csv
  docker cp cassandra\scripts\cass_load_sessions.py iis-cassandra:/tmp/load_sessions.py
  docker exec iis-cassandra python3 /tmp/load_sessions.py
"""

import csv
import subprocess
from datetime import datetime, timezone

CSV_PATH = "/csv/sessions.csv"
KEYSPACE = "iis"
TABLE    = "sessions"

TS_FORMATS = [
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f%z"
]

def parse_ts(val):
    if not val or val.strip() == "":
        return None
    for fmt in TS_FORMATS:
        try:
            return datetime.strptime(val.strip(), fmt)
        except ValueError:
            continue
    return None

def calc_ttl(expires_at_dt):
    if expires_at_dt is None:
        return 86400
    now = datetime.now(timezone.utc)
    ttl = int((expires_at_dt - now).total_seconds())
    return max(ttl, 1)

def escape_str(val):
    """Escape a string value for CQL — wrapped in single quotes."""
    if val is None or str(val).strip() == "":
        return "null"
    return "'" + str(val).replace("'", "''") + "'"

def format_uuid(val):
    """UUIDs in CQL are bare — no quotes."""
    if val is None or str(val).strip() == "":
        return "null"
    return str(val).strip()

def ts_to_cql(dt):
    if dt is None:
        return "null"
    return "'" + dt.strftime("%Y-%m-%dT%H:%M:%S.000+0000") + "'"

def run_cql(cql):
    result = subprocess.run(
        ["cqlsh", "-u", "cassandra", "-p", "cassandra", "--execute", cql],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        err = result.stderr.strip()
        # Ignore the password warning, only fail on real errors
        if "InvalidRequest" in err or "SyntaxException" in err:
            print(f"  CQL ERROR: {err}")
            return False
    return True

print(f"Loading {CSV_PATH} into Cassandra {KEYSPACE}.{TABLE}...")

loaded = 0
skipped = 0

with open(CSV_PATH, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        expires_at = parse_ts(row.get('expires_at', ''))
        ttl = calc_ttl(expires_at)

        if ttl < -2592000:
            skipped += 1
            continue

        cql = (
            f"INSERT INTO {KEYSPACE}.{TABLE} "
            f"(id, user_id, cart, ip_address, user_agent, created_at, last_active_at, expires_at) "
            f"VALUES ("
            f"{escape_str(row['id'])}, "
            f"{format_uuid(row['user_id'])}, "
            f"{escape_str(row['cart'])}, "
            f"{escape_str(row['ip_address'])}, "
            f"{escape_str(row['user_agent'])}, "
            f"{ts_to_cql(parse_ts(row['created_at']))}, "
            f"{ts_to_cql(parse_ts(row['last_active_at']))}, "
            f"{ts_to_cql(expires_at)}"
            f") USING TTL {max(ttl, 1)};"
        )

        if run_cql(cql):
            loaded += 1
            if loaded % 100 == 0:
                print(f"  Loaded {loaded} rows...")
        else:
            skipped += 1

print(f"\nDone. Loaded: {loaded}  Skipped: {skipped}")

result = subprocess.run(
    ["cqlsh", "-u", "cassandra", "-p", "cassandra",
     "--execute", f"SELECT COUNT(*) FROM {KEYSPACE}.{TABLE};"],
    capture_output=True, text=True
)
print("\nRow count in Cassandra:")
print(result.stdout)
