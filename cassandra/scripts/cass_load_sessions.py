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
  docker cp cassandra/scripts/load_sessions.py iis-cassandra:/tmp/load_sessions.py
  docker exec iis-cassandra python3 /tmp/load_sessions.py
"""

import csv
import subprocess
from datetime import datetime, timezone

CSV_PATH = "/csv/sessions.csv"
KEYSPACE = "iis"
TABLE    = "sessions"

# Timestamp formats found in the CSV
TS_FORMATS = [
    "%Y-%m-%dT%H:%M:%S%z",   # 2025-10-11T10:28:01+00:00
    "%Y-%m-%dT%H:%M:%S.%f%z" # with microseconds
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
    """Return seconds until expiry, minimum 1. Negative = already expired."""
    if expires_at_dt is None:
        return 86400  # default 1 day if missing
    now = datetime.now(timezone.utc)
    ttl = int((expires_at_dt - now).total_seconds())
    return max(ttl, 1)  # Cassandra TTL must be >= 1

def escape_cql(val):
    if val is None:
        return "null"
    return "'" + str(val).replace("'", "''") + "'"

def ts_to_cql(dt):
    if dt is None:
        return "null"
    return "'" + dt.strftime("%Y-%m-%dT%H:%M:%S.000+0000") + "'"

def run_cql(cql):
    result = subprocess.run(
        ["cqlsh", "-u", "cassandra", "-p", "cassandra",
         "--execute", cql],
        capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"  CQL ERROR: {result.stderr.strip()}")
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

        # Skip rows that expired more than 30 days ago
        if ttl < -2592000:
            skipped += 1
            continue

        cql = f"""
INSERT INTO {KEYSPACE}.{TABLE}
    (id, user_id, cart, ip_address, user_agent,
     created_at, last_active_at, expires_at)
VALUES (
    {escape_cql(row['id'])},
    {escape_cql(row['user_id']) if row['user_id'] else 'null'},
    {escape_cql(row['cart'])},
    {escape_cql(row['ip_address'])},
    {escape_cql(row['user_agent'])},
    {ts_to_cql(parse_ts(row['created_at']))},
    {ts_to_cql(parse_ts(row['last_active_at']))},
    {ts_to_cql(expires_at)}
) USING TTL {max(ttl, 1)};
""".strip()

        if run_cql(cql):
            loaded += 1
            if loaded % 100 == 0:
                print(f"  Loaded {loaded} rows...")
        else:
            skipped += 1

print(f"\nDone. Loaded: {loaded}  Skipped: {skipped}")

# Verify
result = subprocess.run(
    ["cqlsh", "-u", "cassandra", "-p", "cassandra",
     "--execute", f"SELECT COUNT(*) FROM {KEYSPACE}.{TABLE};"],
    capture_output=True, text=True
)
print("\nRow count in Cassandra:")
print(result.stdout)
