# IIS — Integrated Information System

A multi-database integration project that federates five heterogeneous data stores into a single Oracle-hosted analytical layer, exposing analytics data through REST APIs and OLAP views.

## Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                        Oracle XE 21c (FDBO)                        │
│            Central Integration & Analytics Hub (ORDS)              │
│   UTL_HTTP federates data from all REST APIs into Oracle views     │
└───────────┬────────────┬────────────┬────────────┬─────────────────┘
            │            │            │            │
     PostgREST       PostgREST    RestHeart    Neo4j HTTP
      :3000           :3001         :8081        :7474
            │            │            │            │
     ┌──────┴──┐  ┌──────┴────┐  ┌───┴───┐  ┌────┴───┐
     │Postgres │  │TimescaleDB│  │MongoDB│  │ Neo4j  │
     │  14     │  │(pg14)     │  │   6   │  │   5    │
     │ :5432   │  │  :5433    │  │:27017 │  │ :7687  │
     └─────────┘  └───────────┘  └───────┘  └────────┘
```

| Data Store | Role | Port | REST Interface |
|---|---|---|---|
| Oracle XE 21c | Central hub, OLAP analytics, integration views | 1521 | ORDS `:8181` |
| PostgreSQL 14 | Orders, marketplace invoices | 5432 | PostgREST `:3000` |
| TimescaleDB | User events (time-series) | 5433 | PostgREST `:3001` |
| MongoDB 6 | Product catalogue | 27017 | RestHeart `:8081` |
| Neo4j 5 | Product co-purchase graph (`BOUGHT_WITH`) | 7687 | HTTP `:7474` |

## Repository Structure

```
IIS/
├── 00_data/csv/              # CSV seed data (add CSV files here)
├── 01_scripts/
│   ├── oracle/               # Oracle user setup & CSV import
│   ├── postgres/             # PostgreSQL schema & data load
│   ├── timescaledb/          # TimescaleDB schema & data load
│   ├── mongodb/              # MongoDB product seeding
│   └── neo4j/                # Neo4j Cypher — BOUGHT_WITH graph
├── 02_integration/
│   ├── Connection_Prerequisites.sql  # Oracle ACL grants (run as SYS)
│   ├── 03_fact_views.sql             # ROLAP fact views
│   └── 04_dimension_views.sql        # Dimension views
├── docker-compose.yml        # Full stack orchestration
├── IIS.postman_collection.json  # Postman collection for all REST endpoints
├── ords-readme.md            # ORDS install & endpoint setup guide
└── screenshots/              # Dashboard screenshots
```

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) & Docker Compose
- [Postman](https://www.postman.com/downloads/) (optional, for API testing)
- Java 11+ (for ORDS, if running outside Docker)

## Quick Start

### 1. Start all services

```bash
docker compose up -d
```

Wait for health checks to pass (~3 minutes for Oracle to initialise).

### 2. Load data — PostgreSQL (DS_2)

```bash
# Schema
Get-Content 01_scripts/postgres/01_schema.sql | docker exec -i iis-postgres psql -U iis_user -d iis_db

# Data
Get-Content 01_scripts/postgres/02_load_csv.sql | docker exec -i iis-postgres psql -U iis_user -d iis_db
```

### 3. Load data — TimescaleDB (DS_3)

```bash
Get-Content 01_scripts/timescaledb/ts_01_schema.sql | docker exec -i iis-timescaledb psql -U iis_user -d iis_events
Get-Content 01_scripts/timescaledb/ts_02_load_csv.sql | docker exec -i iis-timescaledb psql -U iis_user -d iis_events
```

### 4. Load data — MongoDB (DS_4)

```bash
docker exec -i iis-mongodb mongosh -u iis_admin -p iis_pass --authenticationDatabase admin iis_db < 01_scripts/mongodb/mongo_01_products.js
```

### 5. Load data — Neo4j (DS_5)

Copy `products.csv` and `order_items.csv` into `neo4j/import/`, then run the Cypher steps described in [`01_scripts/neo4j/incarcareDateNEO4J.md`](01_scripts/neo4j/incarcareDateNEO4J.md) via the Neo4j Browser at [http://localhost:7474](http://localhost:7474).

### 6. Set up Oracle (DS_1)

Follow the steps in [`01_scripts/oracle/`](01_scripts/oracle/) to create the `FDBO` user and import the backup.

### 7. Configure Oracle integration layer

```sql
-- Run as SYS
@02_integration/Connection_Prerequisites.sql
-- Run as FDBO
@02_integration/03_fact_views.sql
@02_integration/04_dimension_views.sql
```

### 8. Set up ORDS REST endpoints

Follow [`ords-readme.md`](ords-readme.md) to install ORDS and publish the analytics module endpoints.

## REST API Endpoints

All analytics endpoints are exposed through ORDS at `http://localhost:8181/ords/fdbo/analytics/`.

| Endpoint | Description |
|---|---|
| `GET /sub-cohort` | Subscription cohort retention analysis |
| `GET /engagement-revenue` | Monthly engagement & revenue by country |
| `GET /geo-density` | Geographic market density with activity bands |
| `GET /product-affinity` | Product affinity co-purchase analytics |
| `GET /sub-retention` | Subscription retention by tier & cohort |
| `GET /billing-trend` | Subscription billing trend with MoM growth |
| `GET /funnel-conversion` | Funnel conversion by tier, stage & product type |
| `GET /payment-behaviour` | Invoice payment behaviour statistics |
| `GET /pg-orders` | Marketplace orders (from PostgreSQL) |
| `GET /pg-order-items` | Order line items (from PostgreSQL) |
| `GET /pg-mkt-invoices` | Marketplace invoices (from PostgreSQL) |
| `GET /ts-events` | User events time-series (from TimescaleDB) |
| `GET /mg-products` | Product catalogue (from MongoDB) |
| `GET /neo4j-bought-with` | Co-purchased product pairs (from Neo4j) |

Import `IIS.postman_collection.json` into Postman to test all endpoints.

## Analytics Screenshots

| Dashboard | Preview |
|---|---|
| Subscription Cohort | ![sub-cohort](screenshots/sub-cohort.png) |
| Subscription Retention | ![sub-retention](screenshots/sub-retention.png) |
| Billing Trend | ![billing-trend](screenshots/billing-trend.png) |
| Engagement & Revenue | ![engagement-revenue](screenshots/engagement-revenue.png) |
| Funnel Conversion | ![funnel-conversion](screenshots/funnel-conversion.png) |
| Payment Behaviour | ![payment-behaviour](screenshots/payment-behaviour.png) |
| Geographic Density | ![geo-density](screenshots/geo-density.png) |
| Product Affinity | ![product-affinity](screenshots/product-affinity.png) |
| Neo4j Bought-With | ![neo4j-bought-with](screenshots/neo4j-bought-with.png) |
| PG Orders | ![pg-orders](screenshots/pg-orders.png) |
| PG Order Items | ![pg-orders-items](screenshots/pg-orders-items.png) |
| PG Market Invoices | ![pg-mkt-invoices](screenshots/pg-mkt-invoices.png) |
| TimescaleDB Events | ![ts-events](screenshots/ts-events.png) |
| MongoDB Products | ![mg-products](screenshots/mg-products.png) |

## Service Credentials

| Service | Username | Password | Database |
|---|---|---|---|
| Oracle | SYS / FDBO | `Oracle21c` / `fdbo_pass` | XEPDB1 |
| PostgreSQL | iis_user | iis_pass | iis_db |
| TimescaleDB | iis_user | iis_pass | iis_events |
| MongoDB | iis_admin | iis_pass | iis_db |
| Neo4j | neo4j | neo4j_admin | — |
