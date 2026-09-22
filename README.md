# WMS Real-Time Inventory on Confluent

Real-time inventory visibility and automated reorder alerting for a
multi-site warehouse operation, built on Confluent Cloud.

The existing WMS is a Laravel application on PostgreSQL serving 15
locations across Indonesia — main warehouses (Jakarta, Balikpapan,
Kendari, Sofifi, Angsana), service points (Muara Teweh, Qinfa Mangalau)
and eight VHS sites. Stock levels were reconciled from per-location
spreadsheets, so a stockout was typically discovered a day late — after
the order had already been promised to a customer.

This project streams the WMS database into Confluent with change data
capture, computes on-hand stock per SKU per site continuously in Flink,
and pushes a graded reorder alert to the purchasing team the moment a SKU
crosses its reorder point. **No application code changes** — the WMS keeps
writing to Postgres exactly as before.

## Architecture

```
 Laravel WMS                Confluent Cloud
 (PostgreSQL)     ┌──────────────────────────────────────────────┐
      │           │                                              │
      │  CDC      │  wms.public.stock_movements  ─┐               │
      ├──────────►│  wms.public.products         ─┤              │
      │ Postgres  │  wms.public.locations        ─┼── Flink SQL   │
      │ CDC V2    │  wms.public.warehouses       ─┘      │        │
      │ (Debezium)│                                      ▼        │
      │           │                        wms.inventory.on_hand  │
      │           │                                      │        │
      │           │                                      ▼        │
      │           │                        wms.alerts.low_stock   │
      │           │                                      │        │
      └───────────┤                            HTTP Sink V2       │
                  └──────────────────────────────────────┼────────┘
                                                         ▼
                                            Purchasing notification API
```

All four source topics and both derived topics carry Avro schemas
registered in Stream Governance, so every field is documented and schema
changes are checked for compatibility before they can break a consumer.

## Confluent components used

| Component | What it does here |
|---|---|
| **PostgreSQL CDC Source V2 (Debezium)** | Captures every INSERT/UPDATE/DELETE on the WMS tables, plus an initial snapshot |
| **HTTP Sink V2** | Delivers low-stock alerts to the WMS notification API |
| **Confluent Cloud for Apache Flink** | Running on-hand stock, graded low-stock alerts, hourly outbound velocity, opname discrepancy watch |
| **Stream Governance / Schema Registry** | Avro schemas with field-level documentation; compatibility enforcement |
| **Stream Lineage** | End-to-end provenance from Postgres table to alert endpoint |

## Repo layout

```
schemas/      Avro schemas (source CDC payloads + derived alert stream)
connectors/   Fully-managed connector configs (CDC source, HTTP sink)
flink/        Flink SQL: on-hand, alerts, velocity, discrepancy watch
scripts/      Postgres CDC prerequisites, schema validator, logic verifier
docs/         Setup runbook
```

## Verified locally

Everything below was executed, not just written.

**Avro schemas parse and round-trip:**

```
$ .venv/bin/python scripts/validate_schemas.py
OK    locations-value: parses + round-trips 6 fields
OK    low_stock_alerts-value: parses + round-trips 10 fields
OK    products-value: parses + round-trips 8 fields
OK    stock_movements-value: parses + round-trips 10 fields

all 4 schema(s) valid
```

**Postgres CDC prerequisites apply cleanly** (Postgres 16, `wal_level=logical`):

```
 wal_level
-----------
 logical

 schemaname |    tablename
------------+-----------------
 public     | locations
 public     | products
 public     | stock_movements
 public     | warehouses

    rolname    | rolcanlogin | rolreplication
---------------+-------------+----------------
 confluent_cdc | t           | t
```

**Alert logic produces the right answers** — the batch equivalent of the
Flink statements, over seeded movements:

```
 warehouse_code |     sku      | min_stock | on_hand
----------------+--------------+-----------+---------
 BPN            | FLT-OIL-1045 |        60 |      12
 JKT            | BLT-V-A55    |        40 |     380
 KDI            | LUB-HD40-20L |        15 |       0
 SFF            | FLT-OIL-1045 |        60 |      25

 warehouse_code |     sku      | on_hand | min_stock | shortfall | severity
----------------+--------------+---------+-----------+-----------+----------
 BPN            | FLT-OIL-1045 |      12 |        60 |        48 | CRITICAL
 SFF            | FLT-OIL-1045 |      25 |        60 |        35 | CRITICAL
 KDI            | LUB-HD40-20L |       0 |        15 |        15 | STOCKOUT
```

Jakarta holds 380 against a reorder point of 40 and correctly raises no
alert; Kendari shipped its last drum and is flagged STOCKOUT.

**Logical decoding emits CDC events** — a replication slot on the
publication captured a live INSERT:

```
change_events | 4
wal_bytes     | 355 bytes
```

## Submitting this project

Draft answers for every form field — including three lengths of the app
description — are in [docs/SUBMISSION.md](docs/SUBMISSION.md).

## Running it

See [docs/SETUP.md](docs/SETUP.md) for the full runbook.

```bash
# 1. Prepare Postgres (needs wal_level=logical)
psql -h <host> -U postgres -d wms -f scripts/postgres_cdc_setup.sql

# 2. Launch the CDC source (fill in placeholders first)
confluent connect cluster create --config-file connectors/postgres-cdc-source.json

# 3. Run the Flink statements
#    Confluent Cloud console -> Stream Processing -> flink/01_inventory_pipeline.sql

# 4. Launch the alert sink
confluent connect cluster create --config-file connectors/http-sink-alerts.json
```

## Local development

```bash
python3 -m venv .venv && .venv/bin/pip install fastavro
.venv/bin/python scripts/validate_schemas.py
```

To reproduce the Postgres verification:

```bash
docker run -d --name wmscdc-test \
  -e POSTGRES_PASSWORD=test -e POSTGRES_DB=wms -p 55433:5432 \
  postgres:16 -c wal_level=logical

docker cp scripts/. wmscdc-test:/tmp/
docker exec wmscdc-test psql -U postgres -d wms -f /tmp/test_schema.sql
docker exec wmscdc-test psql -U postgres -d wms -f /tmp/postgres_cdc_setup.sql
docker exec wmscdc-test psql -U postgres -d wms -f /tmp/verify_logic.sql
```

## Security note

The connector configs in `connectors/` are templates. Every credential is
a `<PLACEHOLDER>` — inject real values from your shell or a secret manager
(the CDC connector supports secret manager integration for
`database.user` and `database.password`). Do not commit filled configs.
