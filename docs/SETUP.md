# Setup runbook

Order matters. Steps 1–3 must be done before the Flink statements have
anything to read, and the Stream Lineage graph the submission form asks
for only renders once data has actually flowed.

## 0. Prerequisites

- A Confluent Cloud account (you have one; no cluster yet).
- The WMS Postgres reachable from Confluent Cloud. If it only listens on
  localhost, expose it first — a fully-managed connector cannot reach a
  laptop behind NAT. Options: an ngrok/Cloudflare TCP tunnel, or run the
  WMS Postgres on a small cloud VM for the demo.
- `confluent` CLI, if you prefer CLI over console:
  `curl -sL --http1.1 https://cnfl.io/cli | sh -s -- latest`

## 1. Create the cluster

Confluent Cloud console → **Environments** → your environment →
**Create cluster**.

- Type: **Basic** (cheapest, covers this workload; the $400 trial credit
  more than covers a Dev Day demo)
- Cloud/region: pick the region closest to your Postgres to keep CDC
  latency low
- Name: `wms-dsp`

Then enable **Schema Registry** for the environment if prompted — the
Avro formats and Stream Governance features depend on it.

## 2. Create API keys

- **Kafka cluster API key**: cluster → **API Keys** → *Create key* →
  Global access (fine for a demo). Save both halves; they go into
  `kafka.api.key` / `kafka.api.secret` in both connector configs.
- **Schema Registry API key**: environment → **Stream Governance** →
  *API keys*. Needed if you run producers/consumers yourself.

## 3. Prepare Postgres

```bash
psql -h <host> -U postgres -d wms -f scripts/postgres_cdc_setup.sql
```

`wal_level` must report `logical`. If it does not:

```sql
ALTER SYSTEM SET wal_level = 'logical';
-- then RESTART Postgres (reload is not enough)
```

Managed Postgres uses a parameter instead:

| Provider | Parameter |
|---|---|
| AWS RDS / Aurora | `rds.logical_replication = 1` |
| Google Cloud SQL | `cloudsql.logical_decoding = on` |
| Azure Database | `wal_level = logical` |

Change the `confluent_cdc` password from `CHANGE_ME` before running this
anywhere real.

## 4. Launch the Postgres CDC source

Fill the placeholders in `connectors/postgres-cdc-source.json`, then:

```bash
confluent connect cluster create --config-file connectors/postgres-cdc-source.json
```

Or in the console: **Connectors** → search *Postgres CDC Source V2* →
there is a **Switch to JSON** toggle, paste the file contents there rather
than filling 20 fields by hand.

Wait for status **Running**, then confirm topics exist:

```bash
confluent kafka topic list
```

You should see `wms.public.stock_movements`, `wms.public.products`,
`wms.public.locations`, `wms.public.warehouses`.

If the connector fails, the message is usually one of:

| Symptom | Cause |
|---|---|
| `could not access file "pgoutput"` | `wal_level` is not `logical` |
| connection timeout | Postgres not reachable from Confluent Cloud |
| `permission denied for table` | skipped the GRANTs in step 3 |
| `replication slot already exists` | a previous connector left `wms_cdc_slot`; drop it with `SELECT pg_drop_replication_slot('wms_cdc_slot');` |

## 5. Generate some movement

The pipeline needs live rows. Either use the WMS UI to record a few goods
receipts and shipments, or seed directly:

```bash
psql -h <host> -U postgres -d wms -f scripts/verify_logic.sql
```

This is also what makes the Stream Lineage graph interesting — lineage
renders from observed traffic, not from configuration.

## 6. Run the Flink statements

Console → **Stream Processing** → create a compute pool (smallest is
fine) → open a workspace, set catalog to your environment and database to
your cluster, then run `flink/01_inventory_pipeline.sql` statement by
statement.

Run statement 0 first and confirm rows come back. If it is empty, CDC is
not flowing and there is no point running the rest.

Statements 1 and 2 are `CREATE TABLE` + `INSERT INTO` — they start
long-running jobs. Leave them running.

## 7. Launch the HTTP sink

Fill in `connectors/http-sink-alerts.json`:

- `http.api.base.url` — your WMS API base
- `api1.http.request.sensitive.headers` — the bearer token

```bash
confluent connect cluster create --config-file connectors/http-sink-alerts.json
```

No endpoint ready yet? Point it at a request-bin style URL
(webhook.site) for the demo — the lineage graph and the sink both work
the same, and you get a visible payload to show.

## 8. Capture Stream Lineage for the submission

Console → left menu → **Stream Lineage**. Pick the
`wms.public.stock_movements` topic as the entry point so the graph shows
the full path: Postgres → CDC connector → topics → Flink → alert topic →
HTTP sink.

Let it sit a minute so all nodes appear, take a full-window screenshot,
upload to imgbb.com (no account needed) or Google Drive set to *anyone
with the link*, and paste that link into the form.

A graph with connectors and Flink nodes visible is worth far more to the
judges than a bare topic list — which is exactly why steps 5 and 6 come
before this one.
