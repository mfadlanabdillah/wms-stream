# Submission answers — Developer Day Confluent app form

Copy-paste material for each form field. Everything factual here traces
back to the WMS codebase and the inventory workbooks; verified output is
in the README.

---

## Which Confluent connector(s) are you using?

```
PostgreSQL CDC Source V2 (Debezium) — captures every INSERT/UPDATE/DELETE
on the WMS operational tables (stock_movements, products, locations,
warehouses) plus an initial snapshot.

HTTP Sink V2 — delivers graded low-stock alerts from the
wms.alerts.low_stock topic to the purchasing notification API.
```

---

## Please describe your Confluent application

Three versions. Pick one — the medium one is the safest default for a
form field. Edit the wording so it sounds like you; the facts are already
correct.

### Short (~80 words)

```
WMS Real-Time Inventory streams our warehouse management database into
Confluent with Postgres CDC, computes on-hand stock per SKU per site
continuously in Flink SQL, and pushes a reorder alert to purchasing the
moment a SKU drops below its reorder point. It replaces a manual
spreadsheet reconciliation across 15 warehouse and service-point
locations, where stockouts were typically found a day late — after the
order had already been promised to a customer. It required no changes to
the existing application code.
```

### Medium (~180 words) — recommended

```
What it does

WMS Real-Time Inventory turns our existing warehouse management system
into a streaming one. The Postgres CDC Source V2 connector captures every
stock movement, product and location change out of the WMS database.
Confluent Cloud for Apache Flink continuously computes on-hand stock per
SKU per site, grades anything below its reorder point as WARNING,
CRITICAL or STOCKOUT, and the HTTP Sink V2 connector delivers that alert
straight to our purchasing team. All topics carry Avro schemas registered
in Stream Governance, so field definitions and compatibility are enforced
rather than assumed.

Who it is for

Warehouse supervisors, inventory planners and the purchasing team
operating 15 locations across Indonesia — from main warehouses in Jakarta
and Balikpapan to service points and eight VHS sites.

The benefit

Stock levels were reconciled from per-location spreadsheets, so a
stockout surfaced about a day late — usually after the order had been
promised to a customer. Detection now happens within seconds of the
movement that causes it, and reordering starts from a real number instead
of yesterday's. Critically, this needed no change to the existing
application: the WMS keeps writing to Postgres exactly as before, and CDC
does the rest.
```

### Long (~280 words) — if the field rewards detail

```
The problem

Our warehouse management system is a Laravel application on PostgreSQL
serving 15 locations across Indonesia — main warehouses (Jakarta,
Balikpapan, Kendari, Sofifi, Angsana), service points (Muara Teweh, Qinfa
Mangalau) and eight VHS sites. Real-time stock was, in practice, a set of
per-location spreadsheets reconciled by hand. The cost was predictable: a
stockout became visible roughly a day after it happened, which in most
cases meant after the order had already been promised to a customer.
Reorder decisions were made against numbers that were already stale.

What we built

The PostgreSQL CDC Source V2 (Debezium) connector captures every INSERT,
UPDATE and DELETE on the WMS tables and publishes them to Kafka. Four
Flink SQL statements run on top:

  1. Running on-hand stock per SKU per site, joining bin locations to
     warehouses and products so an alert names the site and carries the
     reorder point.
  2. Low-stock alerts graded WARNING / CRITICAL / STOCKOUT, so a stockout
     can page someone while a warning only joins a digest.
  3. Hourly outbound velocity per site — how fast a SKU is actually
     moving, which is what sizes a reorder and is exactly what a nightly
     batch report cannot tell you.
  4. A stock-opname discrepancy watch that surfaces large adjustments in
     real time, turning a quarterly audit finding into a same-day
     investigation.

The HTTP Sink V2 connector then delivers alerts to our purchasing
notification endpoint. All six topics carry Avro schemas registered in
Stream Governance with field-level documentation, and Stream Lineage gives
us end-to-end provenance from Postgres table to alert endpoint.

Why it matters

Detection moved from about a day to seconds, and reorder decisions now
start from live numbers. The part I value most: zero application
rewrite. The WMS keeps writing to Postgres exactly as it always has —
CDC, Flink and the sink connector were added alongside it.
```

---

## Paste here your schema

Use `schemas/stock_movements-value.avsc` — it is the core event stream
and shows the documented-field discipline. If the field allows more, add
`schemas/low_stock_alerts-value.avsc` to show the derived stream too.

---

## Screenshot / Stream Lineage link

`[NO AI Usage Allowed]` applies here: the image must be a genuine
screenshot of your own Confluent Cloud console, not an AI-generated or
mocked-up picture. Follow `docs/SETUP.md` steps 1–8, then upload to
imgbb.com or Google Drive (link sharing set to "anyone with the link").

Do not screenshot before data has flowed — lineage renders from observed
traffic, so an idle graph looks empty and undersells the project.

---

## Fields only you can answer

- Current location (e.g. `Jakarta, Indonesia`)
- First name / Last name
- Email used for Confluent Cloud
- Job title
- Company name
- GitHub repo link — push this repo, then paste the URL
