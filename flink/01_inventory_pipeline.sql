-- =====================================================================
-- WMS Real-Time Inventory — Confluent Cloud for Apache Flink
-- =====================================================================
-- Run these statements in order in the Flink SQL workspace
-- (Confluent Cloud console -> Stream Processing -> open your compute pool).
--
-- Every topic created by the Postgres CDC connector is automatically
-- visible here as a table, because Confluent Cloud for Flink shares the
-- Kafka cluster's metadata and Schema Registry. No CREATE TABLE needed
-- for the source tables — that is the Stream Governance payoff.
--
-- Statement 1 and 2 produce the alert stream that the HTTP Sink drains.
-- =====================================================================


-- ---------------------------------------------------------------------
-- 0. Sanity check: confirm CDC events are arriving.
-- ---------------------------------------------------------------------
SELECT
    id,
    product_id,
    location_id,
    `type`,
    qty,
    reference_no,
    created_at
FROM `wms.public.stock_movements`
ORDER BY created_at DESC
LIMIT 20;


-- ---------------------------------------------------------------------
-- 1. Running on-hand stock per SKU per warehouse.
--
--    stock_movements holds signed quantities (out is negative, adjust can
--    be negative), so on-hand is a plain running SUM. Joining locations
--    and warehouses turns a bin id into a site name, and joining products
--    brings in the reorder point.
--
--    This is the table the WMS dashboard reads instead of running a
--    heavy aggregate query against Postgres on every page load.
-- ---------------------------------------------------------------------
CREATE TABLE `wms.inventory.on_hand` (
    warehouse_code   STRING,
    warehouse_name   STRING,
    sku              STRING,
    product_name     STRING,
    unit             STRING,
    min_stock        INT,
    on_hand          INT,
    last_movement_at TIMESTAMP_LTZ(3),
    PRIMARY KEY (warehouse_code, sku) NOT ENFORCED
) DISTRIBUTED BY (warehouse_code, sku) INTO 6 BUCKETS
WITH (
    'changelog.mode' = 'upsert',
    'value.format'   = 'avro-registry',
    'key.format'     = 'avro-registry'
);

INSERT INTO `wms.inventory.on_hand`
SELECT
    w.code                  AS warehouse_code,
    w.name                  AS warehouse_name,
    p.sku                   AS sku,
    p.name                  AS product_name,
    p.unit                  AS unit,
    CAST(MAX(p.min_stock) AS INT) AS min_stock,
    CAST(SUM(m.qty) AS INT)       AS on_hand,
    -- CDC timestamps arrive as TIMESTAMP(6); cast to match the column type.
    CAST(MAX(m.created_at) AS TIMESTAMP_LTZ(3)) AS last_movement_at
FROM `wms.public.stock_movements` AS m
JOIN `wms.public.locations`  AS l ON m.location_id = l.id
JOIN `wms.public.warehouses` AS w ON l.warehouse_id = w.id
JOIN `wms.public.products`   AS p ON m.product_id  = p.id
GROUP BY w.code, w.name, p.sku, p.name, p.unit;


-- ---------------------------------------------------------------------
-- 2. Low-stock alerts.
--
--    Emits one record whenever running on-hand for a SKU at a site sits
--    at or below that SKU's reorder point. Severity is graded so the
--    downstream HTTP sink can route STOCKOUT to a pager and WARNING to
--    a digest.
--
--    Schema matches schemas/low_stock_alerts-value.avsc.
-- ---------------------------------------------------------------------
CREATE TABLE `wms.alerts.low_stock` (
    warehouse_code STRING,
    warehouse_name STRING,
    sku            STRING,
    product_name   STRING,
    unit           STRING,
    on_hand        INT,
    min_stock      INT,
    shortfall      INT,
    severity       STRING,
    detected_at    TIMESTAMP_LTZ(3),
    PRIMARY KEY (warehouse_code, sku) NOT ENFORCED
) DISTRIBUTED BY (warehouse_code, sku) INTO 6 BUCKETS
WITH (
    'changelog.mode' = 'upsert',
    'value.format'   = 'avro-registry',
    'key.format'     = 'avro-registry'
);

INSERT INTO `wms.alerts.low_stock`
SELECT
    warehouse_code,
    warehouse_name,
    sku,
    product_name,
    unit,
    on_hand,
    min_stock,
    min_stock - on_hand AS shortfall,
    CASE
        WHEN on_hand <= 0                        THEN 'STOCKOUT'
        WHEN on_hand * 2 <= min_stock            THEN 'CRITICAL'
        ELSE                                          'WARNING'
    END AS severity,
    last_movement_at AS detected_at
FROM `wms.inventory.on_hand`
WHERE min_stock > 0
  AND on_hand <= min_stock;


-- ---------------------------------------------------------------------
-- 3. Outbound velocity per site, 1-hour tumbling window.
--
--    Answers "how fast is this SKU actually moving right now" — the input
--    a planner needs to size a reorder, and something a nightly batch
--    report cannot give. Uses Flink's TUMBLE table-valued function over
--    the CDC event time.
-- ---------------------------------------------------------------------
SELECT
    window_start,
    window_end,
    w.code                        AS warehouse_code,
    p.sku                         AS sku,
    CAST(SUM(-m.qty) AS INT)      AS qty_shipped,
    COUNT(*)                      AS movement_count
FROM TABLE(
        TUMBLE(TABLE `wms.public.stock_movements`, DESCRIPTOR(`$rowtime`), INTERVAL '1' HOUR)
     ) AS m
JOIN `wms.public.locations`  AS l ON m.location_id = l.id
JOIN `wms.public.warehouses` AS w ON l.warehouse_id = w.id
JOIN `wms.public.products`   AS p ON m.product_id  = p.id
WHERE m.`type` = 'out'
GROUP BY window_start, window_end, w.code, p.sku;


-- ---------------------------------------------------------------------
-- 4. Stock-opname discrepancy watch (adjustments only).
--
--    Large 'adjust' movements mean physical stock disagreed with the
--    system. Surfacing them in real time turns a quarterly audit finding
--    into a same-day investigation.
-- ---------------------------------------------------------------------
SELECT
    w.code            AS warehouse_code,
    p.sku             AS sku,
    p.name            AS product_name,
    m.qty             AS adjustment_qty,
    m.reference_no    AS opname_ref,
    m.note            AS reason,
    m.created_at      AS adjusted_at
FROM `wms.public.stock_movements` AS m
JOIN `wms.public.locations`  AS l ON m.location_id = l.id
JOIN `wms.public.warehouses` AS w ON l.warehouse_id = w.id
JOIN `wms.public.products`   AS p ON m.product_id  = p.id
WHERE m.`type` = 'adjust'
  AND ABS(m.qty) >= 10;
