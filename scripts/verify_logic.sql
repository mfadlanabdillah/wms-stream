-- Logic verification for the Flink pipeline in flink/01_inventory_pipeline.sql.
--
-- Flink's streaming SUM over the CDC stream must converge to the same
-- answer as this batch query over the source table. Running it against a
-- real Postgres proves the on-hand arithmetic and the severity grading
-- before any of it is deployed to Confluent Cloud.

-- Movements: signed quantities, exactly as the WMS records them.
INSERT INTO stock_movements (product_id, location_id, type, qty, reference_no, user_id, created_at)
SELECT p.id, l.id, m.type, m.qty, m.ref, 1, NOW()
FROM (VALUES
    -- Oil filter at Balikpapan: receive 100, ship 88 -> 12 on hand, min 60
    ('FLT-OIL-1045', 'BPN', 'in',      100, 'PO-2026-0455'),
    ('FLT-OIL-1045', 'BPN', 'out',     -60, 'SO-2026-0912'),
    ('FLT-OIL-1045', 'BPN', 'out',     -28, 'SO-2026-0933'),
    -- V-belt at Jakarta: healthy stock, must NOT alert
    ('BLT-V-A55',    'JKT', 'in',      500, 'PO-2026-0461'),
    ('BLT-V-A55',    'JKT', 'out',    -120, 'SO-2026-0940'),
    -- Hydraulic oil at Kendari: shipped everything -> stockout, min 15
    ('LUB-HD40-20L', 'KDI', 'in',       20, 'PO-2026-0470'),
    ('LUB-HD40-20L', 'KDI', 'out',     -20, 'SO-2026-0951'),
    -- Oil filter at Sofifi: opname found 5 missing -> critical
    ('FLT-OIL-1045', 'SFF', 'in',       30, 'PO-2026-0472'),
    ('FLT-OIL-1045', 'SFF', 'adjust',   -5, 'OPN-2026-011')
) AS m(sku, wh, type, qty, ref)
JOIN products   p ON p.sku = m.sku
JOIN warehouses w ON w.code = m.wh
JOIN locations  l ON l.warehouse_id = w.id;

\echo '--- on_hand (mirrors Flink statement 1) ---'
SELECT
    w.code  AS warehouse_code,
    p.sku,
    MAX(p.min_stock)   AS min_stock,
    SUM(m.qty)::int    AS on_hand
FROM stock_movements m
JOIN locations  l ON m.location_id = l.id
JOIN warehouses w ON l.warehouse_id = w.id
JOIN products   p ON m.product_id = p.id
GROUP BY w.code, p.sku
ORDER BY w.code, p.sku;

\echo ''
\echo '--- low_stock_alerts (mirrors Flink statement 2) ---'
WITH on_hand AS (
    SELECT
        w.code AS warehouse_code,
        w.name AS warehouse_name,
        p.sku,
        p.name AS product_name,
        p.unit,
        MAX(p.min_stock)::int AS min_stock,
        SUM(m.qty)::int       AS on_hand
    FROM stock_movements m
    JOIN locations  l ON m.location_id = l.id
    JOIN warehouses w ON l.warehouse_id = w.id
    JOIN products   p ON m.product_id = p.id
    GROUP BY w.code, w.name, p.sku, p.name, p.unit
)
SELECT
    warehouse_code,
    sku,
    on_hand,
    min_stock,
    min_stock - on_hand AS shortfall,
    CASE
        WHEN on_hand <= 0             THEN 'STOCKOUT'
        WHEN on_hand * 2 <= min_stock THEN 'CRITICAL'
        ELSE 'WARNING'
    END AS severity
FROM on_hand
WHERE min_stock > 0
  AND on_hand <= min_stock
ORDER BY severity, warehouse_code;
