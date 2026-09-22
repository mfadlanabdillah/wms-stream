-- Dry-run of the live demo insert in PANDUAN.md step 7.
-- Wrapped in a transaction and rolled back, so the seeded state is
-- preserved for the actual live demo.
BEGIN;

INSERT INTO stock_movements (product_id, location_id, type, qty, reference_no, user_id, created_at)
SELECT p.id, l.id, 'out', -350, 'SO-DEMO-LIVE', 1, NOW()
FROM products p
JOIN warehouses w ON w.code = 'JKT'
JOIN locations  l ON l.warehouse_id = w.id
WHERE p.sku = 'BLT-V-A55';

\echo '--- alerts AFTER the demo insert ---'
WITH oh AS (
    SELECT w.code AS wc, p.sku, MAX(p.min_stock)::int AS ms, SUM(m.qty)::int AS onhand
    FROM stock_movements m
    JOIN locations  l ON m.location_id = l.id
    JOIN warehouses w ON l.warehouse_id = w.id
    JOIN products   p ON m.product_id = p.id
    GROUP BY w.code, p.sku
)
SELECT wc AS warehouse, sku, onhand AS on_hand, ms AS min_stock,
       CASE WHEN onhand <= 0 THEN 'STOCKOUT'
            WHEN onhand * 2 <= ms THEN 'CRITICAL'
            ELSE 'WARNING' END AS severity
FROM oh
WHERE ms > 0 AND onhand <= ms
ORDER BY warehouse;

ROLLBACK;
