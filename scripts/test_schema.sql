-- Minimal WMS schema, transcribed from the Laravel migrations in
-- workspace/wms-fadlan/database/migrations. Used to test the CDC setup
-- script and the Avro schemas against a real Postgres server.
CREATE TABLE warehouses (
    id          BIGSERIAL PRIMARY KEY,
    code        VARCHAR(20) NOT NULL UNIQUE,
    name        VARCHAR(255) NOT NULL,
    created_at  TIMESTAMP,
    updated_at  TIMESTAMP
);

CREATE TABLE locations (
    id            BIGSERIAL PRIMARY KEY,
    warehouse_id  BIGINT NOT NULL REFERENCES warehouses(id) ON DELETE CASCADE,
    code          VARCHAR(20) NOT NULL,
    name          VARCHAR(255) NOT NULL,
    created_at    TIMESTAMP,
    updated_at    TIMESTAMP,
    UNIQUE (warehouse_id, code)
);

CREATE TABLE products (
    id          BIGSERIAL PRIMARY KEY,
    sku         VARCHAR(50) NOT NULL UNIQUE,
    name        VARCHAR(255) NOT NULL,
    unit        VARCHAR(20) NOT NULL,
    barcode     VARCHAR(64) UNIQUE,
    min_stock   INTEGER NOT NULL DEFAULT 0,
    created_at  TIMESTAMP,
    updated_at  TIMESTAMP
);

CREATE TABLE users (
    id    BIGSERIAL PRIMARY KEY,
    name  VARCHAR(255) NOT NULL
);

CREATE TABLE stock_movements (
    id            BIGSERIAL PRIMARY KEY,
    product_id    BIGINT NOT NULL REFERENCES products(id),
    location_id   BIGINT NOT NULL REFERENCES locations(id),
    type          VARCHAR(10) NOT NULL,
    qty           INTEGER NOT NULL,
    reference_no  VARCHAR(100),
    note          TEXT,
    user_id       BIGINT NOT NULL REFERENCES users(id),
    created_at    TIMESTAMP,
    updated_at    TIMESTAMP
);
CREATE INDEX ON stock_movements (product_id, location_id);
CREATE INDEX ON stock_movements (location_id, product_id);

-- Sites taken from the real inventory workbooks in ~/iti/wms.
INSERT INTO warehouses (code, name, created_at) VALUES
    ('JKT', 'Jakarta',            NOW()),
    ('BPN', 'Balikpapan',         NOW()),
    ('KDI', 'Kendari',            NOW()),
    ('SFF', 'Sofifi',             NOW()),
    ('AGS', 'Angsana',            NOW()),
    ('MTW', 'Service Point Muara Teweh', NOW());

INSERT INTO locations (warehouse_id, code, name, created_at)
SELECT w.id, w.code || '-A-01', 'Rack A Level 1', NOW() FROM warehouses w;

INSERT INTO products (sku, name, unit, min_stock, created_at) VALUES
    ('FLT-OIL-1045', 'Oil Filter Komatsu PC200', 'PCS', 60, NOW()),
    ('BLT-V-A55',    'V-Belt A55',               'PCS', 40, NOW()),
    ('LUB-HD40-20L', 'Hydraulic Oil HD40 20L',   'DRM', 15, NOW());

INSERT INTO users (name) VALUES ('warehouse-operator');
