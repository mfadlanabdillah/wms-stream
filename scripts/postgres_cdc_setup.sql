-- =====================================================================
-- Postgres prerequisites for the Confluent PostgreSQL CDC Source V2
-- connector against the WMS database.
--
--   psql -h <host> -U postgres -d wms -f scripts/postgres_cdc_setup.sql
--
-- Requires wal_level = logical on the server. Check and set:
--   SHOW wal_level;                       -- must report 'logical'
--   ALTER SYSTEM SET wal_level = 'logical';
--   -- then restart Postgres (a reload is NOT enough for wal_level)
--
-- On managed Postgres set it via the provider's parameter group instead:
--   AWS RDS / Aurora : rds.logical_replication = 1
--   Google Cloud SQL : cloudsql.logical_decoding = on
--   Azure            : wal_level = logical
-- =====================================================================

-- 1. Least-privilege replication user. Debezium needs LOGIN + REPLICATION;
--    it does not need superuser.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'confluent_cdc') THEN
        CREATE ROLE confluent_cdc WITH LOGIN REPLICATION PASSWORD 'CHANGE_ME';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE wms TO confluent_cdc;
GRANT USAGE  ON SCHEMA public TO confluent_cdc;

-- 2. Read access to the captured tables (and anything added later).
GRANT SELECT ON ALL TABLES IN SCHEMA public TO confluent_cdc;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO confluent_cdc;

-- 3. Publication scoped to just the WMS tables we stream. Matches
--    table.include.list in connectors/postgres-cdc-source.json.
DROP PUBLICATION IF EXISTS wms_cdc_publication;
CREATE PUBLICATION wms_cdc_publication FOR TABLE
    public.stock_movements,
    public.products,
    public.locations,
    public.warehouses;

-- 4. REPLICA IDENTITY FULL on the master tables so UPDATE/DELETE events
--    carry the full previous row. Without this, Debezium emits only the
--    primary key in the "before" image and the Flink joins lose columns
--    on update.
ALTER TABLE public.products   REPLICA IDENTITY FULL;
ALTER TABLE public.locations  REPLICA IDENTITY FULL;
ALTER TABLE public.warehouses REPLICA IDENTITY FULL;

-- stock_movements is append-only in the WMS, so the default (primary key)
-- replica identity is sufficient and cheaper.
ALTER TABLE public.stock_movements REPLICA IDENTITY DEFAULT;

-- 5. Verify. Every row below should look sane before launching the connector.
SELECT current_setting('wal_level') AS wal_level;

SELECT pubname, puballtables
FROM pg_publication
WHERE pubname = 'wms_cdc_publication';

SELECT schemaname, tablename
FROM pg_publication_tables
WHERE pubname = 'wms_cdc_publication'
ORDER BY tablename;

SELECT rolname, rolcanlogin, rolreplication
FROM pg_roles
WHERE rolname = 'confluent_cdc';
