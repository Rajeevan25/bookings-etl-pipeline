-- =============================================================================
-- sql/04_setup_db.sql
-- Run this script as the postgres superuser to create the database,
-- a dedicated application user, and apply the schema.
--
-- Usage:
--   psql -U postgres -f sql/04_setup_db.sql
-- =============================================================================

-- 1. Create database
SELECT 'CREATE DATABASE travel_db'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'travel_db'
)\gexec

-- 2. Connect to it
\c travel_db

-- 3. Create application user with least-privilege
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'etl_user') THEN
        CREATE ROLE etl_user WITH LOGIN PASSWORD 'change_me_in_env';
    END IF;
END
$$;

-- 4. Grant only what's needed (least privilege principle)
GRANT CONNECT ON DATABASE travel_db TO etl_user;
GRANT USAGE  ON SCHEMA public TO etl_user;
GRANT SELECT, INSERT, UPDATE, DELETE
    ON ALL TABLES    IN SCHEMA public TO etl_user;
GRANT USAGE, SELECT
    ON ALL SEQUENCES IN SCHEMA public TO etl_user;

-- Allow future tables to be accessible
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES    TO etl_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT USAGE, SELECT                  ON SEQUENCES TO etl_user;

-- 5. Run schema + indexes
\i sql/01_schema.sql
\i sql/02_indexes.sql

\echo '✅  Database travel_db ready for ETL pipeline.'
