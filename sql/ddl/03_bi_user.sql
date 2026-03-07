-- Read-only Power BI user scoped to the marts schema.
-- dbt appends the custom schema to the profile schema, so the actual schema
-- name is sdpipe_dev_marts (profile schema = sdpipe_dev, custom schema = marts).
-- Password should be overridden via PGPASSWORD or a secrets manager in production.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'bi_reader') THEN
        CREATE ROLE bi_reader WITH LOGIN PASSWORD 'bi_reader';
    END IF;
END
$$;

-- Create the schema if dbt hasn't run yet so this migration doesn't fail.
CREATE SCHEMA IF NOT EXISTS sdpipe_dev_marts;

GRANT CONNECT ON DATABASE sdpwarehouse TO bi_reader;
GRANT USAGE ON SCHEMA sdpipe_dev_marts TO bi_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA sdpipe_dev_marts TO bi_reader;

-- Ensure future mart tables added by dbt are also readable without re-running this script.
ALTER DEFAULT PRIVILEGES IN SCHEMA sdpipe_dev_marts
    GRANT SELECT ON TABLES TO bi_reader;
