-- ExposureIQ workflow tables (Postgres / RDS)
--
-- These are app-owned tables (lead status/notes, saved chart configs) -
-- separate from your Shodan/lead data tables (company_leads, hosts_scored,
-- vulnerable_cves, vulnerable_hosts, distinct_companies), which you're
-- managing separately.
--
-- Run once against your RDS database:
--   psql "$DATABASE_URL" -f schema.sql
-- or paste into any Postgres client (DBeaver, pgAdmin, etc).

CREATE TABLE IF NOT EXISTS lead_status (
    company     TEXT PRIMARY KEY,
    status      TEXT NOT NULL DEFAULT 'New',   -- New | Contacted | Qualified | Disqualified
    notes       TEXT NOT NULL DEFAULT '',
    updated_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS saved_views (
    id            SERIAL PRIMARY KEY,
    name          TEXT NOT NULL,
    query_string  TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS saved_charts (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    dataset     TEXT NOT NULL,                  -- 'leads' | 'hosts'
    group_by    TEXT NOT NULL,
    metric      TEXT NOT NULL,
    chart_type  TEXT NOT NULL DEFAULT 'bar',     -- bar | line | pie | donut | table
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_lead_status_status   ON lead_status(status);
CREATE INDEX IF NOT EXISTS idx_saved_charts_created ON saved_charts(created_at DESC);
