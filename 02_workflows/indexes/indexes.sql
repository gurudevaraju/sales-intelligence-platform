-- Indexes for the live 'cyber' schema data tables.
-- Safe to run now - IF NOT EXISTS, doesn't touch data. These matter most
-- once filtering/sorting is pushed into SQL (WHERE/ORDER BY) rather than
-- done in pandas after a full table pull - an index can't speed up
-- "SELECT * FROM table" with no WHERE clause, since that's always a full
-- scan regardless.
--
-- Run:
--   psql "$DATABASE_URL" -f indexes.sql
-- (or paste into DBeaver's SQL editor, connected to the postgres DB)

SET search_path TO cyber;

-- company_leads: filtered/sorted by grade, score, host/cve counts, provider flag
CREATE INDEX IF NOT EXISTS idx_company_leads_grade      ON company_leads(lead_grade);
CREATE INDEX IF NOT EXISTS idx_company_leads_score      ON company_leads(lead_score DESC);
CREATE INDEX IF NOT EXISTS idx_company_leads_provider   ON company_leads(is_provider);
CREATE INDEX IF NOT EXISTS idx_company_leads_company    ON company_leads(company);

-- Free-text company/domain search (the "q" filter) - trigram index makes
-- ILIKE '%term%' searches fast instead of a full scan per keystroke.
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS idx_company_leads_company_trgm ON company_leads USING gin (company gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_company_leads_domain_trgm  ON company_leads USING gin (domain gin_trgm_ops);

-- hosts_scored: filtered by company (joins/lookups), country, product, severity, port
CREATE INDEX IF NOT EXISTS idx_hosts_scored_company   ON hosts_scored(_company);
CREATE INDEX IF NOT EXISTS idx_hosts_scored_country   ON hosts_scored(country);
CREATE INDEX IF NOT EXISTS idx_hosts_scored_product   ON hosts_scored(product);
CREATE INDEX IF NOT EXISTS idx_hosts_scored_severity  ON hosts_scored(_vuln_severity);
CREATE INDEX IF NOT EXISTS idx_hosts_scored_port      ON hosts_scored(port);

-- vulnerable_hosts: same access pattern as hosts_scored, plus this is the
-- table the Vulnerable Assets page sorts by CVSS on
CREATE INDEX IF NOT EXISTS idx_vuln_hosts_company   ON vulnerable_hosts(_company);
CREATE INDEX IF NOT EXISTS idx_vuln_hosts_country   ON vulnerable_hosts(country);
CREATE INDEX IF NOT EXISTS idx_vuln_hosts_product   ON vulnerable_hosts(product);
CREATE INDEX IF NOT EXISTS idx_vuln_hosts_severity  ON vulnerable_hosts(_vuln_severity);
CREATE INDEX IF NOT EXISTS idx_vuln_hosts_cvss      ON vulnerable_hosts(_vuln_max_cvss DESC);
CREATE INDEX IF NOT EXISTS idx_vuln_hosts_port      ON vulnerable_hosts(port);

-- vulnerable_cves: company detail page pulls all CVEs for one company, sorted by CVSS
CREATE INDEX IF NOT EXISTS idx_vuln_cves_company  ON vulnerable_cves(_company);
CREATE INDEX IF NOT EXISTS idx_vuln_cves_cve      ON vulnerable_cves(cve);
CREATE INDEX IF NOT EXISTS idx_vuln_cves_cvss     ON vulnerable_cves(cvss DESC);
