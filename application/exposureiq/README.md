# ExposureIQ — Sales Intelligence Platform

Flask + pandas backend, plain HTML/CSS/JS frontend styled in an orange/purple
B2B-SaaS look (inspired by firmable.com). All data - both your Shodan/lead
scan tables and the app's own workflow tables - now reads live from Postgres
on RDS. No local CSVs, no SQLite.

## Tables this app expects on RDS

**Your data** (read-only from this app's side - matches `schema.csv`):
`company_leads`, `hosts_scored`, `vulnerable_cves`, `vulnerable_hosts`,
`distinct_companies`.

**App-owned workflow tables** (this app writes to these):
`lead_status`, `saved_views`, `saved_charts` - created by `schema.sql`.

## One-time setup

1. **Create the workflow tables** - run `schema.sql` against your RDS instance
   (only needs to touch `lead_status` / `saved_views` / `saved_charts`; your
   own data tables are assumed to already exist):
   ```bash
   psql "postgresql://USER:PASS@your-endpoint.rds.amazonaws.com:5432/DBNAME" -f schema.sql
   ```
   (or paste it into DBeaver / pgAdmin / whatever client you use)

2. **Point the app at RDS** - copy `.env.example` to `.env` and fill in your
   real connection string:
   ```bash
   cp .env.example .env
   ```
   ```
   DATABASE_URL=postgresql://USER:PASS@your-endpoint.rds.amazonaws.com:5432/DBNAME
   ```
   `python-dotenv` loads `.env` automatically on `python3 app.py` - no extra
   step needed locally. **Never commit the real `.env`.**

   **If your tables live under a non-default schema** (check DBeaver's
   Database Navigator - Databases → your DB → Schemas → if your tables are
   nested under something other than `public`, that's your schema name),
   also set:
   ```
   DB_SCHEMA=your_schema_name
   ```
   Without this, Postgres only looks in `public` by default and you'll get
   a `relation "company_leads" does not exist` error even though the table
   is right there under a different schema.

3. Install dependencies and run:
   ```bash
   pip install -r requirements.txt
   python3 app.py
   ```
   Open http://localhost:5050

Every page now depends on `DATABASE_URL` being set correctly - without it,
every route shows a clean "Database not configured" message instead of
crashing, so it's obvious what's missing rather than a raw stack trace.

## On Elastic Beanstalk

Don't ship `.env` to production. Set `DATABASE_URL` as an **environment
property** instead:
```bash
eb setenv DATABASE_URL="postgresql://USER:PASS@your-endpoint.rds.amazonaws.com:5432/DBNAME"
```
or via the console: Configuration → Software → Environment properties.

Since Beanstalk and RDS are on the same AWS account, make sure the Beanstalk
environment's VPC/security group can actually reach the RDS instance's port
5432 - check the RDS instance's **Connectivity & security** tab if
connections hang or time out. This is usually fine by default if both were
created without a custom VPC setup, but worth a quick check either way.

## Performance - SQL-side filtering, not full table loads

`data_loader.py` pushes filtering, sorting, and pagination down into SQL
(`WHERE`/`ORDER BY`/`LIMIT`/`OFFSET`) rather than pulling full tables into
pandas. This matters a lot once your tables are real-scale (hundreds of MB
to multiple GB) rather than demo-sized - a page load only asks Postgres for
the ~50 rows it's actually going to render, not the entire table.

**Run `indexes.sql` once** against your data tables - without it, SQL-side
filtering still works, just without index support on the columns being
filtered/sorted on:
```bash
psql "$DATABASE_URL" -f indexes.sql
```

The one exception is `company_leads`, which is small enough (~10MB) that
`load_company_leads()` loads it fully - used by the Reports chart builder's
"leads" dataset. Everything touching `hosts_scored`, `vulnerable_hosts`, or
`vulnerable_cves` (the large tables) goes through targeted queries instead.

If your app still feels slow after adding the indexes, the next thing to
check is the RDS instance size itself (`db.t4g.micro` is a burstable,
1GB-RAM instance) - but try the indexes first, since an underpowered
instance running efficient indexed queries is usually still fine for an
app like this; it's unindexed full scans that hurt regardless of instance
size.



## Vulnerable Assets is summary-first now

The page leads with aggregate cards (severity mix, top products, top
companies - all bounded `GROUP BY` queries, not full scans) instead of
dumping a raw row-by-row table by default. Clicking a card applies that
filter to the table below. The per-host CVE list (which used to be one
long unreadable string in a table cell) is now just a count that links to
a dedicated `/vulnerable-assets/host-cves` page for that specific host.

## Performance safeguards (added after real-scale testing)

A few things that turned out to matter once real data was involved, beyond
just SQL-side filtering:

- **Country lookup was accidentally running on every page load.** The
  Company Risk Overview / Lead Management tables don't even display a
  country column, but `filter_leads()` was computing one anyway on every
  call, using a slow per-group `.mode()` in pandas. It's now SQL-only
  (`GROUP BY` + `DISTINCT ON`), and only runs when actually needed (country
  filter, CSV export, Dashboard's geography chart) - not on the default
  page load. This was very likely the main cause of "View" buttons and
  `/leads` timing out.
- **`COUNT(*)` is capped at 50,000** for `vulnerable_hosts` (`data_loader.
  COUNT_CAP`) - an unfiltered "how many rows match" query on a huge table
  is itself an expensive full scan; past the cap, the UI shows "50,000+"
  instead of an exact number. `company_leads` is small enough that its
  count stays exact.
- **CSV exports are capped at 25,000 rows** (`data_loader.EXPORT_ROW_CAP`)
  for the large tables - narrow with filters if you need more than that in
  one export.
- **`statement_timeout` is set to 15s** (`STATEMENT_TIMEOUT_MS` env var) on
  every connection - any single query that runs longer gets cancelled by
  Postgres itself with a clear error, instead of hanging until Beanstalk's
  proxy gives up and shows a bare 504.
- **Gunicorn runs 2 workers, not 3** (`Procfile`) - each worker holds its
  own separate copy of the in-memory caches (`_country_map_cache`,
  `_filter_options_cache`, `_company_leads_cache`), so fewer workers means
  less duplicated memory pressure on a small instance. Bump this back up
  if you move to a bigger instance type and have real concurrent traffic.

## App structure

- `app.py` — routes for all 5 sections; Postgres queries for the workflow
  tables (lead status/notes, saved charts).
- `db.py` — the Postgres connection: a `psycopg2` pool for row-level writes
  (lead status, chart CRUD) and a SQLAlchemy engine for pandas reads.
- `data_loader.py` — pulls `company_leads` / `hosts_scored` / etc. live from
  RDS via `pd.read_sql`, cached in-process per table. All filtering/
  aggregation logic lives here; call `reload_all()` to force a refresh after
  your ETL/Airflow pipeline updates the underlying tables.
- `schema.sql` — creates the three app-owned workflow tables.
- `indexes.sql` — indexes on your existing data tables (`company_leads`,
  `hosts_scored`, `vulnerable_hosts`, `vulnerable_cves`) supporting the
  SQL-side filtering below. Safe to run any time.
- `templates/` — Dashboard, Company Risk Overview, Lead Management,
  Vulnerable Assets, Reports & Analytics, Company detail.
- `static/` — CSS (design tokens at top of `style.css`) and the JS handling
  filter auto-submit, status updates, and tabs.

## Filters implemented

Grade, risk score range, industry, country, technology/product exposed,
worst severity present, min CVSS, min EPSS, min host count, min data
confidence, exclude hosting/ISP providers, free-text company/domain search.
CSV export respects whatever filters are active.

Note: the **Industry** filter has no backing column in your current
`company_leads` schema (`_industry` isn't in `schema.csv`), so it'll render
empty until that column exists - wire it up once you have it from an
enrichment source, no other code changes needed.

## Suggested filters (worth adding once you have real company firmographics)

- **Company size / employee count** — bigger orgs often mean bigger deals
  but slower cycles; SDRs usually want to filter this explicitly.
- **Industry vertical + sub-vertical** — wire `_industry` (or equivalent)
  once it's populated from an enrichment API.
- **Days since last scan** — freshness matters a lot for outbound; a
  critical CVE found 90 days ago may already be patched.
- **"New this week" flag** — companies that just crossed into Grade A,
  so reps chase movement, not just a static snapshot.
- **Exclude already-contacted / already-in-CRM** — once you sync with
  Salesforce/HubSpot, filter out anything already owned by a rep.
- **ASN/hosting provider allowlist-exclude** — beyond the boolean
  `is_provider` flag, letting reps exclude specific known cloud ASNs
  (AWS, Azure, GCP, Cloudflare) cuts a lot of remaining noise.
- **Compliance-relevant exposure** — e.g. a "PCI-relevant" filter (payment
  ports/services exposed) or "healthcare-relevant" (exposed patient portals)
  if you sell compliance-driven security tools.

## Other easy, high-impact ideas

1. **CRM push button** — a "Send to HubSpot/Salesforce" action per lead.
   You're already connected to Zapier in this workspace; it can push a
   filtered lead list into a CRM as a workflow without custom API code.
2. **Saved filter views / watchlists** — let reps save "Grade A + Fintech +
   US" as a named view they reopen daily. The `saved_views` table is
   already in `schema.sql`, just needs UI wiring.
3. **Auto-generated outreach talking point** — you already compute
   `priority_reason`; a one-line "email opener" per company (the specific
   CVE + port + why it matters) turns the score into something a rep can
   paste straight into an email.
4. **Slack/email digest of new Grade-A leads** — a daily cron that diffs
   yesterday's vs today's `company_leads` and pings the team when a new hot
   lead appears, so nobody has to keep re-checking the dashboard.
5. **"Similar companies" on the detail page** — nearest-neighbor on the
   engineered features (same vuln bucket + similar host count) to help reps
   batch outreach by pattern instead of one at a time.
6. **Lightweight auth** — even basic login (Flask-Login + a users table)
   before this goes further than your own laptop, since it's exposing
   company vulnerability data.

## What's not in this version

The Bedrock RAG/tool-use assistant from an earlier iteration has been
pulled out at your request - `rag_engine.py`, `build_rag_index.py`, and the
floating chat widget are removed. Nothing in the current app depends on
`boto3` or AWS Bedrock; `requirements.txt` reflects that. Happy to add it
back on top of the live RDS data whenever you want to revisit it - the
`filter_leads` / `get_company_detail` functions it called are unchanged.
