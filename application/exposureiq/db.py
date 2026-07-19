"""
Postgres connection pool for ExposureIQ's own workflow tables
(lead_status, saved_views, saved_charts) - separate from the Shodan/lead
data itself, which still comes from data_loader.py.

Reads connection info from DATABASE_URL (standard Postgres URL). On
Elastic Beanstalk, set this as an environment property (eb setenv or the
console) rather than shipping a .env file. Locally, copy .env.example to
.env and python-dotenv will pick it up.

DATABASE_URL format:
    postgresql://USERNAME:PASSWORD@YOUR-RDS-ENDPOINT:5432/DBNAME
"""
import os

import psycopg2
import psycopg2.extras
import psycopg2.pool

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # fine in prod - Beanstalk sets real env vars, no .env needed

DATABASE_URL = os.environ.get("DATABASE_URL")
DB_SCHEMA = os.environ.get("DB_SCHEMA", "public")
# search_path checks DB_SCHEMA first, then falls back to public - so this
# works whether your data tables and the app's workflow tables (lead_status
# etc.) are in the same schema or split across two.
SEARCH_PATH = f"{DB_SCHEMA},public" if DB_SCHEMA != "public" else "public"
# Any single query that runs longer than this gets cancelled by Postgres
# itself, with a clear error - instead of hanging until Beanstalk's proxy
# gives up and shows a bare 504.
STATEMENT_TIMEOUT_MS = int(os.environ.get("STATEMENT_TIMEOUT_MS", "15000"))

_pool = None
_engine = None


class DatabaseNotConfigured(Exception):
    pass


def get_engine():
    """SQLAlchemy engine used by data_loader.py for pandas reads
    (company_leads, hosts_scored, vulnerable_cves, vulnerable_hosts,
    distinct_companies) - the actual lead/scan data tables, live on RDS."""
    global _engine
    if _engine is None:
        if not DATABASE_URL:
            raise DatabaseNotConfigured(
                "DATABASE_URL is not set. Copy .env.example to .env (locally) "
                "or set it as an Elastic Beanstalk environment property "
                "(production), then restart the app."
            )
        from sqlalchemy import create_engine
        _engine = create_engine(
            DATABASE_URL, pool_pre_ping=True, pool_size=5, max_overflow=5,
            connect_args={
                "options": f"-c search_path={SEARCH_PATH} -c statement_timeout={STATEMENT_TIMEOUT_MS}",
                "connect_timeout": 5,
            },
        )
    return _engine


def get_pool():
    global _pool
    if _pool is None:
        if not DATABASE_URL:
            raise DatabaseNotConfigured(
                "DATABASE_URL is not set. Copy .env.example to .env (locally) "
                "or set it as an Elastic Beanstalk environment property "
                "(production), then restart the app."
            )
        _pool = psycopg2.pool.SimpleConnectionPool(
            minconn=1, maxconn=10, dsn=DATABASE_URL,
            cursor_factory=psycopg2.extras.RealDictCursor,
            options=f"-c search_path={SEARCH_PATH} -c statement_timeout={STATEMENT_TIMEOUT_MS}",
            connect_timeout=5,
        )
    return _pool


def get_conn():
    conn = get_pool().getconn()
    conn.autocommit = True  # each app query commits itself, no manual .commit() needed
    return conn


def put_conn(conn):
    if _pool is not None:
        _pool.putconn(conn)


def init_db():
    """Idempotent safety net - creates tables if they don't exist yet.
    Prefer running schema.sql yourself with a privileged DB user; this is
    just a fallback so a fresh environment doesn't hard-fail on first run.
    Never lets a connection failure crash app startup - a bad security
    group or wrong DATABASE_URL should show a clear per-request error
    (see app.py's error handler), not take the whole app down."""
    try:
        conn = get_conn()
    except DatabaseNotConfigured as e:
        print(f"[db] Skipping init_db(): {e}")
        return
    except Exception as e:
        print(f"[db] init_db() couldn't connect ({e}) - continuing without it. "
              f"Check DATABASE_URL, DB_SCHEMA, and that the security group allows "
              f"this instance to reach RDS on port 5432.")
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS lead_status (
                    company     TEXT PRIMARY KEY,
                    status      TEXT NOT NULL DEFAULT 'New',
                    notes       TEXT NOT NULL DEFAULT '',
                    updated_at  TIMESTAMPTZ
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS saved_views (
                    id            SERIAL PRIMARY KEY,
                    name          TEXT NOT NULL,
                    query_string  TEXT NOT NULL,
                    created_at    TIMESTAMPTZ DEFAULT now()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS saved_charts (
                    id          SERIAL PRIMARY KEY,
                    name        TEXT NOT NULL,
                    dataset     TEXT NOT NULL,
                    group_by    TEXT NOT NULL,
                    metric      TEXT NOT NULL,
                    chart_type  TEXT NOT NULL DEFAULT 'bar',
                    created_at  TIMESTAMPTZ DEFAULT now()
                )
            """)
    except Exception as e:
        print(f"[db] init_db() couldn't create tables (this is fine if you "
              f"already ran schema.sql yourself): {e}")
    finally:
        put_conn(conn)
