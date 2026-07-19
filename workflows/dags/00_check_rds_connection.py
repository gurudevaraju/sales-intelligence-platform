"""Sanity-check DAG: prove Airflow can reach, log in to, and read from RDS Postgres.

Mirrors the same 4-step story as ``00_check_s3_connection``, so both debug DAGs read
the same way in the logs:

    1. CONNECT  -- DNS -> TCP -> Postgres login (ladder, pinpoints failures)
    2. READ     -- list tables, then SELECT 10 sample rows from real data
                   (falls back to a clear "no data yet" message on a fresh DB)
    3. WRITE    -- create / insert / select / drop a scratch table
    4. SUCCESS  -- confirm the connection is fully usable end to end

Does NOT touch S3 or perform any ETL. Trigger manually.
"""

from __future__ import annotations

import ipaddress
import socket
from datetime import datetime

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    "owner": "you",
    "retries": 0,
}

LINE = "=" * 100


def _banner(step: str, title: str) -> None:
    print()
    print(LINE)
    print(f"  {step}  --  {title}")
    print(LINE)


@dag(
    dag_id="00_check_rds_connection",
    description="Connect to RDS, read sample rows, confirm write access, confirm success",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["debug", "rds", "postgres"],
)
def check_rds_connection():
    @task
    def check() -> dict:
        conn_id = "postgres_rds"
        result: dict = {}

        c = BaseHook.get_connection(conn_id)
        host = c.host
        port = int(c.port or 5432)
        db = c.schema
        user = c.login

        # ------------------------------------------------------------------
        # STEP 1/4: CONNECT -- DNS -> TCP -> Postgres login
        # ------------------------------------------------------------------
        _banner("STEP 1/4", f"CONNECT  --  {conn_id} ({host}:{port}/{db})")
        print(f"  conn_id:  {conn_id}")
        print(f"  host:     {host}")
        print(f"  port:     {port}")
        print(f"  database: {db}")
        print(f"  user:     {user}")

        print("\n[1a] DNS resolution...")
        ip = socket.gethostbyname(host)
        addr = ipaddress.ip_address(ip)
        print(f"     {host} -> {ip}  (private={addr.is_private})")
        result["dns_ip"] = ip
        if addr.is_private:
            raise RuntimeError(
                f"DNS resolves to PRIVATE IP {ip}. RDS is not publicly accessible.\n"
                "Fix: RDS Console -> Modify -> Public access -> Publicly accessible."
            )

        print(f"\n[1b] TCP handshake to {host}:{port} (5s timeout)...")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        try:
            sock.connect((host, port))
            print("     OK -- socket connected.")
        except socket.timeout as e:
            print("     FAILED: timed out (Security Group is likely blocking inbound 5432).")
            raise RuntimeError("TCP handshake timed out") from e
        except ConnectionRefusedError as e:
            print("     FAILED: connection refused.")
            raise RuntimeError("TCP handshake refused") from e
        finally:
            sock.close()

        print("\n[1c] Postgres login via PostgresHook...")
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute("SELECT current_database(), current_user, version();")
        cur_db, cur_user, ver = cur.fetchone()
        print(f"     OK -- authenticated as '{cur_user}' on database '{cur_db}'.")
        print(f"     server version: {ver.split(' on ')[0]}")
        result["current_database"] = cur_db
        result["current_user"] = cur_user

        # ------------------------------------------------------------------
        # STEP 2/4: READ -- list tables, then sample real data if present
        # ------------------------------------------------------------------
        target_schema = Variable.get("target_schema", default_var="cyber")
        _banner("STEP 2/4", f"READ  --  tables in '{target_schema}' schema")
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_name;
            """,
            (target_schema,),
        )
        tables = [r[0] for r in cur.fetchall()]
        result["tables"] = tables

        if not tables:
            print(f"  (no tables in '{target_schema}' yet -- run the ETL DAGs first)")
        else:
            print(f"  Found {len(tables)} table(s): {', '.join(tables)}")
            sample_table = "company_leads" if "company_leads" in tables else tables[0]
            fq_table = f'"{target_schema}"."{sample_table}"'
            print(f"\n  Reading 10 sample record(s) from {target_schema}.{sample_table} ...")
            cur.execute(f"SELECT * FROM {fq_table} LIMIT 10;")
            rows = cur.fetchall()
            col_names = [d[0] for d in cur.description]
            print(f"  Columns ({len(col_names)}): {', '.join(col_names[:10])}"
                  + ("..." if len(col_names) > 10 else ""))
            print(f"  Records ({len(rows)}):")
            for i, row in enumerate(rows, 1):
                preview = ", ".join(str(v)[:20] for v in row[:6])
                print(f"    {i:2d}. {preview}")
            result["sample_table"] = sample_table
            result["sample_row_count"] = len(rows)

        # ------------------------------------------------------------------
        # STEP 3/4: WRITE -- create / insert / select / drop scratch table
        # ------------------------------------------------------------------
        _banner("STEP 3/4", "WRITE  --  create / insert / select / drop scratch table")
        try:
            cur.execute("CREATE TABLE IF NOT EXISTS _airflow_smoke_test (id int, msg text);")
            cur.execute("INSERT INTO _airflow_smoke_test VALUES (1, 'hello from airflow');")
            cur.execute("SELECT COUNT(*) FROM _airflow_smoke_test;")
            n = cur.fetchone()[0]
            cur.execute("DROP TABLE _airflow_smoke_test;")
            conn.commit()
            print(f"  OK -- create/insert/select/drop cycle succeeded ({n} row written, then cleaned up).")
            result["write_ok"] = True
        except Exception as e:
            conn.rollback()
            print(f"  FAILED: {type(e).__name__}: {e}")
            result["write_ok"] = False
            raise
        finally:
            cur.close()
            conn.close()

        # ------------------------------------------------------------------
        # STEP 4/4: SUCCESS
        # ------------------------------------------------------------------
        _banner("STEP 4/4", "CONNECTION SUCCESSFUL")
        print("RDS connection OK -- read and write both confirmed working.")
        return result

    check()


check_rds_connection()
