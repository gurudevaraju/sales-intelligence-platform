"""Airflow ETL DAG: s3://<bucket>/cssip/vulnerable_hosts.csv -> cyber.vulnerable_hosts.

Self-contained pipeline (no imports from other files in this repo). Logs
tell the same 6-step story every run:

    1. READ      -- download the CSV from S3 to /tmp
    2. SCHEMA    -- chunked read over ALL rows; infer a Postgres schema
    3. TRANSFORM -- log the selected columns; add a derived ``ingested_at``
                    audit column (load timestamp)
    4. PREVIEW   -- print 10 sample records of the transformed data
    5. LOAD      -- overwrite cyber.vulnerable_hosts (create-if-missing +
                    TRUNCATE + COPY; table/index objects are never dropped,
                    so any indexes you add manually survive every run)
    6. COMPLETE  -- verify row count, print an ingestion-complete summary

Configuration:
    Connections: aws_default, postgres_rds
    Variables:   s3_bucket (required), target_schema (optional, default 'cyber')
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


TABLE_NAME = "vulnerable_hosts"
S3_KEY = "cssip/vulnerable_hosts.csv"
TARGET_SCHEMA_DEFAULT = "cyber"
CSV_CHUNK_SIZE = 200_000

DEFAULT_ARGS = {
    "owner": "you",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _banner(step: str, title: str) -> None:
    line = "=" * 70
    print()
    print(line)
    print(f"  {step}  --  {title}")
    print(line)


def _human_bytes(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:,.0f} {unit}" if unit == "B" else f"{n:,.2f} {unit}"
        n /= 1024
    return f"{n} B"


def _pg_type(dtype: object) -> str:
    """Map a pandas dtype to a Postgres column type."""
    s = str(dtype)
    if s.startswith("int"):
        return "BIGINT"
    if s.startswith("float"):
        return "DOUBLE PRECISION"
    if s == "bool":
        return "BOOLEAN"
    return "TEXT"


def _coalesce_pg_type(existing: str | None, incoming: str) -> str:
    """Combine per-chunk inferred Postgres types conservatively.

    Rules:
      - First observation wins if no conflict.
      - Any TEXT beats everything (TEXT is the safe superset).
      - BIGINT + DOUBLE PRECISION -> DOUBLE PRECISION.
      - Any other mixed combination -> TEXT (safe fallback).
    """
    if existing is None or existing == incoming:
        return incoming
    if "TEXT" in (existing, incoming):
        return "TEXT"
    if {existing, incoming} == {"BIGINT", "DOUBLE PRECISION"}:
        return "DOUBLE PRECISION"
    return "TEXT"


@dag(
    dag_id="01_etl_vulnerable_hosts",
    description=f"S3 -> RDS ETL: {S3_KEY} -> {TARGET_SCHEMA_DEFAULT}.{TABLE_NAME}",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["etl", "aws", "s3", "rds", "cyber", TABLE_NAME],
)
def s3_to_rds_etl_vulnerable_hosts():
    @task
    def extract() -> str:
        """STEP 1/6: read the raw CSV from S3 down to /tmp."""
        bucket = Variable.get("s3_bucket")
        key = S3_KEY

        _banner("STEP 1/6", f"READ  --  s3://{bucket}/{key}")
        print(f"Reading data file: s3://{bucket}/{key}")

        s3 = S3Hook(aws_conn_id="aws_default")
        head = s3.get_conn().head_object(Bucket=bucket, Key=key)
        remote_size = head["ContentLength"]
        print(f"Object size: {_human_bytes(remote_size)}  ({remote_size:,} bytes)")
        print(f"Modified   : {head['LastModified']}")

        print("Downloading to /tmp ...")
        local_path = s3.download_file(
            key=key,
            bucket_name=bucket,
            local_path="/tmp",
            preserve_file_name=True,
        )
        local_size = os.path.getsize(local_path)
        print(f"Downloaded : {local_path}")
        print(f"Local size : {_human_bytes(local_size)}  ({local_size:,} bytes)")
        print("OK  --  data successfully read from S3.")
        return local_path

    @task
    def transform(input_path: str) -> dict[str, Any]:
        """STEP 2/6 (schema) + STEP 3/6 (transform) + STEP 4/6 (preview)."""
        # ---- STEP 2/6: SCHEMA -- chunked read + type inference -------
        _banner("STEP 2/6", f"SCHEMA  --  inspecting {input_path}")
        print(f"Chunk size : {CSV_CHUNK_SIZE:,} rows (streamed so GB-sized files don't OOM)")

        t0 = time.perf_counter()
        combined: dict[str, str] = {}
        total_rows = 0
        preview_df: pd.DataFrame | None = None

        for chunk in pd.read_csv(
            input_path, chunksize=CSV_CHUNK_SIZE, low_memory=False
        ):
            if preview_df is None:
                preview_df = chunk.head(10).copy()
            total_rows += len(chunk)
            for col in chunk.columns:
                combined[col] = _coalesce_pg_type(
                    combined.get(col), _pg_type(chunk[col].dtype)
                )
        elapsed = time.perf_counter() - t0

        source_schema = list(combined.items())
        source_columns = [c for c, _ in source_schema]
        file_size = os.path.getsize(input_path)

        print(f"File size  : {_human_bytes(file_size)}")
        print(f"Total rows : {total_rows:,} (exact, streamed)")
        print(f"Columns    : {len(source_columns)}")
        print(f"Parse time : {elapsed:.1f}s")
        print()
        print("Inferred schema:")
        for i, (c, t) in enumerate(source_schema, 1):
            print(f"  {i:2d}. {c:35s} {t}")

        # ---- STEP 3/6: TRANSFORM -- select columns + audit column ----
        _banner("STEP 3/6", "TRANSFORM  --  select columns for RDS")
        print(f"Selected columns ({len(source_columns)} of {len(source_columns)}, all kept):")
        print(f"  {', '.join(source_columns)}")
        ingested_at = datetime.now(timezone.utc)
        print()
        print("Derived column added:")
        print(f"  ingested_at  TIMESTAMPTZ  -- load time for this run = {ingested_at.isoformat()}")
        load_schema = source_schema + [("ingested_at", "TIMESTAMPTZ")]

        # ---- STEP 4/6: PREVIEW -- 10 records of the transformed data --
        if preview_df is not None:
            preview_df["ingested_at"] = ingested_at
            preview_cols = (source_columns[:9] + ["ingested_at"])
            pv = preview_df[preview_cols].copy()
            for c in pv.columns:
                if pv[c].dtype == object:
                    pv[c] = pv[c].astype(str).str.slice(0, 22)
            _banner("STEP 4/6", f"PREVIEW  --  first {len(pv)} record(s) after transform")
            print(
                f"Showing {len(preview_cols)} of {len(load_schema)} columns "
                "(long text values are truncated for display only):"
            )
            print("-" * 100)
            with pd.option_context(
                "display.max_columns", None,
                "display.width", 200,
                "display.max_colwidth", 24,
            ):
                print(pv.to_string(index=False))
            print("-" * 100)

        print()
        print(
            f"OK  --  {total_rows:,} rows ready for load, "
            f"{len(load_schema)} columns (incl. ingested_at)."
        )
        return {
            "path": input_path,
            "source_schema": source_schema,
            "load_schema": load_schema,
            "row_count": total_rows,
            "ingested_at": ingested_at.isoformat(),
        }

    @task
    def load(payload: dict) -> int:
        """STEP 5/6: overwrite the RDS table. STEP 6/6: confirm completion."""
        input_path = payload["path"]
        source_cols = payload["source_schema"]
        load_schema = payload["load_schema"]
        expected_rows = payload["row_count"]
        ingested_at = payload["ingested_at"]

        target_schema = Variable.get(
            "target_schema", default_var=TARGET_SCHEMA_DEFAULT
        )
        target_table = TABLE_NAME
        fq_table = f'"{target_schema}"."{target_table}"'

        hook = PostgresHook(postgres_conn_id="postgres_rds")
        engine = hook.get_sqlalchemy_engine()

        # ---- STEP 5/6: LOAD -- overwrite target table via COPY -------
        _banner("STEP 5/6", f"LOAD  --  {target_schema}.{target_table} (overwrite)")
        print(f"Input file : {input_path}")
        print(f"Target     : {target_schema}.{target_table}")
        print(f"Expected   : {expected_rows:,} rows, {len(load_schema)} columns")

        source_col_defs = ",\n            ".join(
            f'"{c}" {t}' for c, t in source_cols
        )
        full_col_defs = (
            f"{source_col_defs},\n            "
            f"\"ingested_at\" TIMESTAMPTZ NOT NULL DEFAULT '{ingested_at}'::timestamptz"
        )
        source_col_list = ", ".join(f'"{c}"' for c, _ in source_cols)

        print(f"\n[1/4] Ensuring schema '{target_schema}' exists...")
        with engine.begin() as conn:
            conn.exec_driver_sql(
                f'CREATE SCHEMA IF NOT EXISTS "{target_schema}";'
            )
        print(f"      schema '{target_schema}' ready.")

        print(
            f"[2/4] Overwriting {target_schema}.{target_table} "
            "(create-if-missing + truncate, so any indexes you added stay intact)..."
        )
        with engine.begin() as conn:
            conn.exec_driver_sql(
                f'CREATE TABLE IF NOT EXISTS {fq_table} (\n            {full_col_defs}\n        );'
            )
            conn.exec_driver_sql(f'TRUNCATE TABLE {fq_table};')
        print(f"      {target_schema}.{target_table} ready ({len(load_schema)} columns).")

        print("[3/4] Streaming CSV to Postgres via COPY FROM STDIN...")
        print("      (ingested_at is not in the CSV; every row gets its column DEFAULT)")
        t0 = time.perf_counter()
        hook.copy_expert(
            sql=(
                f'COPY {fq_table} ({source_col_list}) FROM STDIN WITH '
                f"(FORMAT CSV, HEADER TRUE)"
            ),
            filename=input_path,
        )
        elapsed = time.perf_counter() - t0
        print(f"      COPY finished in {elapsed:.1f}s.")

        print("[4/4] Verifying row count in RDS...")
        with engine.begin() as conn:
            row_count = int(
                conn.exec_driver_sql(
                    f'SELECT COUNT(*) FROM {fq_table};'
                ).scalar()
            )
        print(f"      Row count in {target_schema}.{target_table} = {row_count:,}")
        if row_count == expected_rows:
            print(f"      OK  --  matches transform's count ({expected_rows:,}).")
        else:
            print(
                f"      WARN -- transform saw {expected_rows:,} rows, "
                f"but table has {row_count:,}. Investigate quoted-newline rows "
                "or CSV parsing differences."
            )

        # ---- STEP 6/6: COMPLETE ---------------------------------------
        _banner("STEP 6/6", "INGESTION COMPLETE")
        print(f"Loaded {row_count:,} rows into {target_schema}.{target_table}")
        print(f"  columns       : {len(load_schema)} (incl. ingested_at)")
        print(f"  COPY duration : {elapsed:.1f}s")
        print(f"  ingested_at   : {ingested_at}")
        return row_count

    load(transform(extract()))


s3_to_rds_etl_vulnerable_hosts()
