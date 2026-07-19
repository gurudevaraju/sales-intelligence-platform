"""Sanity-check DAG: prove Airflow can connect to S3 and read real data.

Four clear steps, logged loudly so the task log tells the whole story:

    1. CONNECT  -- open an S3 client and HEAD the target object (no data yet)
    2. READ     -- fetch the object and parse it with pandas
    3. PREVIEW  -- print the schema + first 10 records
    4. SUCCESS  -- confirm the connection + read path both work end to end

Defaults to reading ``cssip/company_leads.csv`` (the output of the
``04_etl_company_leads`` pipeline) so this doubles as a live demo of the
exact object the main ETL DAGs consume. Override via the ``s3_key`` Airflow
Variable if you want to point it at a different file. Does NOT touch RDS.
"""

from __future__ import annotations

import io
from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


default_args = {
    "owner": "you",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

LINE = "=" * 100


def _banner(step: str, title: str) -> None:
    print()
    print(LINE)
    print(f"  {step}  --  {title}")
    print(LINE)


@dag(
    dag_id="00_check_s3_connection",
    description="Connect to S3, read company_leads.csv, print 10 rows, confirm success",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["debug", "s3"],
)
def check_s3_connection():
    @task
    def check() -> dict:
        bucket = Variable.get("s3_bucket")
        key = Variable.get("s3_key", default_var="cssip/company_leads.csv")
        preview_rows = 10
        fetch_bytes = 1_048_576  # 1 MB is plenty to contain the first 10 CSV rows

        # --------------------------------------------------------------
        # STEP 1/4: CONNECT -- open the S3 client, locate the object
        # --------------------------------------------------------------
        _banner("STEP 1/4", f"CONNECT  --  s3://{bucket}/{key}")
        s3 = S3Hook(aws_conn_id="aws_default")
        client = s3.get_conn()
        print(f"Target object : s3://{bucket}/{key}")

        head = client.head_object(Bucket=bucket, Key=key)
        content_length = head["ContentLength"]
        print("OK  --  connected to S3 and located the object.")
        print(f"  Size          : {content_length:,} bytes ({content_length / 1024 / 1024:.2f} MB)")
        print(f"  Last modified : {head['LastModified']}")
        print(f"  ETag          : {head['ETag']}")

        # --------------------------------------------------------------
        # STEP 2/4: READ -- fetch the object and parse it
        # --------------------------------------------------------------
        _banner("STEP 2/4", "READ  --  fetch + parse the CSV")
        upper = min(fetch_bytes, content_length) - 1
        resp = client.get_object(Bucket=bucket, Key=key, Range=f"bytes=0-{upper}")
        body = resp["Body"].read()
        print(f"Fetched {len(body):,} bytes (first {preview_rows} rows are inside this range)")

        df = pd.read_csv(io.BytesIO(body), nrows=preview_rows)
        print(f"OK  --  parsed {len(df)} row(s), {len(df.columns)} column(s).")

        # --------------------------------------------------------------
        # STEP 3/4: PREVIEW -- schema + first 10 records
        # --------------------------------------------------------------
        _banner("STEP 3/4", f"PREVIEW  --  first {len(df)} record(s)")
        print("Columns and dtypes:")
        for col, dtype in df.dtypes.items():
            print(f"  - {col}: {dtype}")
        print()
        print("Records:")
        with pd.option_context(
            "display.max_columns", None,
            "display.width", 200,
            "display.max_colwidth", 40,
        ):
            print(df.to_string(index=False))

        # --------------------------------------------------------------
        # STEP 4/4: SUCCESS
        # --------------------------------------------------------------
        _banner("STEP 4/4", "CONNECTION SUCCESSFUL")
        print(f"S3 connection OK. Read {len(df)} sample row(s) from s3://{bucket}/{key}.")

        return {
            "bucket": bucket,
            "key": key,
            "content_length_bytes": content_length,
            "preview_row_count": int(len(df)),
            "column_count": int(len(df.columns)),
            "columns": [str(c) for c in df.columns],
        }

    check()


check_s3_connection()
