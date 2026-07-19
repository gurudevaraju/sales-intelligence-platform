"""
Data access layer - live RDS Postgres.

IMPORTANT: filtering, sorting, and pagination happen in SQL (WHERE / ORDER
BY / LIMIT / OFFSET), not in pandas after loading a full table. At real
scale (hosts_scored, vulnerable_hosts, vulnerable_cves can be hundreds of
MB to multiple GB), pulling a whole table into memory on every cold cache
is the single biggest performance problem an app like this can have - an
index can't help a query with no WHERE clause, since that's always a full
scan regardless. Every function below either filters with a WHERE company=
/ _company= lookup (fast with the indexes in indexes.sql), or pushes
GROUP BY / COUNT / LIMIT aggregation down to Postgres and only pulls back
the small result.

company_leads is the one table small enough (~10MB) that loading it whole
and working with it in pandas is genuinely fine - that's what
load_company_leads() is for, used by the Reports chart builder's "leads"
dataset.
"""
from collections import Counter

import pandas as pd
from sqlalchemy import text

import db

# ---------------------------------------------------------------------
# In-memory caches - small, safe to hold for the process lifetime.
# Call reload_all() after your ETL/Airflow pipeline refreshes the tables.
# ---------------------------------------------------------------------

_country_map_cache = None
_company_leads_cache = None
_filter_options_cache = None


def reload_all():
    global _country_map_cache, _company_leads_cache, _filter_options_cache
    _country_map_cache = None
    _company_leads_cache = None
    _filter_options_cache = None


def _engine():
    return db.get_engine()


def _in_clause(conditions, params, column, values, prefix):
    """Builds a parameterized `column IN (:p0, :p1, ...)` clause - safe
    from injection since only parameter names are generated, never values."""
    names = []
    for i, v in enumerate(values):
        key = f"{prefix}{i}"
        names.append(f":{key}")
        params[key] = v
    conditions.append(f"{column} IN ({', '.join(names)})")


def _country_map():
    """company -> most common country, derived from hosts_scored. Computed
    entirely in SQL (GROUP BY + DISTINCT ON) so Postgres does the
    aggregation with its indexes, instead of pulling raw rows and doing a
    per-group .mode() in pandas - that was slow enough on real data to be
    a real source of timeouts. Result is one row per company - small,
    cached after first computation."""
    global _country_map_cache
    if _country_map_cache is None:
        query = text("""
            SELECT DISTINCT ON (_company) _company AS company, country
            FROM (
                SELECT _company, country, COUNT(*) AS cnt
                FROM hosts_scored
                WHERE _company IS NOT NULL AND country IS NOT NULL
                GROUP BY _company, country
            ) t
            ORDER BY _company, cnt DESC
        """)
        with _engine().connect() as conn:
            rows = conn.execute(query).all()
        _country_map_cache = {r[0]: r[1] for r in rows}
    return _country_map_cache


def _top_countries(n=8):
    cmap = _country_map()
    counts = Counter(v for v in cmap.values() if v)
    return dict(counts.most_common(n))


def _companies_with_hosts_matching(column, values):
    """Distinct companies with at least one host matching column IN values -
    a small, indexed lookup against hosts_scored, not a full load."""
    if not values:
        return []
    conditions, params = [], {}
    _in_clause(conditions, params, column, values, "v")
    query = text(f"""
        SELECT DISTINCT _company FROM hosts_scored
        WHERE {conditions[0]} AND _company IS NOT NULL
    """)
    with _engine().connect() as conn:
        rows = conn.execute(query, params).scalars().all()
    return list(rows)


def load_company_leads():
    """company_leads is small (~10MB) - loading it fully is fine."""
    global _company_leads_cache
    if _company_leads_cache is None:
        with _engine().connect() as conn:
            df = pd.read_sql(text("SELECT * FROM company_leads"), conn)
        if "is_provider" in df.columns:
            df["is_provider"] = df["is_provider"].astype(bool)
        _company_leads_cache = df
    return _company_leads_cache.copy()


# ---------------------------------------------------------------------
# Filter option helpers (dropdowns) - targeted DISTINCT/GROUP BY queries,
# cached after first computation.
# ---------------------------------------------------------------------

def get_filter_options():
    global _filter_options_cache
    if _filter_options_cache is not None:
        return _filter_options_cache

    with _engine().connect() as conn:
        grades = [r[0] for r in conn.execute(text(
            "SELECT DISTINCT lead_grade FROM company_leads "
            "WHERE lead_grade IS NOT NULL ORDER BY lead_grade"))]
        countries = [r[0] for r in conn.execute(text(
            "SELECT DISTINCT country FROM hosts_scored "
            "WHERE country IS NOT NULL ORDER BY country"))]
        products = [r[0] for r in conn.execute(text(
            "SELECT DISTINCT product FROM hosts_scored "
            "WHERE product IS NOT NULL ORDER BY product"))]
        severities_present = {r[0] for r in conn.execute(text(
            "SELECT DISTINCT _vuln_severity FROM hosts_scored "
            "WHERE _vuln_severity IN ('critical','high','medium','low')"))}
        ports = [r[0] for r in conn.execute(text(
            "SELECT port FROM hosts_scored WHERE port IS NOT NULL "
            "GROUP BY port ORDER BY COUNT(*) DESC LIMIT 40"))]
        maxes = conn.execute(text(
            "SELECT MAX(lead_score), MAX(n_hosts), MAX(n_cves) FROM company_leads")).first()

    _filter_options_cache = {
        "grades": grades,
        "countries": countries,
        "industries": [],  # no _industry column in the real schema
        "products": products,
        "severities": [s for s in ["critical", "high", "medium", "low"] if s in severities_present],
        "ports": sorted(ports, key=int),
        "score_max": float(maxes[0]) if maxes and maxes[0] is not None else 100,
        "hosts_max": int(maxes[1]) if maxes and maxes[1] is not None else 100,
        "cves_max": int(maxes[2]) if maxes and maxes[2] is not None else 100,
    }
    return _filter_options_cache


# ---------------------------------------------------------------------
# company_leads - filter / sort / paginate in SQL
# ---------------------------------------------------------------------

ALLOWED_LEAD_SORT = {
    "company", "domain", "lead_grade", "lead_score", "n_hosts", "n_cves",
    "n_verified_cves", "n_critical", "n_high", "max_cvss", "max_epss",
    "avg_confidence", "is_provider",
}


def filter_leads(args, page=1, page_size=50, include_country=False):
    """Returns (page_df, total_count). page_size=None fetches every
    matching row with no LIMIT - used for CSV export. include_country
    attaches a country column (used only by CSV export - the tables don't
    display it, so the default page-load path skips this entirely)."""

    def getlist(name):
        if hasattr(args, "getlist"):
            return args.getlist(name)
        v = args.get(name)
        return [v] if v else []

    def _num(name):
        v = args.get(name)
        try:
            return float(v) if v not in (None, "") else None
        except (TypeError, ValueError):
            return None

    conditions, params = [], {}

    q = (args.get("q") or "").strip()
    if q:
        conditions.append("(company ILIKE :q OR domain ILIKE :q)")
        params["q"] = f"%{q}%"

    grades = getlist("grade")
    if grades:
        _in_clause(conditions, params, "lead_grade", grades, "grade")

    numeric_filters = [
        ("score_min", "lead_score", ">="), ("score_max", "lead_score", "<="),
        ("hosts_min", "n_hosts", ">="), ("hosts_max", "n_hosts", "<="),
        ("cves_min", "n_cves", ">="),
        ("critical_min", "n_critical", ">="),
        ("cvss_min", "max_cvss", ">="),
        ("epss_min", "max_epss", ">="),
        ("confidence_min", "avg_confidence", ">="),
    ]
    for i, (arg_name, column, op) in enumerate(numeric_filters):
        v = _num(arg_name)
        if v is not None:
            key = f"n{i}"
            conditions.append(f"{column} {op} :{key}")
            params[key] = v

    if args.get("exclude_providers") in ("1", "true", "on", True):
        conditions.append("is_provider = false")

    countries = getlist("country")
    if countries:
        cmap = _country_map()
        matching = [c for c, ctry in cmap.items() if ctry in countries] or ["__none__"]
        _in_clause(conditions, params, "company", matching, "ctry")

    products = getlist("product")
    if products:
        matching = _companies_with_hosts_matching("product", products) or ["__none__"]
        _in_clause(conditions, params, "company", matching, "prod")

    severities = getlist("severity")
    if severities:
        matching = _companies_with_hosts_matching("_vuln_severity", severities) or ["__none__"]
        _in_clause(conditions, params, "company", matching, "sev")

    where_sql = " AND ".join(conditions) if conditions else "TRUE"

    sort = args.get("sort", "lead_score")
    if sort not in ALLOWED_LEAD_SORT:
        sort = "lead_score"
    order = "ASC" if args.get("order") == "asc" else "DESC"

    with _engine().connect() as conn:
        total = conn.execute(text(f"SELECT COUNT(*) FROM company_leads WHERE {where_sql}"), params).scalar()

        data_sql = f"SELECT * FROM company_leads WHERE {where_sql} ORDER BY {sort} {order}"
        query_params = dict(params)
        if page_size is not None:
            data_sql += " LIMIT :limit OFFSET :offset"
            query_params["limit"] = page_size
            query_params["offset"] = max(0, (page - 1) * page_size)
        df = pd.read_sql(text(data_sql), conn, params=query_params)

    if "is_provider" in df.columns:
        df["is_provider"] = df["is_provider"].astype(bool)
    if include_country:
        cmap = _country_map()
        df["country"] = df["company"].map(cmap)

    return df, int(total or 0)


def get_company_detail(company_name):
    """Targeted WHERE company= / _company= lookups - fast regardless of
    total table size once the indexes in indexes.sql are in place."""
    with _engine().connect() as conn:
        company_df = pd.read_sql(text("SELECT * FROM company_leads WHERE company = :c"),
                                  conn, params={"c": company_name})
        if company_df.empty:
            return None
        if "is_provider" in company_df.columns:
            company_df["is_provider"] = company_df["is_provider"].astype(bool)
        company = company_df.iloc[0].to_dict()

        hosts_df = pd.read_sql(text("SELECT * FROM hosts_scored WHERE _company = :c"),
                                conn, params={"c": company_name})
        cves_df = pd.read_sql(text(
            "SELECT * FROM vulnerable_cves WHERE _company = :c ORDER BY cvss DESC"),
            conn, params={"c": company_name})

    return {
        "company": company,
        "hosts": hosts_df.to_dict(orient="records"),
        "cves": cves_df.to_dict(orient="records"),
    }


# ---------------------------------------------------------------------
# vulnerable_hosts - filter / sort / paginate in SQL
# ---------------------------------------------------------------------

ALLOWED_HOST_SORT = {
    "_company", "ip_str", "port", "product", "version",
    "_vuln_severity", "_vuln_max_cvss", "country", "timestamp",
}

COUNT_CAP = 50_000  # never fully count past this - report "50,000+" instead
EXPORT_ROW_CAP = 25_000  # CSV export ceiling for the large tables


def _bounded_count(conn, table, where_sql, params, cap=COUNT_CAP):
    """COUNT(*) is a full scan proportional to matches - fine for a
    filtered result, expensive for an unfiltered scan over a huge table.
    Cap the counting work itself so a broad/no-filter query can't run
    an unbounded scan just to render '205,278 results'."""
    total = conn.execute(text(
        f"SELECT COUNT(*) FROM (SELECT 1 FROM {table} WHERE {where_sql} LIMIT :cap) t"
    ), dict(params, cap=cap)).scalar()
    return int(total or 0), (total or 0) >= cap


def filter_vulnerable_hosts(args, page=1, page_size=50):
    """Returns (page_df, total_count, is_capped). page_size=None fetches
    every matching row up to EXPORT_ROW_CAP - used for CSV export."""

    def getlist(name):
        if hasattr(args, "getlist"):
            return args.getlist(name)
        v = args.get(name)
        return [v] if v else []

    conditions, params = [], {}

    q = (args.get("q") or "").strip()
    if q:
        conditions.append("(_company ILIKE :q OR ip_str ILIKE :q OR product ILIKE :q)")
        params["q"] = f"%{q}%"

    severities = getlist("severity")
    if severities:
        _in_clause(conditions, params, "_vuln_severity", severities, "sev")

    products = getlist("product")
    if products:
        _in_clause(conditions, params, "product", products, "prod")

    countries = getlist("country")
    if countries:
        _in_clause(conditions, params, "country", countries, "ctry")

    port = args.get("port")
    if port:
        conditions.append("port = :port")
        params["port"] = int(port)

    where_sql = " AND ".join(conditions) if conditions else "TRUE"

    sort = args.get("sort", "_vuln_max_cvss")
    if sort not in ALLOWED_HOST_SORT:
        sort = "_vuln_max_cvss"
    order = "ASC" if args.get("order") == "asc" else "DESC"

    with _engine().connect() as conn:
        total, is_capped = _bounded_count(conn, "vulnerable_hosts", where_sql, params)

        data_sql = f"SELECT * FROM vulnerable_hosts WHERE {where_sql} ORDER BY {sort} {order}"
        query_params = dict(params)
        if page_size is not None:
            data_sql += " LIMIT :limit OFFSET :offset"
            query_params["limit"] = page_size
            query_params["offset"] = max(0, (page - 1) * page_size)
        else:
            data_sql += " LIMIT :export_cap"
            query_params["export_cap"] = EXPORT_ROW_CAP
        df = pd.read_sql(text(data_sql), conn, params=query_params)

    return df, total, is_capped


def vulnerable_hosts_summary():
    """Aggregate cards for the top of Vulnerable Assets - severity mix, top
    products, top companies. All bounded GROUP BY queries, never a scan of
    the full result set."""
    with _engine().connect() as conn:
        total, is_capped = _bounded_count(conn, "vulnerable_hosts", "TRUE", {})

        severity_rows = conn.execute(text("""
            SELECT _vuln_severity, COUNT(*) FROM vulnerable_hosts
            WHERE _vuln_severity IN ('critical','high','medium','low')
            GROUP BY _vuln_severity
        """)).all()

        product_rows = conn.execute(text("""
            SELECT product, COUNT(*) AS n FROM vulnerable_hosts
            WHERE product IS NOT NULL
            GROUP BY product ORDER BY n DESC LIMIT 8
        """)).all()

        company_rows = conn.execute(text("""
            SELECT _company, COUNT(*) AS n FROM vulnerable_hosts
            WHERE _company IS NOT NULL
            GROUP BY _company ORDER BY n DESC LIMIT 8
        """)).all()

    return {
        "total": total,
        "total_capped": is_capped,
        "severity_counts": {s: int(n) for s, n in severity_rows},
        "top_products": [{"label": p, "value": int(n)} for p, n in product_rows],
        "top_companies": [{"label": c, "value": int(n)} for c, n in company_rows],
    }


def get_host_cves(company_name, ip_str, port):
    """CVEs for one specific host - narrowed by the indexed _company column
    first, so this stays fast regardless of vulnerable_cves' total size."""
    query = text("""
        SELECT * FROM vulnerable_cves
        WHERE _company = :c AND ip_str = :ip AND port = :port
        ORDER BY cvss DESC
    """)
    with _engine().connect() as conn:
        df = pd.read_sql(query, conn, params={"c": company_name, "ip": ip_str, "port": int(port)})
    return df.to_dict(orient="records")


# ---------------------------------------------------------------------
# Dashboard - all SQL aggregates, never a full table load
# ---------------------------------------------------------------------

def dashboard_stats():
    with _engine().connect() as conn:
        totals = conn.execute(text("""
            SELECT COUNT(*) AS total_companies,
                   COALESCE(SUM(n_hosts), 0) AS total_hosts,
                   COALESCE(SUM(n_critical), 0) AS total_critical,
                   COALESCE(SUM(n_high), 0) AS total_high,
                   AVG(lead_score) AS avg_score,
                   COUNT(*) FILTER (WHERE lead_grade = 'A') AS hot_leads
            FROM company_leads
        """)).mappings().first()

        grade_rows = conn.execute(text(
            "SELECT lead_grade, COUNT(*) FROM company_leads GROUP BY lead_grade")).all()

        severity_rows = conn.execute(text("""
            SELECT _vuln_severity, COUNT(*) FROM hosts_scored
            WHERE _vuln_severity IN ('critical','high','medium','low')
            GROUP BY _vuln_severity
        """)).all()

        product_rows = conn.execute(text("""
            SELECT product, COUNT(*) AS n FROM hosts_scored
            WHERE _vuln_bucket != 'none'
            GROUP BY product ORDER BY n DESC LIMIT 8
        """)).all()

        top_leads_df = pd.read_sql(text(
            "SELECT * FROM company_leads ORDER BY lead_score DESC LIMIT 6"), conn)

    grade_counts = {g: 0 for g in ["A", "B", "C", "D"]}
    for g, n in grade_rows:
        if g in grade_counts:
            grade_counts[g] = int(n)

    if "is_provider" in top_leads_df.columns:
        top_leads_df["is_provider"] = top_leads_df["is_provider"].astype(bool)

    return {
        "total_companies": int(totals["total_companies"] or 0),
        "total_hosts": int(totals["total_hosts"] or 0),
        "total_critical": int(totals["total_critical"] or 0),
        "total_high": int(totals["total_high"] or 0),
        "avg_score": round(float(totals["avg_score"]), 1) if totals["avg_score"] is not None else 0,
        "hot_leads": int(totals["hot_leads"] or 0),
        "grade_counts": grade_counts,
        "severity_counts": {s: int(n) for s, n in severity_rows},
        "top_products": {p: int(n) for p, n in product_rows},
        "country_counts": _top_countries(8),
        "top_leads": top_leads_df.to_dict(orient="records"),
    }


# ---------------------------------------------------------------------
# Reports & Analytics
# ---------------------------------------------------------------------

def top_cves_report(sort="count", order="desc", limit=10):
    """GROUP BY pushed to SQL - vulnerable_cves is the biggest table
    (multi-GB); this must never be loaded whole."""
    allowed_sort = {"cve", "count", "cvss", "epss", "severity"}
    if sort not in allowed_sort:
        sort = "count"
    order_sql = "ASC" if order == "asc" else "DESC"
    query = text(f"""
        SELECT cve,
               COUNT(*) AS count,
               MAX(cvss) AS cvss,
               MAX(epss) AS epss,
               (ARRAY_AGG(severity ORDER BY cvss DESC NULLS LAST))[1] AS severity,
               (ARRAY_AGG(summary ORDER BY cvss DESC NULLS LAST))[1] AS summary
        FROM vulnerable_cves
        GROUP BY cve
        ORDER BY {sort} {order_sql}
        LIMIT :limit
    """)
    with _engine().connect() as conn:
        rows = conn.execute(query, {"limit": limit}).mappings().all()
    return [dict(r) for r in rows]


def scans_by_day(days=30):
    query = text("""
        SELECT LEFT(timestamp, 10) AS day, COUNT(*) AS n
        FROM hosts_scored
        GROUP BY day
        ORDER BY day DESC
        LIMIT :days
    """)
    with _engine().connect() as conn:
        rows = conn.execute(query, {"days": days}).all()
    return dict(sorted(((r[0], int(r[1])) for r in rows), key=lambda x: x[0] or ""))


ALLOWED_CHART_HOST_GROUPBY = {"_vuln_severity", "product", "country", "os"}
ALLOWED_CHART_LEAD_GROUPBY = {"lead_grade", "is_provider", "country"}


def compute_chart_data(dataset, group_by, metric, top_n=15):
    """Group-by + metric aggregation used by the Reports chart builder.
    Returns a list of {label, value} dicts, sorted descending by value."""
    if dataset == "hosts":
        if group_by not in ALLOWED_CHART_HOST_GROUPBY:
            return []
        if metric == "avg_vuln_max_cvss":
            agg = "AVG(_vuln_max_cvss)"
        elif metric == "avg_host_max_epss":
            agg = "AVG(_host_max_epss)"
        else:
            agg = "COUNT(*)"
        query = text(f"""
            SELECT {group_by} AS label, {agg} AS value
            FROM hosts_scored
            WHERE {group_by} IS NOT NULL
            GROUP BY {group_by}
            ORDER BY value DESC
            LIMIT :top_n
        """)
        with _engine().connect() as conn:
            rows = conn.execute(query, {"top_n": top_n}).all()
        return [{"label": str(r[0]), "value": round(float(r[1]), 2)} for r in rows]

    # leads dataset - company_leads is small, pandas is fine here
    if group_by not in ALLOWED_CHART_LEAD_GROUPBY:
        return []
    df = load_company_leads()
    if group_by == "country":
        cmap = _country_map()
        df["country"] = df["company"].map(cmap)
    if group_by not in df.columns:
        return []

    grouped = df.groupby(group_by)
    if metric == "avg_lead_score":
        series = grouped["lead_score"].mean()
    elif metric == "avg_max_cvss":
        series = grouped["max_cvss"].mean()
    elif metric == "sum_n_cves":
        series = grouped["n_cves"].sum()
    elif metric == "avg_max_epss":
        series = grouped["max_epss"].mean()
    else:
        series = grouped.size()

    series = series.sort_values(ascending=False).head(top_n)
    return [{"label": str(k), "value": round(float(v), 2)} for k, v in series.items()]
