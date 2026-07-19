import csv
import io
import math
from datetime import datetime
from urllib.parse import urlencode

from flask import Flask, render_template, request, jsonify, Response, g, redirect, url_for
from sqlalchemy.exc import DBAPIError
from psycopg2 import OperationalError as PGOperationalError
from werkzeug.datastructures import MultiDict

import data_loader as dl
import db

app = Flask(__name__)


@app.errorhandler(db.DatabaseNotConfigured)
def handle_db_not_configured(e):
    return (f"<div style='font-family:sans-serif;max-width:560px;margin:80px auto;'>"
            f"<h2>Database not configured</h2><p>{e}</p></div>", 500)


@app.errorhandler(DBAPIError)
@app.errorhandler(PGOperationalError)
def handle_db_unreachable(e):
    return (f"<div style='font-family:sans-serif;max-width:620px;margin:80px auto;'>"
            f"<h2>Database query problem</h2>"
            f"<p>Either the connection failed (security group / DATABASE_URL / "
            f"DB_SCHEMA) or a query took too long and was cancelled after "
            f"{db.STATEMENT_TIMEOUT_MS // 1000}s. Try narrowing your filters, "
            f"or check the app logs for the full error.</p>"
            f"<p style='color:#888;font-size:13px;'>{e}</p></div>", 500)


# ---------------------------------------------------------------------
# App-owned workflow store (lead status / notes / saved charts) - Postgres
# on RDS. See db.py for the connection pool and schema.sql for the tables.
# ---------------------------------------------------------------------

def get_db():
    if "db" not in g:
        g.db = db.get_conn()
    return g.db


@app.teardown_appcontext
def close_db(exc):
    conn = g.pop("db", None)
    if conn is not None:
        db.put_conn(conn)


def get_all_status():
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM lead_status")
        rows = cur.fetchall()
    return {r["company"]: dict(r) for r in rows}


# ---------------------------------------------------------------------
# Sorting / pagination helpers, shared by every table page
# ---------------------------------------------------------------------

@app.template_global()
def sort_link(column):
    """Build a query string that toggles sort/order for `column`,
    preserving every other active filter."""
    md = MultiDict(request.args)
    current_sort = md.get("sort")
    current_order = md.get("order", "desc")
    new_order = "asc" if (current_sort == column and current_order == "desc") else "desc"
    pairs = [(k, v) for k, v in md.items(multi=True) if k not in ("sort", "order", "page")]
    pairs += [("sort", column), ("order", new_order)]
    return "?" + urlencode(pairs)


@app.template_global()
def sort_state(column):
    """'asc' | 'desc' | None - used to render the active sort arrow."""
    if request.args.get("sort") == column:
        return request.args.get("order", "desc")
    return None


@app.template_global()
def page_link(page_num):
    md = MultiDict(request.args)
    pairs = [(k, v) for k, v in md.items(multi=True) if k != "page"]
    pairs.append(("page", page_num))
    return "?" + urlencode(pairs)


def build_pg(total, page_size, default_size=50):
    """Builds the pagination-info dict the templates expect, from a
    SQL-provided total count rather than an in-memory DataFrame length."""
    try:
        page = max(1, int(request.args.get("page", 1)))
    except ValueError:
        page = 1
    if page_size is None:
        return {"page": 1, "pages": 1, "size": total or 1, "total": total,
                "start": 1 if total else 0, "end": total}
    pages = max(1, math.ceil(total / page_size)) if page_size else 1
    page = min(page, pages)
    start = (page - 1) * page_size
    end = min(start + page_size, total)
    return {"page": page, "pages": pages, "size": page_size, "total": total,
            "start": start + 1 if total else 0, "end": end}


def parse_page_args(default_size=50):
    try:
        page = max(1, int(request.args.get("page", 1)))
    except ValueError:
        page = 1
    size_arg = request.args.get("page_size", str(default_size))
    if size_arg == "all":
        return page, None
    try:
        size = int(size_arg)
    except ValueError:
        size = default_size
    return page, size


# ---------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------

@app.route("/")
def dashboard():
    stats = dl.dashboard_stats()
    return render_template("dashboard.html", stats=stats, active="dashboard")


@app.route("/companies")
def companies():
    """Company Risk Overview - analytic view of every scanned company."""
    page, page_size = parse_page_args()
    page_df, total = dl.filter_leads(request.args, page=page, page_size=page_size)
    options = dl.get_filter_options()
    pg = build_pg(total, page_size)
    return render_template("companies.html", leads=page_df.to_dict(orient="records"),
                            options=options, args=request.args, active="companies",
                            result_count=total, pg=pg)


@app.route("/companies/export.csv")
def export_companies_csv():
    filtered, _ = dl.filter_leads(request.args, page=1, page_size=None, include_country=True)
    cols = ["company", "domain", "country", "lead_grade", "lead_score", "n_hosts",
            "n_cves", "n_verified_cves", "n_critical", "n_high", "max_cvss",
            "max_epss", "avg_confidence", "is_provider", "priority_reason"]
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(cols)
    for _, r in filtered.iterrows():
        writer.writerow([r.get(c, "") for c in cols])
    return Response(buf.getvalue(), mimetype="text/csv",
                     headers={"Content-Disposition": "attachment; filename=company_risk_overview.csv"})


@app.route("/company/<path:company_name>")
def company_detail(company_name):
    detail = dl.get_company_detail(company_name)
    if detail is None:
        return "Company not found", 404
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM lead_status WHERE company=%s", (company_name,))
        row = cur.fetchone()
    status = dict(row) if row else {"status": "New", "notes": ""}
    return render_template("company_detail.html", detail=detail, status=status, active="companies")


@app.route("/leads")
def leads():
    """Lead Management - actionable, filterable list for the sales team."""
    page, page_size = parse_page_args()
    page_df, total = dl.filter_leads(request.args, page=page, page_size=page_size)
    options = dl.get_filter_options()
    status_map = get_all_status()
    pg = build_pg(total, page_size)
    records = page_df.to_dict(orient="records")
    for r in records:
        s = status_map.get(r["company"])
        r["status"] = s["status"] if s else "New"
    return render_template("leads.html", leads=records, options=options,
                            args=request.args, active="leads",
                            result_count=total, pg=pg)


@app.route("/leads/export.csv")
def export_leads_csv():
    filtered, _ = dl.filter_leads(request.args, page=1, page_size=None, include_country=True)
    status_map = get_all_status()
    cols = ["company", "domain", "country", "lead_grade", "lead_score",
            "n_hosts", "n_cves", "n_critical", "n_high", "max_cvss", "max_epss",
            "avg_confidence", "is_provider", "priority_reason"]
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(cols + ["status"])
    for _, r in filtered.iterrows():
        s = status_map.get(r["company"])
        writer.writerow([r.get(c, "") for c in cols] + [s["status"] if s else "New"])
    return Response(buf.getvalue(), mimetype="text/csv",
                     headers={"Content-Disposition": "attachment; filename=leads_export.csv"})


@app.route("/lead/<path:company>/status", methods=["POST"])
def update_lead_status(company):
    payload = request.get_json(force=True)
    status = payload.get("status", "New")
    notes = payload.get("notes")
    conn = get_db()
    with conn.cursor() as cur:
        if notes is None:
            cur.execute("SELECT notes FROM lead_status WHERE company=%s", (company,))
            existing = cur.fetchone()
            notes = existing["notes"] if existing else ""
        cur.execute("""
            INSERT INTO lead_status (company, status, notes, updated_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (company) DO UPDATE SET status=EXCLUDED.status,
                notes=EXCLUDED.notes, updated_at=EXCLUDED.updated_at
        """, (company, status, notes, datetime.utcnow()))
    return jsonify({"ok": True, "status": status})


@app.route("/vulnerable-assets")
def vulnerable_assets():
    page, page_size = parse_page_args()
    page_df, total, is_capped = dl.filter_vulnerable_hosts(request.args, page=page, page_size=page_size)
    options = dl.get_filter_options()
    summary = dl.vulnerable_hosts_summary()
    pg = build_pg(total, page_size)
    return render_template("vulnerable_assets.html", hosts=page_df.to_dict(orient="records"),
                            options=options, args=request.args, active="vulnerable",
                            result_count=total, result_capped=is_capped, summary=summary, pg=pg)


@app.route("/vulnerable-assets/export.csv")
def export_vulnerable_assets_csv():
    hosts, _, _ = dl.filter_vulnerable_hosts(request.args, page=1, page_size=None)
    cols = ["_company", "_company_domain", "ip_str", "port", "product", "version",
            "os", "_vuln_severity", "_vuln_max_cvss", "_vuln_count",
            "country", "city", "asn", "isp", "timestamp"]
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(cols)
    for _, r in hosts.iterrows():
        writer.writerow([r.get(c, "") for c in cols])
    return Response(buf.getvalue(), mimetype="text/csv",
                     headers={"Content-Disposition": "attachment; filename=vulnerable_assets.csv"})


@app.route("/vulnerable-assets/host-cves")
def host_cves():
    company = request.args.get("company", "")
    ip = request.args.get("ip", "")
    port = request.args.get("port", "")
    if not (company and ip and port):
        return "Missing company, ip, or port", 400
    cves = dl.get_host_cves(company, ip, port)
    return render_template("host_cves.html", company=company, ip=ip, port=port,
                            cves=cves, active="vulnerable")


# ---------------------------------------------------------------------
# Reports & Analytics - saved, configurable graphs (Postgres-backed)
# ---------------------------------------------------------------------

CHART_FIELDS = {
    "leads": {
        "label": "Companies (leads)",
        "group_by": [("lead_grade", "Lead grade"),
                      ("country", "Country"), ("is_provider", "Hosting/ISP provider")],
        "metric": [("count", "Count of companies"), ("avg_lead_score", "Average lead score"),
                   ("avg_max_cvss", "Average max CVSS"), ("sum_n_cves", "Total CVEs"),
                   ("avg_max_epss", "Average max EPSS")],
    },
    "hosts": {
        "label": "Hosts",
        "group_by": [("_vuln_severity", "Severity"), ("product", "Product"),
                      ("country", "Country"), ("os", "Operating system")],
        "metric": [("count", "Count of hosts"), ("avg_vuln_max_cvss", "Average max CVSS"),
                   ("avg_host_max_epss", "Average max EPSS")],
    },
}


CHART_PALETTE = ["#EA580C", "#8B5CF6", "#2FBF71", "#F0C242", "#3B82F6",
                 "#E4483F", "#14B8A6", "#F472B6", "#A3A3A3", "#7C3AED",
                 "#0EA5E9", "#84CC16"]


def enrich_chart_for_render(chart):
    """Attach whatever extra fields each chart_type needs to render:
    legend + conic-gradient for pie/donut, SVG polyline points for line."""
    data = chart["data"]
    chart["legend"] = [
        {"label": row["label"], "value": row["value"],
         "color": CHART_PALETTE[i % len(CHART_PALETTE)]}
        for i, row in enumerate(data)
    ]

    if chart["chart_type"] in ("pie", "donut"):
        total = sum(row["value"] for row in data) or 1
        cumulative = 0.0
        segments = []
        for i, row in enumerate(data):
            frac = row["value"] / total
            start = cumulative * 360
            cumulative += frac
            end = cumulative * 360
            color = CHART_PALETTE[i % len(CHART_PALETTE)]
            segments.append(f"{color} {start:.2f}deg {end:.2f}deg")
        chart["pie_css"] = "conic-gradient(" + ", ".join(segments) + ")" if data else "none"

    elif chart["chart_type"] == "line":
        n = len(data)
        max_val = max((row["value"] for row in data), default=1) or 1
        points = []
        for i, row in enumerate(data):
            x = 24 + i * (352 / max(n - 1, 1))
            y = 118 - (row["value"] / max_val * 96)
            points.append({"x": round(x, 1), "y": round(y, 1),
                            "label": row["label"], "value": row["value"]})
        chart["line_points"] = points
        chart["line_polyline"] = " ".join(f"{p['x']},{p['y']}" for p in points)

    return chart


@app.route("/reports")
def reports():
    stats = dl.dashboard_stats()

    top_cves = dl.top_cves_report(
        sort=request.args.get("sort", "count"),
        order=request.args.get("order", "desc"),
        limit=10,
    )

    scans_by_day = dl.scans_by_day(30)

    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM saved_charts ORDER BY created_at DESC")
        saved = cur.fetchall()
    charts = []
    for row in saved:
        chart = dict(row)
        chart["data"] = dl.compute_chart_data(chart["dataset"], chart["group_by"], chart["metric"])
        charts.append(enrich_chart_for_render(chart))

    return render_template("reports.html", stats=stats, top_cves=top_cves,
                            scans_by_day=scans_by_day, active="reports",
                            chart_fields=CHART_FIELDS, saved_charts=charts)


@app.route("/reports/charts", methods=["POST"])
def create_chart():
    name = request.form.get("name", "").strip() or "Untitled graph"
    dataset = request.form.get("dataset", "leads")
    chart_type = request.form.get("chart_type", "bar")
    group_by = request.form.get(f"group_by_{dataset}")
    metric = request.form.get(f"metric_{dataset}")
    if group_by and metric:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO saved_charts (name, dataset, group_by, metric, chart_type, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (name, dataset, group_by, metric, chart_type, datetime.utcnow()))
    return redirect(url_for("reports"))


@app.route("/reports/charts/<int:chart_id>/delete", methods=["POST"])
def delete_chart(chart_id):
    conn = get_db()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM saved_charts WHERE id=%s", (chart_id,))
    return jsonify({"ok": True})


if __name__ == "__main__":
    db.init_db()
    app.run(debug=True, host="0.0.0.0", port=5050)
else:
    db.init_db()
