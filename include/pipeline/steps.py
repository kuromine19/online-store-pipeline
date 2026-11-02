"""Pipeline steps. Each function is called by one Airflow task and can also be run locally."""
import csv
import json
import os
from datetime import date
from pathlib import Path

from sqlalchemy import text

from . import transforms as t
from .config import LANDING_DIR, SITE_DATA_DIR, SOURCE_FILES
from .db import fetch_all, get_engine, run_sql_file
from .generate import generate_day

DDL = Path(__file__).parent / "sql" / "ddl.sql"

CLEANERS = {
    "dim_category": t.clean_categories,
    "dim_payment_method": t.clean_payment_methods,
    "dim_customer": t.clean_customers,
    "dim_product": t.clean_products,
    "fact_orders": None,  # needs the business date, see extract_to_stg
    "fact_order_items": t.clean_order_items,
}

# Upsert from staging into core. Facts only load rows whose parents exist in core.
UPSERTS = {
    "dim_category": """
        INSERT INTO core.dim_category (category_id, category_name)
        SELECT category_id, category_name FROM stg.dim_category
        ON CONFLICT (category_id) DO UPDATE SET category_name = EXCLUDED.category_name, updated_at = now()""",
    "dim_payment_method": """
        INSERT INTO core.dim_payment_method (payment_method_code, payment_method_name)
        SELECT payment_method_code, payment_method_name FROM stg.dim_payment_method
        ON CONFLICT (payment_method_code) DO UPDATE
        SET payment_method_name = EXCLUDED.payment_method_name, updated_at = now()""",
    "dim_customer": """
        INSERT INTO core.dim_customer (customer_id, full_name, email, city, signup_date)
        SELECT customer_id, full_name, email, city, signup_date FROM stg.dim_customer
        ON CONFLICT (customer_id) DO UPDATE SET full_name = EXCLUDED.full_name, email = EXCLUDED.email,
            city = EXCLUDED.city, signup_date = EXCLUDED.signup_date, updated_at = now()""",
    "dim_product": """
        INSERT INTO core.dim_product (product_id, product_name, category_id, unit_price)
        SELECT s.product_id, s.product_name, s.category_id, s.unit_price
        FROM stg.dim_product s JOIN core.dim_category c USING (category_id)
        ON CONFLICT (product_id) DO UPDATE SET product_name = EXCLUDED.product_name,
            category_id = EXCLUDED.category_id, unit_price = EXCLUDED.unit_price, updated_at = now()""",
    "fact_orders": """
        INSERT INTO core.fact_orders (order_id, order_date, order_ts, customer_id, payment_method_code, status)
        SELECT s.order_id, s.order_ts::date, s.order_ts, s.customer_id, s.payment_method_code, s.status
        FROM stg.fact_orders s
        JOIN core.dim_customer c USING (customer_id)
        JOIN core.dim_payment_method p USING (payment_method_code)
        ON CONFLICT (order_id) DO UPDATE SET status = EXCLUDED.status, order_ts = EXCLUDED.order_ts,
            order_date = EXCLUDED.order_date, customer_id = EXCLUDED.customer_id,
            payment_method_code = EXCLUDED.payment_method_code, updated_at = now()""",
    "fact_order_items": """
        INSERT INTO core.fact_order_items (order_id, line_no, product_id, quantity, unit_price, line_amount)
        SELECT s.order_id, s.line_no, s.product_id, s.quantity, s.unit_price, s.quantity * s.unit_price
        FROM stg.fact_order_items s
        JOIN core.fact_orders o USING (order_id)
        JOIN core.dim_product p USING (product_id)
        ON CONFLICT (order_id, line_no) DO UPDATE SET product_id = EXCLUDED.product_id,
            quantity = EXCLUDED.quantity, unit_price = EXCLUDED.unit_price,
            line_amount = EXCLUDED.line_amount, updated_at = now()""",
}

# Staged rows that the upsert above skips because a parent row is missing.
ORPHANS = {
    "dim_product": ("unknown category_id",
                    "SELECT s.* FROM stg.dim_product s LEFT JOIN core.dim_category c USING (category_id) "
                    "WHERE c.category_id IS NULL"),
    "fact_orders": ("unknown customer or payment method",
                    "SELECT s.* FROM stg.fact_orders s "
                    "LEFT JOIN core.dim_customer c USING (customer_id) "
                    "LEFT JOIN core.dim_payment_method p USING (payment_method_code) "
                    "WHERE c.customer_id IS NULL OR p.payment_method_code IS NULL"),
    "fact_order_items": ("order or product not loaded",
                         "SELECT s.* FROM stg.fact_order_items s "
                         "LEFT JOIN core.fact_orders o USING (order_id) "
                         "LEFT JOIN core.dim_product p USING (product_id) "
                         "WHERE o.order_id IS NULL OR p.product_id IS NULL"),
}


def prepare_warehouse() -> None:
    run_sql_file(DDL)


def generate_sources(business_date: date) -> dict:
    return generate_day(business_date, LANDING_DIR)


def _read_csv(path: Path) -> list[dict]:
    with Path(path).open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _reject(conn, business_date: date, table: str, rows: list[tuple[str, dict]]) -> None:
    if rows:
        conn.execute(
            text("INSERT INTO audit.rejected_rows (run_date, table_name, reason, payload) "
                 "VALUES (:d, :t, :r, CAST(:p AS jsonb))"),
            [{"d": business_date, "t": table, "r": reason, "p": json.dumps(row, default=str)}
             for reason, row in rows],
        )


def extract_to_stg(table: str, business_date: date) -> dict:
    """Read the source file, apply cleaning rules, replace the staging table."""
    raw = _read_csv(LANDING_DIR / business_date.isoformat() / SOURCE_FILES[table])
    if table == "fact_orders":
        clean, rejected = t.clean_orders(raw, business_date)
    else:
        clean, rejected = CLEANERS[table](raw)

    with get_engine().begin() as conn:
        conn.execute(text(f"DELETE FROM audit.rejected_rows WHERE run_date = :d AND table_name = :t"),
                     {"d": business_date, "t": table})
        conn.execute(text(f"TRUNCATE stg.{table}"))
        if clean:
            cols = list(clean[0].keys())
            conn.execute(
                text(f"INSERT INTO stg.{table} ({', '.join(cols)}) VALUES ({', '.join(':' + c for c in cols)})"),
                clean,
            )
        _reject(conn, business_date, table, rejected)
    return {"extracted": len(raw), "rejected": len(rejected), "staged": len(clean)}


def load_to_core(table: str, business_date: date, extracted: int, rejected: int) -> dict:
    """Upsert staged rows into core and record load statistics for the run."""
    with get_engine().begin() as conn:
        orphan_rows = []
        if table in ORPHANS:
            reason, sql = ORPHANS[table]
            orphan_rows = [(reason, dict(r._mapping)) for r in conn.execute(text(sql))]
            _reject(conn, business_date, table, orphan_rows)
        loaded = conn.execute(text(UPSERTS[table])).rowcount
        conn.execute(
            text("INSERT INTO audit.load_stats (run_date, table_name, extracted, rejected, loaded) "
                 "VALUES (:d, :t, :e, :r, :l) ON CONFLICT (run_date, table_name) DO UPDATE "
                 "SET extracted = EXCLUDED.extracted, rejected = EXCLUDED.rejected, "
                 "loaded = EXCLUDED.loaded, loaded_at = now()"),
            {"d": business_date, "t": table, "e": extracted, "r": rejected + len(orphan_rows), "l": loaded},
        )
    return {"loaded": loaded, "orphans": len(orphan_rows)}


QUALITY_CHECKS = [
    # (name, severity, SQL returning the number of failing rows)
    ("orders_have_items", "warning",
     "SELECT count(*) FROM core.fact_orders o WHERE o.order_date = :d "
     "AND NOT EXISTS (SELECT 1 FROM core.fact_order_items i WHERE i.order_id = o.order_id)"),
    ("no_negative_amounts", "blocking",
     "SELECT count(*) FROM core.fact_order_items i JOIN core.fact_orders o USING (order_id) "
     "WHERE o.order_date = :d AND (i.quantity <= 0 OR i.unit_price < 0)"),
    ("no_future_orders", "blocking",
     # Only orders loaded by this run (still in staging), so runs for earlier days
     # are not failed by orders that later runs have already loaded.
     "SELECT count(*) FROM core.fact_orders o JOIN stg.fact_orders s USING (order_id) "
     "WHERE o.order_date > :d"),
    ("customers_have_email", "warning",
     "SELECT count(*) FROM core.dim_customer WHERE email IS NULL"),
    ("daily_volume_in_range", "warning",
     """WITH today AS (SELECT count(*) n FROM core.fact_orders WHERE order_date = :d),
             hist AS (SELECT avg(n) a FROM (SELECT count(*) n FROM core.fact_orders
                      WHERE order_date BETWEEN CAST(:d AS date) - 7 AND CAST(:d AS date) - 1
                      GROUP BY order_date) x)
        SELECT CASE WHEN hist.a IS NULL THEN 0
                    WHEN today.n BETWEEN hist.a * 0.5 AND hist.a * 1.5 THEN 0 ELSE 1 END
        FROM today, hist"""),
]


def run_quality_checks(business_date: date) -> list[dict]:
    results = []
    with get_engine().begin() as conn:
        for name, severity, sql in QUALITY_CHECKS:
            failed = conn.execute(text(sql), {"d": business_date}).scalar() or 0
            passed = failed == 0
            conn.execute(
                text("INSERT INTO audit.quality_results (run_date, check_name, severity, failed_rows, passed) "
                     "VALUES (:d, :n, :s, :f, :p) ON CONFLICT (run_date, check_name) DO UPDATE "
                     "SET severity = EXCLUDED.severity, failed_rows = EXCLUDED.failed_rows, "
                     "passed = EXCLUDED.passed, checked_at = now()"),
                {"d": business_date, "n": name, "s": severity, "f": failed, "p": passed},
            )
            results.append({"check": name, "severity": severity, "failed_rows": failed, "passed": passed})
    blocking = [r for r in results if r["severity"] == "blocking" and not r["passed"]]
    if blocking:
        raise ValueError(f"Blocking quality checks failed: {blocking}")
    return results


def build_marts(business_date: date) -> None:
    revenue_statuses = "('" + "','".join(t.REVENUE_STATUSES) + "')"
    with get_engine().begin() as conn:
        conn.execute(text("DELETE FROM mart.daily_sales WHERE sales_date = :d"), {"d": business_date})
        conn.execute(text(f"""
            INSERT INTO mart.daily_sales (sales_date, orders, items, revenue, avg_order_value)
            SELECT o.order_date, count(DISTINCT o.order_id), coalesce(sum(i.quantity), 0),
                   coalesce(sum(i.line_amount), 0),
                   round(coalesce(sum(i.line_amount), 0) / nullif(count(DISTINCT o.order_id), 0), 2)
            FROM core.fact_orders o JOIN core.fact_order_items i USING (order_id)
            WHERE o.order_date = :d AND o.status IN {revenue_statuses}
            GROUP BY o.order_date"""), {"d": business_date})
        conn.execute(text("DELETE FROM mart.category_sales WHERE sales_date = :d"), {"d": business_date})
        conn.execute(text(f"""
            INSERT INTO mart.category_sales (sales_date, category_name, revenue, items)
            SELECT o.order_date, c.category_name, sum(i.line_amount), sum(i.quantity)
            FROM core.fact_orders o
            JOIN core.fact_order_items i USING (order_id)
            JOIN core.dim_product p USING (product_id)
            JOIN core.dim_category c USING (category_id)
            WHERE o.order_date = :d AND o.status IN {revenue_statuses}
            GROUP BY o.order_date, c.category_name"""), {"d": business_date})


def export_site_data(business_date: date, out_dir: Path = SITE_DATA_DIR) -> Path:
    """Write the JSON that the GitHub Pages dashboard (docs/index.html) reads."""
    daily = fetch_all("SELECT sales_date::text AS date, orders, items, revenue::float AS revenue, "
                      "avg_order_value::float AS aov FROM mart.daily_sales ORDER BY sales_date")
    categories = fetch_all("SELECT category_name AS category, sum(revenue)::float AS revenue "
                           "FROM mart.category_sales GROUP BY category_name ORDER BY 2 DESC")
    stats = fetch_all("SELECT table_name AS table, extracted, rejected, loaded FROM audit.load_stats "
                      "WHERE run_date = :d ORDER BY table_name", d=business_date)
    checks = fetch_all("SELECT check_name AS check, severity, failed_rows, passed FROM audit.quality_results "
                       "WHERE run_date = :d ORDER BY check_name", d=business_date)
    reasons = fetch_all("SELECT table_name AS table, reason, count(*) AS rows FROM audit.rejected_rows "
                        "WHERE run_date = :d GROUP BY 1, 2 ORDER BY 3 DESC", d=business_date)
    payload = {"generated_for": business_date.isoformat(), "daily_sales": daily, "category_sales": categories,
               "last_run": {"load_stats": stats, "quality_checks": checks, "rejections": reasons}}
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    path = out / "dashboard.json"
    # Write to a temporary file and swap it in. This is atomic, and it also works when the existing
    # file belongs to another user (for example a file checked out on the host and mounted into Docker).
    tmp = out / "dashboard.json.tmp"
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(tmp, path)
    return path
