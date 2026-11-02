"""Pure-Python cleaning rules. No database access here, so everything is unit-testable.

Each clean_* function takes raw CSV rows (dicts of strings) and returns (clean_rows, rejected),
where rejected is a list of (reason, raw_row).
"""
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$")
ORDER_STATUSES = {"completed", "delivered", "shipped", "cancelled", "returned"}
REVENUE_STATUSES = ("completed", "delivered", "shipped")


def _s(value) -> str:
    return (value or "").strip()


def _money(value):
    try:
        return Decimal(_s(value)).quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError):
        return None


def _dedupe(rows, key):
    """Keep the last occurrence of each key (later rows in an export win)."""
    seen = {}
    for row in rows:
        seen[key(row)] = row
    return list(seen.values())


def clean_categories(rows):
    clean, rejected = [], []
    for r in rows:
        cid, name = _s(r.get("category_id")), _s(r.get("category_name"))
        if not cid or not name:
            rejected.append(("missing category_id or category_name", r))
            continue
        clean.append({"category_id": cid, "category_name": name})
    return _dedupe(clean, lambda x: x["category_id"]), rejected


def clean_payment_methods(rows):
    clean, rejected = [], []
    for r in rows:
        code = _s(r.get("payment_method_code")).upper().replace(" ", "_")
        name = _s(r.get("payment_method_name"))
        if not code:
            rejected.append(("missing payment_method_code", r))
            continue
        clean.append({"payment_method_code": code, "payment_method_name": name or code.replace("_", " ").title()})
    return _dedupe(clean, lambda x: x["payment_method_code"]), rejected


def clean_customers(rows):
    clean, rejected = [], []
    for r in rows:
        cid = _s(r.get("customer_id"))
        email = _s(r.get("email")).lower()
        if not cid:
            rejected.append(("missing customer_id", r))
            continue
        if email and not EMAIL_RE.match(email):
            email = ""  # keep the customer, drop the unusable email
        try:
            signup = date.fromisoformat(_s(r.get("signup_date"))) if _s(r.get("signup_date")) else None
        except ValueError:
            rejected.append(("invalid signup_date", r))
            continue
        clean.append({
            "customer_id": cid,
            "full_name": " ".join(_s(r.get("full_name")).split()).title() or None,
            "email": email or None,
            "city": _s(r.get("city")).title() or None,
            "signup_date": signup,
        })
    return _dedupe(clean, lambda x: x["customer_id"]), rejected


def clean_products(rows):
    clean, rejected = [], []
    for r in rows:
        pid, cat = _s(r.get("product_id")), _s(r.get("category_id"))
        price = _money(r.get("unit_price"))
        if not pid:
            rejected.append(("missing product_id", r))
        elif not cat:
            rejected.append(("missing category_id", r))
        elif price is None or price < 0:
            rejected.append(("invalid unit_price", r))
        else:
            clean.append({"product_id": pid, "product_name": _s(r.get("product_name")),
                          "category_id": cat, "unit_price": price})
    return _dedupe(clean, lambda x: x["product_id"]), rejected


def clean_orders(rows, business_date: date):
    clean, rejected = [], []
    for r in rows:
        oid = _s(r.get("order_id"))
        status = _s(r.get("status")).lower()
        pm = _s(r.get("payment_method_code")).upper().replace(" ", "_")
        if not oid:
            rejected.append(("missing order_id", r))
            continue
        try:
            ts = datetime.fromisoformat(_s(r.get("order_ts")))
        except ValueError:
            rejected.append(("invalid order_ts", r))
            continue
        if ts.date() > business_date:
            rejected.append(("order_ts after business date", r))
        elif not _s(r.get("customer_id")):
            rejected.append(("missing customer_id", r))
        elif not pm:
            rejected.append(("missing payment_method_code", r))
        elif status not in ORDER_STATUSES:
            rejected.append(("unknown status", r))
        else:
            clean.append({"order_id": oid, "order_ts": ts, "customer_id": _s(r.get("customer_id")),
                          "payment_method_code": pm, "status": status})
    return _dedupe(clean, lambda x: x["order_id"]), rejected


def clean_order_items(rows):
    clean, rejected = [], []
    for r in rows:
        oid, pid = _s(r.get("order_id")), _s(r.get("product_id"))
        price = _money(r.get("unit_price"))
        try:
            line_no, qty = int(_s(r.get("line_no"))), int(_s(r.get("quantity")))
        except ValueError:
            rejected.append(("invalid line_no or quantity", r))
            continue
        if not oid or not pid:
            rejected.append(("missing order_id or product_id", r))
        elif qty <= 0:
            rejected.append(("quantity must be positive", r))
        elif price is None or price < 0:
            rejected.append(("invalid unit_price", r))
        else:
            clean.append({"order_id": oid, "line_no": line_no, "product_id": pid,
                          "quantity": qty, "unit_price": price})
    return _dedupe(clean, lambda x: (x["order_id"], x["line_no"])), rejected
