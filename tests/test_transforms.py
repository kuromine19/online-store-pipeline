from datetime import date
from decimal import Decimal

from include.pipeline import transforms as t


def test_customers_are_trimmed_deduplicated_and_keep_bad_emails_as_null():
    rows = [
        {"customer_id": "CU1", "full_name": "  alex   nguyen ", "email": " ALEX@EXAMPLE.COM ", "city": "riverton",
         "signup_date": "2025-03-01"},
        {"customer_id": "CU1", "full_name": "alex nguyen", "email": "alex@example.com", "city": "lakeside",
         "signup_date": "2025-03-01"},
        {"customer_id": "CU2", "full_name": "sam lee", "email": "sam at example.com", "city": "", "signup_date": ""},
        {"customer_id": "", "full_name": "nobody", "email": "", "city": "", "signup_date": ""},
    ]
    clean, rejected = t.clean_customers(rows)
    by_id = {c["customer_id"]: c for c in clean}
    assert len(clean) == 2
    assert by_id["CU1"]["full_name"] == "Alex Nguyen"
    assert by_id["CU1"]["city"] == "Lakeside"          # last row in the export wins
    assert by_id["CU2"]["email"] is None               # invalid email dropped, customer kept
    assert [r[0] for r in rejected] == ["missing customer_id"]


def test_payment_codes_are_normalised():
    clean, _ = t.clean_payment_methods([{"payment_method_code": "bank transfer", "payment_method_name": ""}])
    assert clean == [{"payment_method_code": "BANK_TRANSFER", "payment_method_name": "Bank Transfer"}]


def test_products_with_negative_price_or_no_category_are_rejected():
    clean, rejected = t.clean_products([
        {"product_id": "P1", "product_name": "Kettle", "category_id": "C02", "unit_price": "19.999"},
        {"product_id": "P2", "product_name": "Bad", "category_id": "C01", "unit_price": "-1"},
        {"product_id": "P3", "product_name": "Lost", "category_id": "", "unit_price": "5"},
    ])
    assert clean[0]["unit_price"] == Decimal("20.00")
    assert sorted(r[0] for r in rejected) == ["invalid unit_price", "missing category_id"]


def test_orders_after_business_date_and_unknown_status_are_rejected():
    d = date(2026, 10, 13)
    base = {"customer_id": "CU1", "payment_method_code": "card"}
    clean, rejected = t.clean_orders([
        {**base, "order_id": "O1", "order_ts": "2026-10-13 10:00:00", "status": " Completed "},
        {**base, "order_id": "O2", "order_ts": "2026-10-15 10:00:00", "status": "completed"},
        {**base, "order_id": "O3", "order_ts": "2026-10-13 11:00:00", "status": "lost"},
    ], d)
    assert [o["order_id"] for o in clean] == ["O1"]
    assert clean[0]["status"] == "completed" and clean[0]["payment_method_code"] == "CARD"
    assert sorted(r[0] for r in rejected) == ["order_ts after business date", "unknown status"]


def test_order_items_require_positive_quantity():
    clean, rejected = t.clean_order_items([
        {"order_id": "O1", "line_no": "1", "product_id": "P1", "quantity": "2", "unit_price": "10"},
        {"order_id": "O1", "line_no": "2", "product_id": "P2", "quantity": "-1", "unit_price": "10"},
        {"order_id": "O1", "line_no": "x", "product_id": "P2", "quantity": "1", "unit_price": "10"},
    ])
    assert len(clean) == 1
    assert sorted(r[0] for r in rejected) == ["invalid line_no or quantity", "quantity must be positive"]
