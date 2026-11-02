"""Synthetic source files for one business day.

Everything here is fake and deterministic: the same date always produces the same files.
A small share of rows is deliberately dirty (duplicates, bad emails, negative quantities,
future timestamps, inconsistent casing) so that the cleaning and quality rules have work to do.
"""
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

CATEGORIES = [
    ("C01", "Electronics"), ("C02", "Home & Kitchen"), ("C03", "Fashion"), ("C04", "Beauty"),
    ("C05", "Sports & Outdoors"), ("C06", "Books"), ("C07", "Toys & Games"), ("C08", "Grocery"),
]
PAYMENT_METHODS = [("card", "Card"), ("E-Wallet", "E-wallet"), ("bank transfer", "Bank transfer"),
                   ("COD", "Cash on delivery")]
CITIES = ["springfield", "riverton", "lakeside", "fairview", "georgetown", "maple grove", "oakridge"]
FIRST = ["alex", "sam", "jordan", "taylor", "morgan", "casey", "jamie", "riley", "avery", "quinn",
         "drew", "kai", "rowan", "sky", "noah", "mia", "liam", "emma", "lucas", "zoe"]
LAST = ["nguyen", "smith", "tran", "lee", "garcia", "pham", "brown", "le", "martin", "ho"]
ADJ = ["Classic", "Smart", "Eco", "Compact", "Pro", "Ultra", "Everyday", "Travel", "Mini", "Deluxe"]
NOUN = {"C01": ["Earbuds", "Charger", "Speaker", "Keyboard", "Webcam"],
        "C02": ["Kettle", "Pan", "Blender", "Lamp", "Storage Box"],
        "C03": ["T-Shirt", "Sneakers", "Backpack", "Jacket", "Cap"],
        "C04": ["Serum", "Sunscreen", "Lip Balm", "Face Mask", "Shampoo"],
        "C05": ["Yoga Mat", "Water Bottle", "Dumbbell", "Tent", "Jump Rope"],
        "C06": ["Notebook", "Cookbook", "Novel", "Planner", "Comic"],
        "C07": ["Puzzle", "Building Set", "Board Game", "Plush", "RC Car"],
        "C08": ["Coffee Beans", "Green Tea", "Granola", "Olive Oil", "Snack Box"]}
N_CUSTOMERS, N_PRODUCTS = 1500, 200
STATUS_WEIGHTS = [("completed", 70), ("Delivered", 10), ("shipped ", 8), ("CANCELLED", 9), ("returned", 3)]


def _write(path: Path, header, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    return len(rows)


def _products():
    rng = random.Random(7)
    rows = []
    for i in range(1, N_PRODUCTS + 1):
        cid = CATEGORIES[(i - 1) % len(CATEGORIES)][0]
        name = f"{rng.choice(ADJ)} {rng.choice(NOUN[cid])}"
        price = round(rng.uniform(3, 180), 2)
        rows.append([f"P{i:04d}", name, cid, f"{price:.2f}"])
    return rows


def _customers(day_index: int):
    rng = random.Random(42)
    rows = []
    for i in range(1, N_CUSTOMERS + 1):
        first, last = rng.choice(FIRST), rng.choice(LAST)
        email = f"{first}.{last}{i}@example.com"
        signup = date(2025, 1, 1) + timedelta(days=rng.randint(0, 600))
        rows.append([f"CU{i:05d}", f"  {first} {last} ", email, rng.choice(CITIES), signup.isoformat()])
    return rows


def generate_day(business_date: date, landing_dir: Path) -> dict:
    """Write all source CSVs for business_date into landing_dir/<date>/ and return row counts."""
    out = Path(landing_dir) / business_date.isoformat()
    rng = random.Random(business_date.toordinal())
    counts = {}

    counts["categories.csv"] = _write(out / "categories.csv", ["category_id", "category_name"],
                                      [list(c) for c in CATEGORIES])
    counts["payment_methods.csv"] = _write(out / "payment_methods.csv",
                                           ["payment_method_code", "payment_method_name"],
                                           [list(p) for p in PAYMENT_METHODS])

    customers = _customers(business_date.toordinal())
    for row in rng.sample(customers, 12):          # inconsistent casing and spacing
        row[2] = "  " + row[2].upper() + " "
    for row in rng.sample(customers, 3):           # invalid emails
        row[2] = row[2].replace("@", " at ")
    customers += [list(r) for r in rng.sample(customers, 8)]   # duplicated rows in the export
    customers.append(["", "unknown person", "", "", ""])        # missing key
    counts["customers.csv"] = _write(out / "customers.csv",
                                     ["customer_id", "full_name", "email", "city", "signup_date"], customers)

    products = _products()
    for row in rng.sample(products, 15):            # small daily price changes
        row[3] = f"{float(row[3]) * rng.choice([0.9, 0.95, 1.05]):.2f}"
    products.append(["P9998", "Mystery Item", "", "9.99"])      # missing category
    products.append(["P9999", "Broken Price", "C01", "-5.00"])  # negative price
    counts["products.csv"] = _write(out / "products.csv",
                                    ["product_id", "product_name", "category_id", "unit_price"], products)

    weekday_boost = 1.25 if business_date.weekday() >= 5 else 1.0
    n_orders = int(rng.randint(380, 520) * weekday_boost)
    statuses, weights = zip(*STATUS_WEIGHTS)
    pm_codes = [p[0] for p in PAYMENT_METHODS]
    price_by_id = {p[0]: p[3] for p in products}
    orders, items = [], []
    for n in range(1, n_orders + 1):
        oid = f"O{business_date:%Y%m%d}{n:04d}"
        ts = datetime.combine(business_date, datetime.min.time()) + timedelta(seconds=rng.randint(0, 86399))
        orders.append([oid, ts.isoformat(sep=" "), f"CU{rng.randint(1, N_CUSTOMERS):05d}",
                       rng.choice(pm_codes), rng.choices(statuses, weights)[0]])
        for line in range(1, rng.choices([1, 2, 3, 4], [55, 28, 12, 5])[0] + 1):
            pid = f"P{rng.randint(1, N_PRODUCTS):04d}"
            items.append([oid, line, pid, rng.choices([1, 2, 3], [80, 15, 5])[0], price_by_id[pid]])

    for row in rng.sample(orders, 4):               # timestamps after the business date
        row[1] = (datetime.fromisoformat(row[1]) + timedelta(days=2)).isoformat(sep=" ")
    for row in rng.sample(orders, 3):
        row[3] = ""                                  # missing payment method
    orders += [list(r) for r in rng.sample(orders, 5)]   # duplicates
    for row in rng.sample(items, 6):
        row[3] = -1                                  # negative quantity
    counts["orders.csv"] = _write(out / "orders.csv",
                                  ["order_id", "order_ts", "customer_id", "payment_method_code", "status"], orders)
    counts["order_items.csv"] = _write(out / "order_items.csv",
                                       ["order_id", "line_no", "product_id", "quantity", "unit_price"], items)
    return counts
