import os
from pathlib import Path

ROOT = Path(os.environ.get("PIPELINE_ROOT", Path(__file__).resolve().parents[2]))

WAREHOUSE_URL = os.environ.get(
    "WAREHOUSE_URL", "postgresql+psycopg2://demo:demo@localhost:5432/warehouse"
)
LANDING_DIR = Path(os.environ.get("LANDING_DIR", ROOT / "data" / "landing"))
SITE_DATA_DIR = Path(os.environ.get("SITE_DATA_DIR", ROOT / "docs" / "data"))

# Tables processed by the pipeline, grouped in dependency waves.
WAVES = [
    ["dim_category", "dim_payment_method", "dim_customer"],
    ["dim_product"],
    ["fact_orders"],
    ["fact_order_items"],
]

# dimension/fact name -> source file name in the landing folder
SOURCE_FILES = {
    "dim_category": "categories.csv",
    "dim_payment_method": "payment_methods.csv",
    "dim_customer": "customers.csv",
    "dim_product": "products.csv",
    "fact_orders": "orders.csv",
    "fact_order_items": "order_items.csv",
}
