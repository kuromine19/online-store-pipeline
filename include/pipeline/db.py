from functools import lru_cache
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from .config import WAREHOUSE_URL


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    return create_engine(WAREHOUSE_URL, pool_pre_ping=True)


def run_sql_file(path: Path) -> None:
    sql = Path(path).read_text(encoding="utf-8")
    with get_engine().begin() as conn:
        conn.exec_driver_sql(sql)


def fetch_all(sql: str, **params) -> list[dict]:
    with get_engine().connect() as conn:
        return [dict(r._mapping) for r in conn.execute(text(sql), params)]
