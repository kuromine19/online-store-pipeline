"""Run the full pipeline without Airflow, for one or more business days.

Usage:
    python scripts/run_local.py 2026-10-01 2026-10-14
"""
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from include.pipeline import steps  # noqa: E402
from include.pipeline.config import WAVES  # noqa: E402


def run_day(d: date) -> None:
    steps.generate_sources(d)
    for wave in WAVES:
        for table in wave:
            s = steps.extract_to_stg(table, d)
            l = steps.load_to_core(table, d, s["extracted"], s["rejected"])
            print(f"{d} {table:<20} extracted={s['extracted']:>5} rejected={s['rejected'] + l['orphans']:>3} "
                  f"loaded={l['loaded']:>5}")
    steps.run_quality_checks(d)
    steps.build_marts(d)


if __name__ == "__main__":
    start = date.fromisoformat(sys.argv[1]) if len(sys.argv) > 1 else date.today() - timedelta(days=1)
    end = date.fromisoformat(sys.argv[2]) if len(sys.argv) > 2 else start
    steps.prepare_warehouse()
    d = start
    while d <= end:
        run_day(d)
        d += timedelta(days=1)
    print("Dashboard data written to", steps.export_site_data(end))
