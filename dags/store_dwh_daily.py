"""Daily warehouse load for a fictional online store (Airflow 3).

Each table runs extract_to_stg -> load_to_core inside its own task group. Tables are grouped in
waves so that dimensions are loaded before the facts that reference them.
"""
from datetime import date, timedelta

import pendulum
from airflow.sdk import Param, dag, get_current_context, task, task_group

from include.pipeline import steps
from include.pipeline.config import WAVES


def business_date() -> date:
    """The day being processed: the `business_date` param if given, otherwise the day before the run."""
    ctx = get_current_context()
    override = (ctx.get("params") or {}).get("business_date")
    if override:
        return date.fromisoformat(override)
    run_time = ctx.get("logical_date") or pendulum.now("UTC")
    return run_time.date() - timedelta(days=1)


def table_group(table: str):
    @task_group(group_id=table)
    def group():
        @task(task_id="extract_to_stg")
        def extract() -> dict:
            return steps.extract_to_stg(table, business_date())

        @task(task_id="load_to_core")
        def load(stats: dict) -> dict:
            return steps.load_to_core(table, business_date(), stats["extracted"], stats["rejected"])

        load(extract())

    return group()


@dag(
    dag_id="store_dwh_daily",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1),
                  "execution_timeout": timedelta(minutes=20)},
    params={"business_date": Param(None, type=["null", "string"], format="date",
                                   description="Optional. Day to process (YYYY-MM-DD). "
                                               "Defaults to the day before the run.")},
    tags=["demo", "warehouse"],
    doc_md=__doc__,
)
def store_dwh_daily():
    @task
    def prepare_warehouse():
        steps.prepare_warehouse()

    @task
    def generate_sources() -> dict:
        return steps.generate_sources(business_date())

    @task(retries=0)
    def quality_checks() -> list:
        return steps.run_quality_checks(business_date())

    @task
    def build_marts():
        steps.build_marts(business_date())

    @task
    def export_site_data() -> str:
        return str(steps.export_site_data(business_date()))

    previous = generate_sources()
    prepare_warehouse() >> previous
    for i, tables in enumerate(WAVES, start=1):
        @task_group(group_id=f"wave_{i}")
        def wave(tables=tables):
            for table in tables:
                table_group(table)

        current = wave()
        previous >> current
        previous = current

    previous >> quality_checks() >> build_marts() >> export_site_data()


store_dwh_daily()
