"""Keystone on Airflow: the local orchestration showcase (Phase 10, ADR-063).

Every task calls the same `keystone` CLI that GitHub Actions runs in production; no pipeline
logic lives here. The fixture hours make the run self-contained (no downloads, no secrets):
parse, the rollup through the profile check (no `full` stack in this container, so it falls
back to free and records it), score, metrics, site exports and the brief (skipped without a
key). Airflow's OpenLineage provider records each task run (compose sets the transport).
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

ROOT = "/opt/keystone/data/airflow"
CLI = "cd /opt/keystone && keystone"

with DAG(
    dag_id="keystone_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule=None,  # production schedules live in GitHub Actions (ADR-041)
    catchup=False,
    tags=["keystone", "showcase"],
) as dag:
    parse = BashOperator(
        task_id="parse_fixture_hours",
        bash_command=f"{CLI} parse files tests/fixtures/gharchive/*.json.gz --root {ROOT}",
    )
    rollup = BashOperator(
        task_id="profile_check_rollup",
        bash_command=f"{CLI} profile check --run --root {ROOT}",
    )
    score = BashOperator(
        task_id="score",
        bash_command=(
            f"{CLI} score live --rollup {ROOT}/rollup/rollup.duckdb --warehouse {ROOT}/none "
            f"--out {ROOT}/score/live.json && {CLI} score backtest --rollup "
            f"{ROOT}/rollup/rollup.duckdb --warehouse {ROOT}/none --out {ROOT}/score/backtest.json "
            f"--report {ROOT}/score/backtest.md"
        ),
    )
    metrics = BashOperator(
        task_id="metrics_emit",
        bash_command=f"{CLI} metrics emit --root {ROOT}",
    )
    export = BashOperator(
        task_id="export_site",
        bash_command=(
            f"{CLI} export site --root {ROOT} --score-dir {ROOT}/score --warehouse {ROOT}/none "
            f"--metrics {ROOT}/metrics/pipeline_metrics.json --out {ROOT}/site"
        ),
    )
    brief = BashOperator(
        task_id="narrate_brief",
        bash_command=(
            f"{CLI} narrate brief --site {ROOT}/site --root {ROOT} --cache-dir {ROOT}/narrate"
        ),
    )
    parse >> rollup >> score >> metrics >> export >> brief
