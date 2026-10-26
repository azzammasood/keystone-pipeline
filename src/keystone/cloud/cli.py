"""`keystone cloud`: the Databricks Free Edition and BigQuery sandbox rungs (ADR-067, ADR-068).

Both run on GitHub Actions (`.github/workflows/cloud.yml`) with secrets from the environment.
Each step is a stage record (`telemetry/runs.jsonl`), the engines[] evidence, and the measured
numbers go to one JSON file; nothing here prints a token.
"""

import json
from pathlib import Path
from typing import Any

import typer

from keystone.paths import DATA_DIR
from keystone.telemetry import versions
from keystone.telemetry.runlog import Stage

app = typer.Typer(no_args_is_help=True, help="Databricks and BigQuery rungs (ADR-067, ADR-068).")
databricks_app = typer.Typer(no_args_is_help=True, help="Databricks Free Edition (ADR-067).")
bigquery_app = typer.Typer(no_args_is_help=True, help="BigQuery sandbox (ADR-068).")
app.add_typer(databricks_app, name="databricks")
app.add_typer(bigquery_app, name="bigquery")


def _write(out: Path, doc: dict[str, Any]) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(doc, indent=2, default=str) + "\n", encoding="utf-8")
    typer.echo(json.dumps(doc, indent=2, default=str))


@databricks_app.command("run")
def databricks_run(
    root: Path = typer.Option(DATA_DIR, help="Data directory with silver/ and rollup/."),
    out: Path = typer.Option(Path("cloud-evidence/databricks.json"), help="Evidence JSON."),
    lakeflow: bool = typer.Option(True, help="Run the Lakeflow Declarative Pipeline update."),
) -> None:
    """Stage the dbt sources in Unity Catalog, `dbt build --target databricks`, run the
    Lakeflow pipeline, read Unity Catalog lineage; exit 1 if staging or dbt fails."""
    import time

    from keystone.cloud import databricks as dbx
    from keystone.warehouse.dbt import WarehousePaths, _environ, run_dbt, run_results

    ws = dbx.Workspace.from_env()
    paths = WarehousePaths(root)
    doc: dict[str, Any] = {"catalog": ws.catalog, "data": "committed fixtures (CI path)"}
    t0 = time.perf_counter()
    with Stage(root, "cloud.databricks.stage", "databricks-sql", "serverless") as st:
        rows = dbx.stage(ws, paths, root / "cloud" / "stage")
        st.records_out = sum(rows.values())
        st.detail["tables"] = rows
    doc["stage"] = {"tables": rows, "seconds": round(time.perf_counter() - t0, 1)}
    t0 = time.perf_counter()
    ver = versions.package("dbt-databricks")
    with (
        _environ(dbx.dbt_env(ws)),
        Stage(root, "cloud.databricks.dbt", "dbt-databricks", ver) as st,
    ):
        ok = run_dbt(["build"], paths, target="databricks")
        res = run_results(paths.target_dir)
        st.records_out = res.nodes
        st.detail.update(tests_passed=res.tests_passed, tests_failed=res.tests_failed)
        doc["dbt"] = {
            "ok": ok,
            "version": ver,
            "nodes": res.nodes,
            "tests_passed": res.tests_passed,
            "tests_failed": res.tests_failed,
            "seconds": round(time.perf_counter() - t0, 1),
        }
        if not ok:  # a failed stage record, then exit 1
            _write(out, doc)
            raise typer.Exit(1)
    if lakeflow:
        try:
            with Stage(
                root, "cloud.databricks.lakeflow", "databricks-lakeflow", "serverless"
            ) as st:
                doc["lakeflow"] = dbx.lakeflow(ws)
                if doc["lakeflow"]["state"] != "COMPLETED":
                    raise dbx.DatabricksError(f"pipeline update {doc['lakeflow']['state']}")
                rows_mv = ws.sql(f"SELECT count(*) FROM {ws.catalog}.{dbx.LDP_SCHEMA}.repo_week")
                st.records_out = int(rows_mv[0][0]) if rows_mv else 0
                doc["lakeflow"]["rows"] = st.rec.records_out
        except dbx.DatabricksError as exc:  # quota or feature limits: logged honestly
            doc.setdefault("lakeflow", {})["error"] = str(exc)[:400]
    doc["lineage"] = dbx.lineage(ws)
    _write(out, doc)


@databricks_app.command("keepalive")
def databricks_keepalive() -> None:
    """One trivial statement on the serverless warehouse, so the account counts as active."""
    from keystone.cloud import databricks as dbx

    ws = dbx.Workspace.from_env()
    rows = ws.sql("SELECT current_date()")
    typer.echo(f"databricks keepalive ok: {rows[0][0] if rows else '?'}")


@bigquery_app.command("run")
def bigquery_run(
    root: Path = typer.Option(DATA_DIR, help="Data directory (run log)."),
    out: Path = typer.Option(Path("cloud-evidence/bigquery.json"), help="Evidence JSON."),
    day: list[str] = typer.Option(
        ["20150101", "20261019"], help="githubarchive.day tables (YYYYMMDD) to measure."
    ),
    rollup: str = typer.Option("20261019", help="Day rolled up in place and exported."),
    budget_gb: float = typer.Option(20.0, help="Stop before this run bills more (GB)."),
) -> None:
    """Dry-run bytes per column set, day counts, one in-place rollup and its export."""
    from keystone.cloud import bigquery as bq

    sb = bq.Sandbox.from_env()
    sb.budget = int(budget_gb * 1e9)
    doc: dict[str, Any] = {
        "project": sb.project,
        "guards": {
            "max_bytes_billed_per_query": bq.QUERY_CAP_BYTES,
            "run_budget_bytes": sb.budget,
            "month_budget_bytes": bq.MONTH_BUDGET_BYTES,
            "free_tier_bytes": bq.FREE_TIER_BYTES,
        },
    }
    with Stage(root, "cloud.bigquery", "bigquery", "sandbox") as st:
        try:
            sb.month_start = sb.month_billed()
        except bq.BigQueryError as exc:
            doc["month_billed_error"] = str(exc)[:300]
        doc["month_billed_before"] = sb.month_start
        doc["column_sets"] = bq.column_set_bytes(sb, day)
        try:
            doc["counts"] = [bq.day_count(sb, d) for d in day]
            doc["rollup"] = bq.rollup_day(
                sb, rollup, root / "cloud" / f"bq_repo_day_{rollup}.parquet"
            )
            st.records_out = doc["rollup"]["exported_rows"]
        except bq.BudgetStop as exc:
            doc["stopped"] = str(exc)
        doc["queries"] = sb.log
        doc["billed_this_run"] = sb.billed
        st.bytes_in = sb.billed
    _write(out, doc)
    if "stopped" in doc:
        raise typer.Exit(1)
