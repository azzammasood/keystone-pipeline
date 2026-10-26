"""Cloud rungs (ADR-067, ADR-068): the parts that run without a workspace or a token."""

from pathlib import Path

import pytest

from keystone.cloud import bigquery as bq
from keystone.cloud import databricks as dbx
from keystone.telemetry.runlog import StageRecord, append, import_runs, read


def test_host_is_bare() -> None:
    assert dbx._host("https://dbc-1.cloud.databricks.com/") == "dbc-1.cloud.databricks.com"


def test_every_dbt_source_is_staged() -> None:
    t = dbx.source_tables()
    assert "repo_hour" in t and t["repo_hour"] == "SELECT * FROM rollup.main.repo_hour"
    assert all("{{" not in v or "KEYSTONE_DATA_ROOT" in v for v in t.values())


def test_budget_stops_before_running(monkeypatch: pytest.MonkeyPatch) -> None:
    sb = bq.Sandbox("p", "t", month_start=0)
    monkeypatch.setattr(sb, "dry_run", lambda sql: bq.QUERY_CAP_BYTES + 1)
    monkeypatch.setattr(sb, "_job", lambda *a, **k: pytest.fail("a job ran"))
    with pytest.raises(bq.BudgetStop):
        sb.query("SELECT 1", label="big")
    monkeypatch.setattr(sb, "dry_run", lambda sql: 1_000)
    sb.month_start = None  # unknown monthly usage: stop too
    with pytest.raises(bq.BudgetStop):
        sb.query("SELECT 1", label="unknown month")


def test_import_runs_dedupes_and_keeps_last_run(tmp_path: Path) -> None:
    mine = StageRecord("run-b", "publish", "keystone", "1", "2026-10-26T10:00:00+00:00")
    append(tmp_path, mine)
    other = tmp_path / "other.jsonl"
    cloud = StageRecord("run-a", "cloud.bigquery", "bigquery", "sandbox", "2026-10-26T11:00:00")
    append(tmp_path / "o", cloud)
    other.write_bytes((tmp_path / "o" / "telemetry" / "runs.jsonl").read_bytes())
    assert import_runs(tmp_path, [other]) == 1
    assert import_runs(tmp_path, [other]) == 0
    assert [r.run_id for r in read(tmp_path)] == ["run-a", "run-b"]
