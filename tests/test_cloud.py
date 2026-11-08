"""Cloud rungs (ADR-067, ADR-068): the parts that run without a workspace or a token."""

from pathlib import Path
from typing import Any

import httpx
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


def _workspace(handler: Any) -> Any:
    return dbx.Workspace("dbc.example", "t", transport=httpx.MockTransport(handler))


def test_keepalive_starts_a_stopped_warehouse_then_runs_sql() -> None:
    state = {"w": "STOPPED", "started": False}

    def handler(req: httpx.Request) -> httpx.Response:
        p = req.url.path
        if p.endswith("/Me"):
            return httpx.Response(200, json={"userName": "owner"})
        if p == "/api/2.0/sql/warehouses":
            return httpx.Response(200, json={"warehouses": [{"id": "w1"}]})
        if p.endswith("/start"):
            state["started"], state["w"] = True, "RUNNING"
            return httpx.Response(200, json={})
        if p == "/api/2.0/sql/warehouses/w1":
            return httpx.Response(200, json={"state": state["w"]})
        if p == "/api/2.0/sql/statements":
            body = {"status": {"state": "SUCCEEDED"}, "result": {"data_array": [["2026-11-08"]]}}
            return httpx.Response(200, json=body)
        return httpx.Response(404)

    res = dbx.keepalive(_workspace(handler), poll_s=0)
    assert state["started"] and res.sql_ok and res.warehouse_state == "RUNNING"


def test_keepalive_tolerates_refused_compute_but_not_auth() -> None:
    def refused(req: httpx.Request) -> httpx.Response:
        p = req.url.path
        if p.endswith("/Me"):
            return httpx.Response(200, json={"userName": "owner"})
        if p == "/api/2.0/sql/warehouses":
            return httpx.Response(200, json={"warehouses": [{"id": "w1"}]})
        if p == "/api/2.0/sql/warehouses/w1":
            return httpx.Response(200, json={"state": "RUNNING"})
        msg = "The request could not be processed by the warehouse."
        return httpx.Response(400, json={"error_code": "BAD_REQUEST", "message": msg})

    res = dbx.keepalive(_workspace(refused), poll_s=0)
    assert not res.sql_ok and "HTTP 400" in res.note

    def expired(req: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"message": "invalid token"})

    with pytest.raises(dbx.DatabricksError, match="HTTP 401"):
        dbx.keepalive(_workspace(expired), poll_s=0)
