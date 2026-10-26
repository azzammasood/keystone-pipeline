"""Databricks Free Edition over its REST API (ADR-067): Unity Catalog staging, the dbt target,
one Lakeflow Declarative Pipeline, lineage from system tables and a keepalive.

The workspace cannot fetch GH Archive, so the runner delivers the dbt sources as Parquet into a
Unity Catalog volume and creates one table per source. The token is read from the environment
and only ever sent in the Authorization header; errors carry the URL path and the API's
message, never headers.
"""

import base64
import os
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import httpx
import yaml

from keystone.paths import REPO_ROOT
from keystone.warehouse.dbt import WarehousePaths

SOURCES_YML = REPO_ROOT / "dbt" / "models" / "staging" / "_sources.yml"
SOURCE_SCHEMA = "keystone_src"  # dbt sources on the databricks target (_sources.yml)
VOLUME = "landing"
LDP_SCHEMA = "keystone_ldp"
LDP_NAME = "keystone-marts"
LDP_FILE = "/Workspace/Shared/keystone/marts_pipeline.sql"
LDP_SQL = Path(__file__).parent / "lakeflow" / "marts_pipeline.sql"
TERMINAL = {"SUCCEEDED", "FAILED", "CANCELED", "CLOSED"}


class DatabricksError(RuntimeError):
    pass


def _host(raw: str) -> str:
    """`DATABRICKS_HOST` with or without the scheme; dbt wants it bare."""
    return re.sub(r"^https?://", "", raw.strip()).rstrip("/")


@dataclass
class Workspace:
    host: str
    token: str
    catalog: str = "workspace"  # Free Edition's default catalog

    @classmethod
    def from_env(cls) -> "Workspace":
        host, token = os.environ.get("DATABRICKS_HOST", ""), os.environ.get("DATABRICKS_TOKEN", "")
        if not host or not token:
            raise DatabricksError("DATABRICKS_HOST and DATABRICKS_TOKEN must be set")
        return cls(_host(host), token, os.environ.get("DATABRICKS_CATALOG", "workspace"))

    def _client(self) -> httpx.Client:
        return httpx.Client(
            base_url=f"https://{self.host}",
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=httpx.Timeout(120.0),
        )

    def api(self, method: str, path: str, **kw: Any) -> Any:
        with self._client() as c:
            r = c.request(method, path, **kw)
        if r.status_code >= 400:
            raise DatabricksError(f"{method} {path}: HTTP {r.status_code}: {r.text[:300]}")
        return r.json() if r.content and "json" in r.headers.get("content-type", "") else None

    # SQL warehouse ------------------------------------------------------------------------
    def warehouse_id(self) -> str:
        found = self.api("GET", "/api/2.0/sql/warehouses").get("warehouses") or []
        if not found:
            raise DatabricksError("no SQL warehouse in the workspace")
        return str(found[0]["id"])

    def http_path(self) -> str:
        return f"/sql/1.0/warehouses/{self.warehouse_id()}"

    def sql(self, statement: str, warehouse: str | None = None) -> list[list[Any]]:
        """Run one statement on the serverless warehouse; rows of the first result chunk."""
        body = {
            "warehouse_id": warehouse or self.warehouse_id(),
            "statement": statement,
            "wait_timeout": "50s",
            "on_wait_timeout": "CONTINUE",
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        }
        res = self.api("POST", "/api/2.0/sql/statements", json=body)
        while res["status"]["state"] not in TERMINAL:
            time.sleep(3)
            res = self.api("GET", f"/api/2.0/sql/statements/{res['statement_id']}")
        state = res["status"]["state"]
        if state != "SUCCEEDED":
            msg = (res["status"].get("error") or {}).get("message", "")[:400]
            raise DatabricksError(f"statement {state}: {msg}")
        return list((res.get("result") or {}).get("data_array") or [])

    def put_file(self, local: Path, remote: str) -> None:
        with self._client() as c:
            r = c.put(
                f"/api/2.0/fs/files{remote}",
                params={"overwrite": "true"},
                content=local.read_bytes(),
                headers={"Content-Type": "application/octet-stream"},
            )
        if r.status_code >= 400:
            raise DatabricksError(f"PUT {remote}: HTTP {r.status_code}: {r.text[:300]}")


def source_tables() -> dict[str, str]:
    """dbt source name -> DuckDB SELECT over the local data (silver Parquet or the rollup)."""
    doc = yaml.safe_load(SOURCES_YML.read_text(encoding="utf-8"))
    out: dict[str, str] = {}
    for src in doc["sources"]:
        for t in src["tables"]:
            loc = (t.get("meta") or {}).get("external_location")
            if loc:
                out[t["name"]] = f"SELECT * FROM {loc}"
            else:  # the rollup DuckDB file, attached as `rollup` (dbt/profiles.yml)
                out[t["name"]] = f"SELECT * FROM rollup.main.{t['name']}"
    return out


def stage(ws: Workspace, paths: WarehousePaths, work: Path) -> dict[str, int]:
    """Each dbt source -> one Parquet file in the volume -> one Unity Catalog table."""
    root = paths.root.resolve().as_posix()
    work.mkdir(parents=True, exist_ok=True)
    wh = ws.warehouse_id()
    ws.sql(f"CREATE SCHEMA IF NOT EXISTS {ws.catalog}.{SOURCE_SCHEMA}", wh)
    ws.sql(f"CREATE VOLUME IF NOT EXISTS {ws.catalog}.{SOURCE_SCHEMA}.{VOLUME}", wh)
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute(f"ATTACH '{paths.rollup_db.resolve().as_posix()}' AS rollup (READ_ONLY)")
    rows: dict[str, int] = {}
    for name, select in source_tables().items():
        select = re.sub(r"\{\{\s*env_var\('KEYSTONE_DATA_ROOT'\)\s*\}\}", root, select)
        local = work / f"{name}.parquet"
        con.execute(f"COPY ({select}) TO '{local.as_posix()}' (FORMAT parquet)")
        n = con.execute(f"SELECT count(*) FROM '{local.as_posix()}'").fetchone()
        rows[name] = int(n[0]) if n else 0
        remote = f"/Volumes/{ws.catalog}/{SOURCE_SCHEMA}/{VOLUME}/{name}.parquet"
        ws.put_file(local, remote)
        ws.sql(
            f"CREATE OR REPLACE TABLE {ws.catalog}.{SOURCE_SCHEMA}.{name} AS "
            f"SELECT * FROM parquet.`{remote}`",
            wh,
        )
    con.close()
    return rows


def dbt_env(ws: Workspace) -> dict[str, str]:
    """What the `databricks` output of dbt/profiles.yml reads (never printed)."""
    return {
        "DATABRICKS_HOST": ws.host,
        "DATABRICKS_HTTP_PATH": ws.http_path(),
        "DATABRICKS_CATALOG": ws.catalog,
    }


def lakeflow(ws: Workspace, timeout_s: int = 1800) -> dict[str, Any]:
    """Create or update the one serverless pipeline over the dbt marts and run one update."""
    ws.api("POST", "/api/2.0/workspace/mkdirs", json={"path": LDP_FILE.rsplit("/", 1)[0]})
    ws.api(
        "POST",
        "/api/2.0/workspace/import",
        json={
            "path": LDP_FILE,
            "format": "SOURCE",
            "language": "SQL",
            "overwrite": True,
            "content": base64.b64encode(LDP_SQL.read_bytes()).decode(),
        },
    )
    spec = {
        "name": LDP_NAME,
        "serverless": True,
        "catalog": ws.catalog,
        "schema": LDP_SCHEMA,
        "continuous": False,
        "development": True,
        "libraries": [{"file": {"path": LDP_FILE}}],
    }
    found = ws.api("GET", "/api/2.0/pipelines", params={"filter": f"name LIKE '{LDP_NAME}'"})
    statuses = (found or {}).get("statuses") or []
    if statuses:
        pid = statuses[0]["pipeline_id"]
        ws.api("PUT", f"/api/2.0/pipelines/{pid}", json={**spec, "id": pid})
    else:
        pid = ws.api("POST", "/api/2.0/pipelines", json=spec)["pipeline_id"]
    upd = ws.api("POST", f"/api/2.0/pipelines/{pid}/updates", json={"full_refresh": True})
    uid = upd["update_id"]
    t0 = time.monotonic()
    state = "QUEUED"
    while time.monotonic() - t0 < timeout_s:
        info = ws.api("GET", f"/api/2.0/pipelines/{pid}/updates/{uid}")["update"]
        state = info["state"]
        if state in {"COMPLETED", "FAILED", "CANCELED"}:
            break
        time.sleep(15)
    out: dict[str, Any] = {"state": state, "seconds": round(time.monotonic() - t0, 1)}
    if state != "COMPLETED":
        ev = ws.api(
            "GET",
            f"/api/2.0/pipelines/{pid}/events",
            params={"max_results": 25, "filter": "level='ERROR'"},
        )
        out["errors"] = [e.get("message", "")[:300] for e in (ev or {}).get("events", [])][:5]
    return out


def lineage(ws: Workspace) -> dict[str, Any]:
    """Unity Catalog lineage edges recorded for the dbt schemas (system tables, if readable)."""
    try:
        rows = ws.sql(
            "SELECT count(*), count(DISTINCT target_table_full_name) "
            "FROM system.access.table_lineage "
            f"WHERE target_table_catalog = '{ws.catalog}' "
            "AND target_table_schema IN ('staging', 'intermediate', 'marts', 'metrics', "
            f"'{LDP_SCHEMA}') AND event_date >= current_date() - INTERVAL 7 DAYS"
        )
    except DatabricksError as exc:
        return {"available": False, "reason": str(exc)[:300]}
    edges, targets = (int(x) for x in rows[0]) if rows else (0, 0)
    return {"available": True, "edges_7d": edges, "target_tables_7d": targets}
