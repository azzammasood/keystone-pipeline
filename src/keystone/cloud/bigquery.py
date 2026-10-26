"""BigQuery sandbox rung (brief Phase 1 item (a), ADR-068): what the public `githubarchive`
dataset costs per column set, an in-place rollup and its export path, all at $0.

Guards, in order, for every query: a dry run first (free; reports the bytes the query would
scan); refused when that exceeds the per-query cap or what is left of the run budget and of the
monthly free tier after this project's bytes billed this month (INFORMATION_SCHEMA); then the
real job with `maximumBytesBilled` set, so BigQuery itself refuses anything larger. The project
has no billing account (sandbox), so a charge is impossible; the guards keep the free tier too.
Auth: a short-lived access token from keyless Workload Identity Federation, never printed.
"""

import calendar
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb
import httpx
import pyarrow as pa

API = "https://bigquery.googleapis.com/bigquery/v2"
FREE_TIER_BYTES = 1_000_000_000_000  # 1 TB of query bytes per month (sandbox and free tier)
MONTH_BUDGET_BYTES = 100_000_000_000  # Keystone's own stop: 10% of the free tier per month
QUERY_CAP_BYTES = 5_000_000_000  # no single query may bill more than 5 GB
MIN_BILLED = 10_485_760  # BigQuery bills at least 10 MB per table referenced

# Column sets of the public dataset, from what the rollups need to the full payload.
COLUMN_SETS: dict[str, str] = {
    "type": "type",
    "type,repo.id": "type, repo.id",
    "type,repo.id,created_at": "type, repo.id, created_at",
    "type,repo.id,actor.id,created_at": "type, repo.id, actor.id, created_at",
    "payload": "payload",
    "all": "*",
}
_EXPORT = ("activity_date", "repo_id", "event_type", "events")


class BigQueryError(RuntimeError):
    pass


class BudgetStop(BigQueryError):
    """A query would exceed a cap: stop before running it (CLAUDE.md: hard stop)."""


@dataclass
class Sandbox:
    project: str
    token: str
    location: str = "US"  # githubarchive lives in the US multi-region
    billed: int = 0  # bytes billed by this run
    budget: int = 20_000_000_000  # this run's stop
    month_start: int | None = None  # billed this month before the run (None: not readable)
    log: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_env(cls) -> "Sandbox":
        token = os.environ.get("GOOGLE_OAUTH_ACCESS_TOKEN", "")
        if not token:
            raise BigQueryError("GOOGLE_OAUTH_ACCESS_TOKEN is not set (google-github-actions/auth)")
        return cls(os.environ.get("GCP_PROJECT", "keystone-pipeline"), token)

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        with httpx.Client(timeout=120.0, headers={"Authorization": f"Bearer {self.token}"}) as c:
            r = c.post(f"{API}/projects/{self.project}/{path}", json=body)
        data: dict[str, Any] = r.json()
        if r.status_code >= 400:
            raise BigQueryError(f"{path}: HTTP {r.status_code}: {str(data.get('error'))[:300]}")
        return data

    def _get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        with httpx.Client(timeout=120.0, headers={"Authorization": f"Bearer {self.token}"}) as c:
            r = c.get(f"{API}/projects/{self.project}/{path}", params=params)
        data: dict[str, Any] = r.json()
        if r.status_code >= 400:
            raise BigQueryError(f"{path}: HTTP {r.status_code}: {str(data.get('error'))[:300]}")
        return data

    def _job(self, sql: str, dry: bool, cap: int = 0) -> dict[str, Any]:
        q: dict[str, Any] = {"query": sql, "useLegacySql": False, "useQueryCache": False}
        if cap:
            q["maximumBytesBilled"] = str(cap)
        body = {
            "configuration": {"query": q, "dryRun": dry},
            "jobReference": {"projectId": self.project, "location": self.location},
        }
        return self._post("jobs", body)

    def dry_run(self, sql: str) -> int:
        """Bytes the query would process (free; no job runs)."""
        stats = self._job(sql, dry=True, cap=QUERY_CAP_BYTES)["statistics"]
        return int(stats.get("totalBytesProcessed", 0))

    def month_billed(self) -> int:
        """Bytes billed by this project's jobs this calendar month (UTC), summed from jobs.list
        (an API read: no query, nothing billed). Covers the jobs of this identity, the only
        one that queries the sandbox."""
        now = time.gmtime()
        start = calendar.timegm((now.tm_year, now.tm_mon, 1, 0, 0, 0))
        params: dict[str, Any] = {
            "projection": "full",
            "minCreationTime": str(start * 1000),
            "maxResults": 1000,
        }
        total = 0
        while True:
            page = self._get("jobs", params)
            for job in page.get("jobs", []):
                q = (job.get("statistics") or {}).get("query") or {}
                total += int(q.get("totalBytesBilled", 0) or 0)
            if not page.get("nextPageToken"):
                return total
            params["pageToken"] = page["nextPageToken"]

    def query(
        self, sql: str, label: str, check_month: bool = True, max_rows: int = 0
    ) -> tuple[list[list[Any]], dict[str, Any]]:
        """Dry run, budget checks, then the job with maximumBytesBilled; returns rows (all
        pages up to `max_rows`, 0 = the first page only) and the measured record."""
        processed = self.dry_run(sql)
        planned = max(processed, MIN_BILLED)
        cap = min(QUERY_CAP_BYTES, planned + MIN_BILLED)
        if planned > QUERY_CAP_BYTES:
            raise BudgetStop(f"{label}: would scan {processed:,} bytes (> per-query cap)")
        if self.billed + planned > self.budget:
            raise BudgetStop(f"{label}: run budget {self.budget:,} bytes would be exceeded")
        if check_month:
            if self.month_start is None:
                raise BudgetStop(f"{label}: this month's bytes billed are unknown")
            month = self.month_start + self.billed
            if month + planned > min(MONTH_BUDGET_BYTES, FREE_TIER_BYTES):
                raise BudgetStop(f"{label}: monthly budget would be exceeded ({month:,} billed)")
        t0 = time.perf_counter()
        job = self._job(sql, dry=False, cap=cap)
        ref = job["jobReference"]
        params: dict[str, Any] = {"location": ref["location"], "timeoutMs": 60000}
        if max_rows:
            params["maxResults"] = 50000
        res = self._get(f"queries/{ref['jobId']}", params)
        while not res.get("jobComplete"):
            res = self._get(f"queries/{ref['jobId']}", params)
        rows = [[c["v"] for c in r["f"]] for r in res.get("rows", [])]
        pages = 1
        while max_rows and res.get("pageToken") and len(rows) < max_rows:
            res = self._get(f"queries/{ref['jobId']}", {**params, "pageToken": res["pageToken"]})
            rows += [[c["v"] for c in r["f"]] for r in res.get("rows", [])]
            pages += 1
        seconds = round(time.perf_counter() - t0, 2)
        done = self._get(f"jobs/{ref['jobId']}", {"location": ref["location"]})
        q = done["statistics"].get("query", {})
        billed = int(q.get("totalBytesBilled", 0))
        self.billed += billed
        rec = {
            "label": label,
            "dry_run_bytes": processed,
            "bytes_processed": int(q.get("totalBytesProcessed", 0)),
            "bytes_billed": billed,
            "cache_hit": bool(q.get("cacheHit", False)),
            "slot_ms": int(done["statistics"].get("totalSlotMs", 0)),
            "seconds": seconds,
            "rows": int(res.get("totalRows", len(rows))),
            "pages": pages,
        }
        self.log.append(rec)
        return rows, rec


def column_set_bytes(sb: Sandbox, days: list[str]) -> list[dict[str, Any]]:
    """Dry-run bytes per column set and day table (free: nothing runs)."""
    out = []
    for day in days:
        for name, cols in COLUMN_SETS.items():
            sql = f"SELECT {cols} FROM `githubarchive.day.{day}`"
            out.append({"day": day, "columns": name, "dry_run_bytes": sb.dry_run(sql)})
    return out


def rollup_day(sb: Sandbox, day: str, out: Path) -> dict[str, Any]:
    """The repo-day rollup computed in place, then exported by paging the results (the only
    $0 export path: EXPORT DATA needs a Cloud Storage bucket, which needs billing)."""
    sql = (
        "SELECT DATE(created_at) AS activity_date, repo.id AS repo_id, type AS event_type, "
        "COUNT(*) AS events "
        f"FROM `githubarchive.day.{day}` GROUP BY 1, 2, 3"
    )
    rows, rec = sb.query(sql, label=f"rollup_repo_day {day}", max_rows=5_000_000)
    t0 = time.perf_counter()
    out.parent.mkdir(parents=True, exist_ok=True)
    cols = list(zip(*rows, strict=True)) if rows else [(), (), (), ()]
    page = pa.table(  # noqa: F841 (read by DuckDB's replacement scan)
        {n: pa.array(c, pa.string()) for n, c in zip(_EXPORT, cols, strict=True)}
    )
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE t AS SELECT CAST(activity_date AS DATE) AS activity_date, "
        "CAST(repo_id AS BIGINT) AS repo_id, event_type, CAST(events AS BIGINT) AS events "
        "FROM page"
    )
    con.execute(f"COPY t TO '{out.as_posix()}' (FORMAT parquet, COMPRESSION zstd)")
    total = con.execute("SELECT count(*), sum(events) FROM t").fetchone()
    con.close()
    rec.update(
        exported_rows=int(total[0]) if total else 0,
        exported_events=int(total[1] or 0) if total else 0,
        export_seconds=round(time.perf_counter() - t0, 2),
        export_bytes=out.stat().st_size,
    )
    return rec


def day_count(sb: Sandbox, day: str) -> dict[str, Any]:
    """Rows in a day table: COUNT(*) reads table metadata only (0 bytes processed)."""
    rows, rec = sb.query(f"SELECT COUNT(*) FROM `githubarchive.day.{day}`", label=f"count {day}")
    rec["events"] = int(rows[0][0]) if rows else 0
    return rec
