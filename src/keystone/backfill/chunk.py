"""One backfill chunk: fetch, parse in parallel, roll up per hour, merge exactly, delete.

Hours are fetched by the polite client (2 connections, policy rate) and handed to a process
pool. Each worker parses its hour to silver (salted actor keys), runs the two rollup queries of
`rollup/sql` over that hour alone into partial Parquet, then deletes the raw file and the
silver. Rollup measures are sums and maxima, so merging the partials by key is exact. Event
time and file hour differ at the edges: the chunk also reads `overlap_hours` past its end and
keeps only windows inside `[start, end)`; what still falls outside is counted, never guessed.

Only `repo_hour` and the key-free contributor table leave the job; `repo_actor_day` partials
(salted keys) stay in the work directory, which the runner discards.
"""

import asyncio
import os
import shutil
import sys
import threading
import time
from concurrent.futures import Future, ProcessPoolExecutor
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from datetime import time as dtime
from pathlib import Path
from typing import Any

import duckdb

from keystone import __version__
from keystone.fetch.client import DiskGuard, PoliteClient
from keystone.fetch.manifest import FileRecord, Manifest
from keystone.parsers import drift as driftmod
from keystone.parsers.parquet import GhArchiveSink
from keystone.parsers.privacy import ActorHasher, salt_from_env
from keystone.policy import load_policy
from keystone.rollup.contract import duckdb_projection
from keystone.rollup.spec import Rollup, _agg_sql, duckdb_select, input_sql, rollups
from keystone.sources import gharchive as gha
from keystone.sources import load_sources

ASSET_LIMIT = int(1.5 * 2**30)  # ADR-039: keep a margin under GitHub's 2 GiB per asset
CONTRIBUTORS_SQL = (
    "SELECT window_start, repo_id, era, count(*) AS human_actors, "
    "CAST(sum(events) AS BIGINT) AS human_events, max(events) AS top_actor_events "
    "FROM repo_actor_day GROUP BY window_start, repo_id, era"
)

_SINK: GhArchiveSink | None = None
_WORK: Path | None = None


def _init_worker(work: str) -> None:
    global _SINK, _WORK
    _WORK = Path(work)
    _SINK = GhArchiveSink(
        load_sources().gharchive,
        _WORK / "silver",
        _WORK / "quarantine",
        _WORK / "drift",
        ActorHasher(salt_from_env()),
        driftmod.load_registry(),
    )


def _sql_path(p: Path) -> str:
    return p.as_posix().replace("'", "''")


def parse_hour(path: Path, rec: FileRecord) -> dict[str, Any]:
    """Worker: parse one verified hour, write its partial rollups, delete raw and silver."""
    assert _SINK is not None and _WORK is not None and rec.hour is not None
    stem = path.name.removesuffix(".json.gz")
    before = dict(_SINK.totals)
    out: dict[str, Any] = {"hour": rec.hour.isoformat(), "error": None}
    try:
        _SINK(path, rec)
        events = _WORK / "silver" / "events" / f"date={rec.hour:%Y-%m-%d}" / f"{stem}.parquet"
        con = duckdb.connect()
        con.execute("SET TimeZone = 'UTC'; SET threads = 1; SET enable_progress_bar = false")
        if events.exists():  # an hour may hold only quarantined rows: no partials then
            src = f"read_parquet('{_sql_path(events)}')"
            con.execute(f"CREATE VIEW events AS SELECT {duckdb_projection()} FROM {src}")
            con.execute(f"CREATE VIEW rollup_input AS {input_sql()}")
            for r in rollups():
                dest = _WORK / "partials" / r.name / f"{stem}.parquet"
                dest.parent.mkdir(parents=True, exist_ok=True)
                con.execute(
                    f"COPY (SELECT *, TIMESTAMPTZ '{rec.hour.isoformat()}' AS file_hour "
                    f"FROM ({duckdb_select(r)})) TO '{_sql_path(dest)}' (FORMAT parquet)"
                )
        con.close()
    except driftmod.DriftError as exc:
        out["error"] = f"drift: {exc}"[:400]
    except Exception as exc:  # one bad hour is recorded, not fatal to the chunk
        out["error"] = f"{type(exc).__name__}: {exc}"[:400]
    finally:
        path.unlink(missing_ok=True)
        for sub in ("silver", "quarantine"):
            for f in (_WORK / sub).rglob(f"{stem}.parquet"):
                f.unlink(missing_ok=True)
    out.update({k: _SINK.totals[k] - before[k] for k in ("events", "quarantined", "bytes_in")})
    return out


def _peak_rss_mb() -> dict[str, float | None]:
    if sys.platform == "win32":
        return {"self": None, "children": None}
    import resource

    kb = 1024  # Linux reports ru_maxrss in KiB
    return {
        "self": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * kb / 1e6,
        "children": resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss * kb / 1e6,
    }


def _merge_sql(r: Rollup, start: datetime, end: datetime, glob: str) -> str:
    measures = ", ".join(_agg_sql(r, m) for m in r.measures)
    keys = ", ".join(r.keys)
    return (
        f"SELECT window_start, {keys}, {measures} FROM read_parquet('{glob}') "
        f"WHERE window_start >= TIMESTAMPTZ '{start.isoformat()}' "
        f"AND window_start < TIMESTAMPTZ '{end.isoformat()}' "
        f"GROUP BY window_start, {keys}"
    )


def merge(work: Path, out: Path, name: str, start: datetime, end: datetime,
          overlap_end: datetime) -> dict[str, Any]:  # fmt: skip
    """Merge the partials into the chunk's two published files; count edge events."""
    out.mkdir(parents=True, exist_ok=True)
    (work / "tmp").mkdir(exist_ok=True)
    con = duckdb.connect(str(work / "merge.duckdb"))
    con.execute(f"SET TimeZone = 'UTC'; SET temp_directory = '{_sql_path(work / 'tmp')}'")
    con.execute("SET preserve_insertion_order = false; SET enable_progress_bar = false")
    stats: dict[str, Any] = {"rows": {}, "bytes": {}}
    by_name = {r.name: r for r in rollups()}
    for table in ("repo_hour", "repo_actor_day"):
        part = work / "partials" / table
        if not any(part.glob("*.parquet")):
            raise RuntimeError(f"no partials for {table}: no hour parsed")
        con.execute(
            f"CREATE TABLE {table} AS "
            f"{_merge_sql(by_name[table], start, end, _sql_path(part) + '/*.parquet')}"
        )
    # Edge accounting from repo_hour partials (one row per hour file, window and key):
    # before start but filed after the previous chunk's overlap, or after end but filed before
    # end, are events no chunk keeps. Inside the first and last chunk of a window, "before" is
    # also outside the window by design.
    ph = _sql_path(work / "partials" / "repo_hour") + "/*.parquet"
    s, e = f"TIMESTAMPTZ '{start.isoformat()}'", f"TIMESTAMPTZ '{end.isoformat()}'"
    ov = f"TIMESTAMPTZ '{(start + (overlap_end - end)).isoformat()}'"
    row = con.execute(
        f"""SELECT
            coalesce(sum(events) FILTER (WHERE window_start >= {s} AND window_start < {e}), 0),
            coalesce(sum(events) FILTER (WHERE window_start < {s} AND file_hour < {ov}), 0),
            coalesce(sum(events) FILTER (WHERE window_start < {s} AND file_hour >= {ov}), 0),
            coalesce(sum(events) FILTER (WHERE window_start >= {e} AND file_hour >= {e}), 0),
            coalesce(sum(events) FILTER (WHERE window_start >= {e} AND file_hour < {e}), 0),
            strftime(min(window_start) FILTER (WHERE window_start >= {s}), '%Y-%m-%dT%H:%MZ'),
            strftime(max(window_start) FILTER (WHERE window_start < {e}), '%Y-%m-%dT%H:%MZ')
        FROM read_parquet('{ph}')"""
    ).fetchone()
    assert row is not None
    stats["events"] = {
        "in_chunk": int(row[0]),
        "before_start_in_previous_overlap": int(row[1]),
        "before_start_lost": int(row[2]),
        "after_end_next_chunk": int(row[3]),
        "after_end_lost": int(row[4]),
    }
    stats["first_window"], stats["last_window"] = row[5], row[6]
    eras = con.execute("SELECT DISTINCT era FROM repo_hour ORDER BY 1").fetchall()
    stats["eras"] = [x[0] for x in eras]
    outputs = {
        "repo_hour": "SELECT * FROM repo_hour ORDER BY window_start, repo_id, era, event_type",
        "contributors": f"SELECT * FROM ({CONTRIBUTORS_SQL}) ORDER BY window_start, repo_id, era",
    }
    for label, sql in outputs.items():
        dest = out / f"{label}-{name}.parquet"
        con.execute(
            f"COPY ({sql}) TO '{_sql_path(dest)}' "
            "(FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1000000)"
        )
        n = con.execute(f"SELECT count(*) FROM read_parquet('{_sql_path(dest)}')").fetchone()
        stats["rows"][label] = int(n[0]) if n else 0
        stats["bytes"][label] = dest.stat().st_size
        if dest.stat().st_size > ASSET_LIMIT:
            raise RuntimeError(f"{dest.name} is over 1.5 GiB: rerun with a smaller --chunk-days")
    rad = con.execute("SELECT count(*) FROM repo_actor_day").fetchone()
    stats["rows"]["repo_actor_day_in_job"] = int(rad[0]) if rad else 0  # count only
    con.close()
    return stats


def run_chunk(window: str, start: date, end: date, work: Path, out: Path, *,
              workers: int, overlap_hours: int = 2, max_pending: int = 0,
              now: datetime | None = None) -> dict[str, Any]:  # fmt: skip
    """Fetch, parse and merge one chunk; returns the chunk record (written by the caller)."""
    t0 = time.monotonic()
    begin = datetime.combine(start, dtime(), UTC)
    finish = datetime.combine(end, dtime(), UTC)
    overlap_end = finish + timedelta(hours=overlap_hours)
    name = f"{start:%Y%m%d}-{end:%Y%m%d}"
    if work.exists():
        shutil.rmtree(work)
    raw = work / "raw"
    raw.mkdir(parents=True)
    salt_from_env()  # fail before any download when the salt is missing
    cfg = load_sources().gharchive
    policy = load_policy()
    max_pending = max_pending or workers * 2
    slots = threading.BoundedSemaphore(max_pending)
    disk = DiskGuard(10 * 10**9)
    futures: list[Future[dict[str, Any]]] = []
    pool = ProcessPoolExecutor(workers, initializer=_init_worker, initargs=(str(work),))

    def on_file(path: Path, rec: FileRecord) -> None:
        slots.acquire()  # backpressure: blocks the fetch loop while the pool is full
        size = rec.bytes or 0
        fut = pool.submit(parse_hour, path, rec)

        def done(_: Future[dict[str, Any]]) -> None:
            disk.release(size)
            slots.release()

        fut.add_done_callback(done)
        futures.append(fut)

    async def fetch() -> gha.RunSummary:
        manifest = Manifest(work / "manifest.duckdb")
        try:
            async with PoliteClient(policy) as client:
                return await gha.ingest_hours(
                    client, manifest, cfg, begin, overlap_end, raw, disk,
                    keep=True, on_file=on_file, now=now,
                )  # fmt: skip
        finally:
            manifest.close()

    try:
        summary = asyncio.run(fetch())
        fetch_s = time.monotonic() - t0
        results = [f.result() for f in futures]
    finally:
        pool.shutdown(wait=True)
    parse_s = time.monotonic() - t0
    manifest = Manifest(work / "manifest.duckdb")
    try:
        records = manifest.records(gha.SOURCE, begin, overlap_end)
    finally:
        manifest.close()
    failed = sorted(r["hour"] for r in results if r["error"])
    fetch_errors = sorted(h.isoformat() for h, r in records.items() if r.status == "error")
    merged = merge(work, out, name, begin, finish, overlap_end)
    total_s = time.monotonic() - t0
    hours_in_chunk = int((finish - begin).total_seconds() // 3600)
    complete = not failed and not fetch_errors and summary.skipped_unpublished == 0
    record: dict[str, Any] = {
        "record_version": 1,
        "window": window,
        "chunk": name,
        "start": begin.isoformat(),
        "end": finish.isoformat(),
        "overlap_hours": overlap_hours,
        "complete": complete,
        "generated_at": datetime.now(UTC).isoformat(),
        "keystone_version": __version__,
        "duckdb_version": duckdb.__version__,
        "runner": {"cpus": os.cpu_count(), "workers": workers, "platform": sys.platform},
        "hours": {
            "in_chunk": hours_in_chunk,
            "fetched": len(records),
            "status": dict(summary.counts),
            "skipped_unpublished": summary.skipped_unpublished,
            "parse_failed": failed,
            "parse_errors": sorted({r["error"] for r in results if r["error"]})[:20],
            "fetch_errors": fetch_errors,
            "missing": sorted(h.isoformat() for h, r in records.items() if r.status == "missing"),
        },
        "download": {"bytes": summary.downloaded_bytes, "requests": summary.requests},
        "parse": {
            "events": sum(r["events"] for r in results),
            "quarantined": sum(r["quarantined"] for r in results),
            "bytes_in": sum(r["bytes_in"] for r in results),
        },
        **merged,
        "seconds": {
            "fetch": round(fetch_s, 1),
            "fetch_and_parse": round(parse_s, 1),
            "total": round(total_s, 1),
        },
        "peak_rss_mb": _peak_rss_mb(),
        "cost_usd": 0,
        # Repository-level fetch manifest: URLs, sizes, checksums; no person data.
        "manifest": [
            {
                k: v
                for k, v in asdict(r).items()
                if k in ("url", "status", "bytes", "md5", "sha256", "etag", "last_modified")
            }
            for _, r in sorted(records.items())
        ],  # fmt: skip
    }
    for m in record["manifest"]:
        if isinstance(m.get("last_modified"), datetime):
            m["last_modified"] = m["last_modified"].isoformat()
    return record
