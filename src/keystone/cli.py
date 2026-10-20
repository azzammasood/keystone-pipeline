"""Keystone CLI. Orchestrators (GitHub Actions, Airflow) only call these commands."""

import asyncio
import json as jsonlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import typer

from keystone import __version__
from keystone.fetch.client import DiskBudgetExceeded, DiskGuard, PoliteClient
from keystone.fetch.manifest import FileRecord, Manifest
from keystone.machine import measure
from keystone.parsers import drift as driftmod
from keystone.parsers.gharchive import parse_file
from keystone.parsers.parquet import GhArchiveSink
from keystone.parsers.privacy import ActorHasher, SaltMissing, salt_from_env
from keystone.paths import DATA_DIR, QUALITY_DIR, QUARANTINE_DIR, REPO_ROOT, SILVER_DIR
from keystone.policy import load_policy
from keystone.sources import GhArchiveConfig, load_sources, osv
from keystone.sources import gharchive as gha

app = typer.Typer(no_args_is_help=True, add_completion=False)
ingest_app = typer.Typer(no_args_is_help=True, help="Polite, resumable ingestion (Phase 2).")
app.add_typer(ingest_app, name="ingest")
parse_app = typer.Typer(no_args_is_help=True, help="Era-aware parsing to Parquet (Phase 3).")
app.add_typer(parse_app, name="parse")

MANIFEST_PATH = DATA_DIR / "manifest" / "manifest.duckdb"
GHARCHIVE_DIR = DATA_DIR / "raw" / "gharchive"
OSV_DIR = DATA_DIR / "raw" / "osv"
GHARCHIVE_SILVER = SILVER_DIR / "gharchive"
GHARCHIVE_QUARANTINE = QUARANTINE_DIR / "gharchive"
DRIFT_DIR = QUALITY_DIR / "drift"


def _version(value: bool) -> None:
    if value:
        typer.echo(f"keystone {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(False, "--version", callback=_version, is_eager=True),
) -> None:
    """Open-source supply-chain risk radar built on GH Archive."""


@app.command()
def info() -> None:
    """Show version, repo root and client policy."""
    policy = load_policy()
    typer.echo(f"keystone {__version__}")
    typer.echo(f"root: {REPO_ROOT}")
    typer.echo(f"user agent: {policy.user_agent}")


@app.command()
def machine(json: bool = typer.Option(False, "--json", help="Print JSON.")) -> None:
    """Measure disk, memory, CPUs and Docker against the disk budget."""
    report = measure(REPO_ROOT, load_policy().disk_budget_fraction)
    if json:
        typer.echo(report.model_dump_json(indent=2))
        return
    for key, value in report.model_dump().items():
        typer.echo(f"{key}: {value}")


def _when(value: str) -> datetime:
    """`2026-10-19` or `2026-10-19T05` (UTC)."""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise typer.BadParameter(f"not an ISO date or hour: {value}") from exc
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _range(start: str, end: str | None) -> tuple[datetime, datetime]:
    begin = _when(start)
    finish = _when(end) if end else begin + timedelta(days=1)
    if finish <= begin:
        raise typer.BadParameter("--end must be after --start")
    return begin, finish


def _disk_guard(max_gb: float | None) -> DiskGuard:
    policy = load_policy()
    budget = measure(REPO_ROOT, policy.disk_budget_fraction).disk_budget_gb
    if max_gb is not None:
        budget = min(budget, max_gb)
    return DiskGuard(int(budget * 1e9))


@ingest_app.command("gharchive")
def ingest_gharchive(
    start: str = typer.Option(..., help="First hour or day, UTC (2026-10-19 or 2026-10-19T05)."),
    end: str | None = typer.Option(None, help="Exclusive end, UTC. Default: start + 1 day."),
    keep: bool = typer.Option(False, help="Keep verified files under data/raw (else delete)."),
    head_only: bool = typer.Option(False, help="HEAD only: sizes and ETags, no download."),
    max_gb: float | None = typer.Option(None, help="Disk cap in GB (never above the budget)."),
    parse: bool = typer.Option(True, help="Parse each verified hour to Parquet (Phase 3)."),
) -> None:
    """Check GH Archive hours: conditional GET, MD5 check, manifest update, parse, delete."""
    begin, finish = _range(start, end)
    cfg = load_sources().gharchive
    disk = _disk_guard(max_gb)
    sink = _sink(cfg) if parse and not head_only else None

    async def run() -> gha.RunSummary:
        manifest = Manifest(MANIFEST_PATH)
        try:
            async with PoliteClient(load_policy()) as client:
                return await gha.ingest_hours(
                    client,
                    manifest,
                    cfg,
                    begin,
                    finish,
                    GHARCHIVE_DIR,
                    disk,
                    keep=keep,
                    head_only=head_only,
                    on_file=sink,
                    is_done=sink.is_done if sink is not None else None,
                )
        finally:
            manifest.close()

    try:
        summary = asyncio.run(run())
    except DiskBudgetExceeded as exc:
        typer.echo(f"stopped: {exc}", err=True)
        raise typer.Exit(2) from exc
    typer.echo(summary.line())
    if sink is not None:
        typer.echo(" ".join(f"parsed_{k}={v}" for k, v in sink.totals.items()))
    if summary.counts.get("error"):
        raise typer.Exit(1)


def _sink(cfg: GhArchiveConfig) -> GhArchiveSink:
    try:
        hasher = ActorHasher(salt_from_env())
    except SaltMissing as exc:
        typer.echo(f"stopped: {exc}", err=True)
        raise typer.Exit(2) from exc
    return GhArchiveSink(
        cfg,
        GHARCHIVE_SILVER,
        GHARCHIVE_QUARANTINE,
        DRIFT_DIR,
        hasher,
        driftmod.load_registry(),
    )


def _kept_hours(start: str, end: str | None) -> list[tuple[Path, FileRecord]]:
    """Kept raw hours in the range whose manifest SHA-256 matches the local file."""
    begin, finish = _range(start, end)
    manifest = Manifest(MANIFEST_PATH)
    try:
        rows = manifest.records(gha.SOURCE, begin, finish)
    finally:
        manifest.close()
    out = []
    for hour, rec in sorted(rows.items()):
        path = GHARCHIVE_DIR / f"{hour:%Y}" / f"{hour:%m}" / gha.hour_name(hour)
        if not path.exists() or rec.sha256 is None:
            continue
        if gha.sha256_file(path) != rec.sha256:
            typer.echo(f"skip {path.name}: local copy differs from the manifest", err=True)
            continue
        out.append((path, rec))
    if not out:
        typer.echo(
            f"no kept, verified hours in {begin:%Y-%m-%dT%H} .. {finish:%Y-%m-%dT%H}", err=True
        )
    return out


@parse_app.command("gharchive")
def parse_gharchive(
    start: str = typer.Option(..., help="First hour or day, UTC."),
    end: str | None = typer.Option(None, help="Exclusive end, UTC. Default: start + 1 day."),
) -> None:
    """Parse kept, verified raw hours (data/raw) to Parquet; drift fails the hour."""
    sink = _sink(load_sources().gharchive)
    failed = 0
    for path, rec in _kept_hours(start, end):
        try:
            sink(path, rec)
        except driftmod.DriftError as exc:
            failed += 1
            typer.echo(f"drift: {exc}", err=True)
        except Exception as exc:  # one bad file (corrupt gzip, ...) must not stop the batch
            failed += 1
            typer.echo(f"error: {path.name}: {exc!r}"[:300], err=True)
    typer.echo(" ".join(f"parsed_{k}={v}" for k, v in sink.totals.items()) + f" failed={failed}")
    if failed:
        raise typer.Exit(1)


@parse_app.command("files")
def parse_files(
    files: list[Path] = typer.Argument(..., help="Hourly files named YYYY-MM-DD-H.json.gz."),
    root: Path = typer.Option(DATA_DIR, help="Data directory to write silver/ and quarantine/."),
) -> None:
    """Parse local hour files outside the manifest (fixtures, CI). Lineage: the file's own
    SHA-256 and modification time stand in for the manifest's fetch facts."""
    cfg = load_sources().gharchive
    sink = _sink(cfg)
    sink.silver_dir = root / "silver" / "gharchive"
    sink.quarantine_dir = root / "quarantine" / "gharchive"
    sink.drift_dir = root / "quality" / "drift"
    failed = 0
    for path in files:
        day, _, h = path.name.removesuffix(".json.gz").rpartition("-")
        hour = datetime.fromisoformat(day).replace(hour=int(h), tzinfo=UTC)
        rec = FileRecord(
            url=gha.hour_url(cfg, hour),
            source=gha.SOURCE,
            fetched_at=datetime.fromtimestamp(path.stat().st_mtime, UTC),
            status="new",
            hour=hour,
            sha256=gha.sha256_file(path),
        )
        try:
            sink(path, rec)
        except driftmod.DriftError as exc:
            failed += 1
            typer.echo(f"drift: {exc}", err=True)
        except Exception as exc:  # one bad file (corrupt gzip, ...) must not stop the batch
            failed += 1
            typer.echo(f"error: {path.name}: {exc!r}"[:300], err=True)
    typer.echo(" ".join(f"parsed_{k}={v}" for k, v in sink.totals.items()) + f" failed={failed}")
    if failed:
        raise typer.Exit(1)


@parse_app.command("learn-drift")
def parse_learn_drift(
    start: str = typer.Option(..., help="First hour or day, UTC."),
    end: str | None = typer.Option(None, help="Exclusive end, UTC. Default: start + 1 day."),
) -> None:
    """Add the key paths and types of kept hours to the drift registry (review the diff)."""
    cfg = load_sources().gharchive
    sink = _sink(cfg)
    registry = driftmod.load_registry()
    for path, rec in _kept_hours(start, end):
        ctx = sink.context(rec)
        result = parse_file(path, ctx, sink.hasher, None)
        shape = [f for f in result.drift if f.kind == "era_shape"]
        if shape:
            typer.echo(f"not learned, era shape: {shape[0].line()}", err=True)
            raise typer.Exit(1)
        if result.quarantine:  # learn only from hours whose every record parsed
            typer.echo(f"not learned, {len(result.quarantine)} quarantined: {path.name}", err=True)
            raise typer.Exit(1)
        added = result.observer.merge_into(registry, ctx.era)
        typer.echo(f"{path.name} {ctx.era}: {result.lines} lines, {added} new entries")
    driftmod.save_registry(registry)


@ingest_app.command("gaps")
def ingest_gaps(
    start: str = typer.Option(..., help="First hour or day, UTC."),
    end: str | None = typer.Option(None, help="Exclusive end, UTC. Default: start + 1 day."),
    json: bool = typer.Option(False, "--json", help="Print JSON lines."),
) -> None:
    """Report unchecked, missing, near-empty, small, late and rewritten hours with their period."""
    begin, finish = _range(start, end)
    cfg = load_sources().gharchive
    manifest = Manifest(MANIFEST_PATH)
    try:
        findings = gha.find_gaps(manifest, cfg, begin, finish)
    finally:
        manifest.close()
    for f in findings:
        if json:
            row = {
                "hour": f.hour.isoformat(),
                "period": f.period,
                "kind": f.kind,
                "detail": f.detail,
            }
            typer.echo(jsonlib.dumps(row))
        else:
            typer.echo(f"{f.hour:%Y-%m-%dT%H}Z  {f.period:<4} {f.kind:<10} {f.detail}")
    typer.echo(f"findings={len(findings)}", err=True)


@ingest_app.command("osv")
def ingest_osv(
    zips: bool = typer.Option(True, help="Also fetch per-ecosystem all.zip files."),
    ecosystem: list[str] | None = typer.Option(
        None, help="Ecosystem zip to fetch (repeatable). Default: sources.yml list."
    ),
    max_gb: float | None = typer.Option(None, help="Disk cap in GB (never above the budget)."),
) -> None:
    """OSV increment from the head of modified_id.csv, plus per-ecosystem zips."""
    cfg = load_sources().osv
    disk = _disk_guard(max_gb)

    async def run() -> bool:
        ok = True
        manifest = Manifest(MANIFEST_PATH)
        try:
            async with PoliteClient(load_policy()) as client:
                inc = await osv.snapshot_increment(client, manifest, cfg, OSV_DIR)
                typer.echo(
                    f"modified_id.csv: new_rows={inc.new_rows} bytes_read={inc.bytes_read} "
                    f"chunks={inc.chunks} restarts={inc.restarts} "
                    f"quarantined={inc.quarantined} cursor={inc.cursor}"
                )
                if zips:
                    for z in await osv.fetch_zips(
                        client, manifest, cfg, ecosystem or cfg.ecosystems, OSV_DIR, disk
                    ):
                        typer.echo(f"{z.ecosystem}/all.zip: {z.status} bytes={z.bytes}")
                        ok = ok and z.status != "error"
        finally:
            manifest.close()
        return ok

    try:
        ok = asyncio.run(run())
    except DiskBudgetExceeded as exc:
        typer.echo(f"stopped: {exc}", err=True)
        raise typer.Exit(2) from exc
    if not ok:
        raise typer.Exit(1)


@ingest_app.command("status")
def ingest_status() -> None:
    """Manifest counts per source and latest status."""
    manifest = Manifest(MANIFEST_PATH)
    try:
        rows = manifest.db.execute(
            "SELECT source, status, count(*), sum(bytes) FROM files GROUP BY ALL ORDER BY ALL"
        ).fetchall()
    finally:
        manifest.close()
    for source, status, n, size in rows:
        typer.echo(f"{source:<10} {status:<10} {n:>7} files {size or 0:>15,} B")


quality_app = typer.Typer(no_args_is_help=True, help="Contracts and expectations (Phase 3).")
app.add_typer(quality_app, name="quality")

# Contract server paths start with `data/`; `--root` points them at another data directory.
_TABLE_DIRS = {
    "events": ("silver", "gharchive", "events"),
    "hour_types": ("silver", "gharchive", "hour_types"),
    "quarantine": ("quarantine", "gharchive"),
}


def _glob(root: Path, rel: str) -> tuple[str, bool]:
    path = root / Path(rel).relative_to("data")
    return path.as_posix(), any(path.parent.parent.glob("*/*.parquet"))


@quality_app.command("contracts")
def quality_contracts(
    root: Path = typer.Option(DATA_DIR, help="Data directory holding silver/ and quarantine/."),
    require_all: bool = typer.Option(False, help="Fail if any contract's table has no rows (CI)."),
) -> None:
    """Test every ODCS contract in contracts/ with datacontract-cli; fail on any violation."""
    from keystone.quality.contracts import contract_files, load_contract, run_contract

    bad = 0
    for path in contract_files():
        rows, present = _glob(root, load_contract(path)["servers"][0]["path"])
        if not present:
            typer.echo(f"{path.name}: no rows under {rows} (skipped)")
            bad += require_all
            continue
        result = run_contract(path, rows)
        status = "ok" if result.ok else "FAILED"
        typer.echo(f"{path.name}: {status} ({len(result.checks)} checks)")
        for c in result.problems:
            bad += 1
            typer.echo(f"  {c.result}: {c.field or ''} {c.name} | {c.reason}"[:300])
    if bad:
        raise typer.Exit(1)


@quality_app.command("gx")
def quality_gx(
    root: Path = typer.Option(DATA_DIR, help="Data directory holding silver/ and quarantine/."),
) -> None:
    """Validate every parsed Parquet file with its Great Expectations suite."""
    from keystone.quality.expectations import Validator

    validator = Validator()
    files = bad = 0
    for table, parts in _TABLE_DIRS.items():
        for path in sorted(root.joinpath(*parts).glob("*/*.parquet")):
            files += 1
            result = validator.validate(table, path)
            if not result.ok:
                bad += 1
                typer.echo(f"FAILED {table} {path.name}: {'; '.join(result.failed)}"[:500])
    typer.echo(f"gx files={files} failed={bad}")
    if bad:
        raise typer.Exit(1)


rollup_app = typer.Typer(no_args_is_help=True, help="Rollup engines: DuckDB and Flink (Phase 4).")
app.add_typer(rollup_app, name="rollup")
stream_app = typer.Typer(no_args_is_help=True, help="Redpanda replay of silver hours (Phase 4).")
app.add_typer(stream_app, name="stream")
ROLLUP_DIR = DATA_DIR / "rollup"


def _log_run(root: Path, record: dict[str, object]) -> None:
    """Append one measured run to `rollup/runs.jsonl` (the pipeline-metrics feed, Phase 6)."""
    path = root / "rollup" / "runs.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"at": datetime.now(UTC).isoformat(), **record}
    with path.open("a", encoding="utf-8") as fh:
        fh.write(jsonlib.dumps(record, default=str) + "\n")


@rollup_app.command("run")
def rollup_run(
    root: Path = typer.Option(DATA_DIR, help="Data directory holding silver/; state in rollup/."),
    json: bool = typer.Option(False, "--json", help="Print JSON."),
) -> None:
    """`free` engine: incremental DuckDB rollup of new, re-parsed or removed silver hours."""
    from dataclasses import asdict

    from keystone.rollup.duck import DuckRollup

    duck = DuckRollup(root / "rollup" / "rollup.duckdb", root / "silver" / "gharchive")
    try:
        stats = duck.run()
        counts = duck.counts()
    finally:
        duck.close()
    out = {"engine": "duckdb", **asdict(stats), "table_rows": counts}
    _log_run(root, out)
    if json:
        typer.echo(jsonlib.dumps(out, indent=2))
        return
    typer.echo(
        f"changed={stats.changed_files} removed={stats.removed_files} inputs={stats.input_files} "
        f"windows={stats.windows} written={stats.rows_written} seconds={stats.seconds:.1f}"
    )
    typer.echo(" ".join(f"{k}={v}" for k, v in counts.items()))


@stream_app.command("check-schema")
def stream_check_schema(
    schema: Path = typer.Option(None, help="Candidate .avsc (default: the contract)."),
    topic: str = typer.Option("keystone.gharchive.events.v1", help="Topic whose subject to check."),
) -> None:
    """Ask the schema registry whether a schema may follow the subject's latest version."""
    from keystone.rollup import stream as st
    from keystone.rollup.contract import schema_text

    text = schema.read_text(encoding="utf-8") if schema else schema_text()
    ok = st.Registry().is_compatible(st.subject_for(topic), text)
    typer.echo(f"{st.subject_for(topic)}: {'compatible' if ok else 'INCOMPATIBLE'}")
    if not ok:
        raise typer.Exit(1)


@stream_app.command("produce")
def stream_produce(
    root: Path = typer.Option(DATA_DIR, help="Data directory holding silver/."),
    topic: str = typer.Option("keystone.gharchive.events.v1", help="Kafka topic."),
    fresh: bool = typer.Option(False, help="Delete and recreate the topic first."),
    shuffle_seed: int = typer.Option(
        None, help="Replay hours in a seeded random order (late-data test) instead of event time."
    ),
) -> None:
    """Replay silver hours into Redpanda (Avro, schema registry), in event-time order."""
    import random

    from keystone.rollup import stream as st

    if fresh:
        st.delete_topic(st.DEFAULT_BOOTSTRAP, topic)
    st.ensure_topic(st.DEFAULT_BOOTSTRAP, topic)
    files = st.files_in_event_order(root / "silver" / "gharchive")
    if shuffle_seed is not None:
        random.Random(shuffle_seed).shuffle(files)
        typer.echo("order: " + " ".join(p.stem.rsplit("-", 1)[1] for p in files))
    stats = st.produce(files, topic)
    rate = stats.records / stats.seconds if stats.seconds else 0.0
    _log_run(root, {"engine": "redpanda", "topic": topic, **vars(stats)})
    typer.echo(
        f"files={stats.files} records={stats.records} bytes={stats.bytes} "
        f"seconds={stats.seconds:.1f} records_per_s={rate:.0f}"
    )


@rollup_app.command("flink")
def rollup_flink(
    namespace: str = typer.Option(..., help="Iceberg namespace to write (one per run)."),
    topic: str = typer.Option("keystone.gharchive.events.v1", help="Kafka topic to read."),
    mode: str = typer.Option("stream", help="stream (live and replay) or batch (backfill)."),
    unbounded: bool = typer.Option(False, help="Stream mode: keep running for new hours."),
    root: Path = typer.Option(DATA_DIR, help="Data directory for the run log."),
) -> None:
    """`full` engine: Flink SQL job (Redpanda to Iceberg), then the fold of late rows."""
    from keystone.rollup import flink as fl

    if mode not in ("stream", "batch"):
        raise typer.BadParameter("mode is stream or batch")
    job = fl.submit(
        fl.render_job(namespace, topic, cast(fl.Mode, mode), bounded=not unbounded),
        f"{namespace}-{mode}",
    )
    fold = fl.submit(fl.render_fold(namespace), f"{namespace}-fold")
    _log_run(
        root,
        {
            "engine": "flink",
            "mode": mode,
            "namespace": namespace,
            "topic": topic,
            "job_seconds": job.seconds,
            "fold_seconds": fold.seconds,
        },
    )
    typer.echo(f"{mode} job {job.seconds:.0f}s, fold {fold.seconds:.0f}s -> ice.{namespace}")


@rollup_app.command("parity")
def rollup_parity(
    namespace: str = typer.Option(..., help="Iceberg namespace the Flink run wrote."),
    root: Path = typer.Option(DATA_DIR, help="Data directory holding rollup/rollup.duckdb."),
    json: bool = typer.Option(False, "--json", help="Print JSON."),
) -> None:
    """Compare DuckDB and Flink rollups row for row; exit 1 on any difference."""
    from keystone.rollup import compare

    results = compare.compare(root / "rollup" / "rollup.duckdb", namespace)
    late = compare.late_rows(namespace)
    out = [{**vars(r), "ok": r.ok, "late_rows": late[r.table]} for r in results]
    _log_run(root, {"engine": "parity", "namespace": namespace, "tables": out})
    if json:
        typer.echo(jsonlib.dumps(out, indent=2))
    else:
        for r in out:
            typer.echo(
                f"{r['table']}: {'ok' if r['ok'] else 'DIFF'} free={r['free_rows']} "
                f"full={r['full_rows']} only_free={r['only_free']} only_full={r['only_full']} "
                f"late_rows={r['late_rows']}"
            )
    if not all(r.ok for r in results):
        raise typer.Exit(1)
