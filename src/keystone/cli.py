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
from keystone.lineage import datasets as lds
from keystone.machine import measure
from keystone.parsers import drift as driftmod
from keystone.parsers import osv as osvparse
from keystone.parsers.gharchive import parse_file
from keystone.parsers.parquet import GhArchiveSink, write_osv
from keystone.parsers.privacy import ActorHasher, SaltMissing, salt_from_env
from keystone.paths import DATA_DIR, QUALITY_DIR, QUARANTINE_DIR, REPO_ROOT, SILVER_DIR
from keystone.policy import load_policy
from keystone.sources import GhArchiveConfig, load_sources, osv
from keystone.sources import gharchive as gha
from keystone.telemetry import versions
from keystone.telemetry.runlog import Stage
from keystone.warehouse import coverage as whcoverage
from keystone.warehouse import dbt as whdbt
from keystone.warehouse import lake as whlake

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

    stage = Stage(
        DATA_DIR,
        "ingest",
        "gharchive-fetch",
        __version__,
        outputs=lds.silver_gharchive() if sink is not None else (),
        job="ingest.gharchive",
    )
    with stage:
        try:
            summary = asyncio.run(run())
        except DiskBudgetExceeded as exc:
            typer.echo(f"stopped: {exc}", err=True)
            raise typer.Exit(2) from exc
        stage.bytes_in = summary.downloaded_bytes
        stage.detail.update(counts=dict(summary.counts), requests=summary.requests)
        typer.echo(summary.line())
        if sink is not None:
            _count_parse(stage, sink)
            typer.echo(" ".join(f"parsed_{k}={v}" for k, v in sink.totals.items()))
        if summary.counts.get("error"):
            raise typer.Exit(1)


def _count_parse(stage: Stage, sink: GhArchiveSink) -> None:
    stage.records_in = sink.totals["events"] + sink.totals["quarantined"]
    stage.records_out = sink.totals["events"]
    stage.bytes_out = sink.totals["bytes_out"]
    stage.detail.update(files=sink.totals["files"], quarantined=sink.totals["quarantined"])
    stage.inputs = lds.hour_files(sink.stems)


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
    _parse_batch(DATA_DIR, sink, _kept_hours(start, end))


def _parse_batch(root: Path, sink: GhArchiveSink, files: list[tuple[Path, FileRecord]]) -> None:
    """Parse files with the sink as one measured stage; drift or errors fail it at the end."""
    stage = Stage(
        root,
        "parse",
        "keystone-parser",
        __version__,
        outputs=lds.silver_gharchive(),
        job="parse.gharchive",
    )
    with stage:
        failed = 0
        for path, rec in files:
            try:
                sink(path, rec)
            except driftmod.DriftError as exc:
                failed += 1
                typer.echo(f"drift: {exc}", err=True)
            except Exception as exc:  # one bad file (corrupt gzip, ...) must not stop the batch
                failed += 1
                typer.echo(f"error: {path.name}: {exc!r}"[:300], err=True)
        _count_parse(stage, sink)
        stage.bytes_in = sink.totals["bytes_in"]
        stage.detail["failed"] = failed
        totals = " ".join(f"parsed_{k}={v}" for k, v in sink.totals.items())
        typer.echo(f"{totals} failed={failed}")
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
    batch = []
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
        batch.append((path, rec))
    _parse_batch(root, sink, batch)


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


@parse_app.command("osv")
def parse_osv(
    ecosystem: list[str] | None = typer.Option(
        None, help="Ecosystem zip to parse (repeatable). Default: every kept zip."
    ),
    zip_file: Path | None = typer.Option(
        None, "--zip", help="A local all.zip outside the manifest (fixtures, CI)."
    ),
    root: Path = typer.Option(DATA_DIR, help="Data directory to write silver/ and quarantine/."),
) -> None:
    """Parse OSV ecosystem zips to Parquet (vulns, affected, repo_refs); bad records quarantined.
    With --zip, the file's own SHA-256 and modification time stand in for the manifest's facts."""
    jobs: list[tuple[str, Path, osvparse.ZipContext]] = []
    if zip_file is not None:
        name = zip_file.parent.name or "local"
        ctx = osvparse.ZipContext(
            source_file=f"osv/{name}/{zip_file.name}",
            source_sha256=gha.sha256_file(zip_file),
            fetched_at=datetime.fromtimestamp(zip_file.stat().st_mtime, UTC),
        )
        jobs.append((name, zip_file, ctx))
    else:
        cfg = load_sources().osv
        manifest = Manifest(MANIFEST_PATH)
        try:
            for eco in ecosystem or cfg.ecosystems:
                name = osv.safe_dir(eco)
                path = OSV_DIR / "zips" / name / "all.zip"
                rec = manifest.get(osv.zip_url(cfg, eco))
                if not path.exists() or rec is None or rec.sha256 is None:
                    continue
                if gha.sha256_file(path) != rec.sha256:
                    typer.echo(f"skip {eco}: local zip differs from the manifest", err=True)
                    continue
                source = f"osv/{name}/all.zip"
                jobs.append((name, path, osvparse.ZipContext(source, rec.sha256, rec.fetched_at)))
        finally:
            manifest.close()
    if not jobs:
        typer.echo("no kept, verified OSV zips", err=True)
        raise typer.Exit(1)
    stage = Stage(
        root,
        "parse-osv",
        "keystone-parser",
        __version__,
        inputs=[lds.osv_zip(name) for name, _, _ in jobs],
        outputs=lds.silver_osv(),
        job="parse.osv",
    )
    with stage:
        records = quarantined = size = 0
        for name, path, ctx in jobs:
            size += path.stat().st_size
            result = osvparse.parse_zip(path, ctx)
            write_osv(name, result, root / "silver" / "osv", root / "quarantine" / "osv")
            records += len(result.vulns)
            quarantined += len(result.quarantine)
            typer.echo(
                f"{name}: vulns={len(result.vulns)} affected={len(result.affected)} "
                f"repo_refs={len(result.repo_refs)} quarantined={len(result.quarantine)}"
            )
        stage.records_in, stage.records_out, stage.bytes_in = records + quarantined, records, size
        stage.detail["quarantined"] = quarantined


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
    pattern = Path(rel).relative_to("data")
    return (root / pattern).as_posix(), any(root.glob(pattern.as_posix()))


@quality_app.command("contracts")
def quality_contracts(
    root: Path = typer.Option(DATA_DIR, help="Data directory holding silver/ and quarantine/."),
    require_all: bool = typer.Option(False, help="Fail if any contract's table has no rows (CI)."),
    generated: bool = typer.Option(
        True, help="Also test the generated contracts in metadata/contracts/ (Phase 6)."
    ),
) -> None:
    """Test every ODCS contract with datacontract-cli; fail on any violation."""
    from keystone.paths import METADATA_DIR
    from keystone.quality.contracts import contract_files, load_contract, run_contract

    files = contract_files()
    if generated:  # metadata/contracts also holds copies of the hand-written ones
        hand = {p.name for p in files}
        files += [p for p in contract_files(METADATA_DIR / "contracts") if p.name not in hand]
    bad = passed = tested = 0
    with Stage(
        root, "quality-contracts", "datacontract-cli", versions.package("datacontract-cli")
    ) as st:
        for path in files:
            spec = load_contract(path)
            skip = [
                p["value"]
                for p in spec.get("customProperties", [])
                if p["property"] == "keystoneTest"
            ]
            if skip:
                typer.echo(f"{path.name}: {skip[0]}")
                continue
            server = spec["servers"][0]
            if server["type"] == "duckdb":
                db = root / Path(server["database"]).relative_to("data")
                target, present = {"database": db.as_posix()}, db.exists()
            else:
                rows, present = _glob(root, server["path"])
                target = {"rows_glob": rows}
            if not present:
                typer.echo(f"{path.name}: no data at {next(iter(target.values()))} (skipped)")
                bad += require_all
                continue
            result = run_contract(path, **target)
            tested += 1
            passed += sum(c.result == "passed" for c in result.checks)
            status = "ok" if result.ok else "FAILED"
            typer.echo(f"{path.name}: {status} ({len(result.checks)} checks)")
            for c in result.problems:
                bad += 1
                typer.echo(f"  {c.result}: {c.field or ''} {c.name} | {c.reason}"[:300])
        st.records_in, st.records_out = tested, passed
        st.detail.update(contracts=tested, checks_passed=passed, violations=bad)
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


@rollup_app.command("run")
def rollup_run(
    root: Path = typer.Option(DATA_DIR, help="Data directory holding silver/; state in rollup/."),
    json: bool = typer.Option(False, "--json", help="Print JSON."),
) -> None:
    """`free` engine: incremental DuckDB rollup of new, re-parsed or removed silver hours."""
    from dataclasses import asdict

    import duckdb

    from keystone.rollup.duck import DuckRollup

    stage = Stage(
        root,
        "rollup",
        "duckdb",
        duckdb.__version__,
        [lds.events_input()],
        lds.rollup_tables(),
        job="rollup.duckdb",
    )
    with stage:
        duck = DuckRollup(root / "rollup" / "rollup.duckdb", root / "silver" / "gharchive")
        try:
            stats = duck.run()
            counts = duck.counts()
        finally:
            duck.close()
        stage.records_out = sum(stats.rows_written.values())
        stage.detail.update(
            changed_files=stats.changed_files, removed_files=stats.removed_files, table_rows=counts
        )
    out = {"engine": "duckdb", **asdict(stats), "table_rows": counts}
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
    stage = Stage(
        root,
        "produce",
        "redpanda",
        versions.compose_tag("redpanda"),
        [lds.events_input()],
        [lds.topic(topic)],
        job="stream.produce",
    )
    with stage:
        stats = st.produce(files, topic)
        stage.records_in = stage.records_out = stats.records
        stage.bytes_out = stats.bytes
        stage.detail.update(files=stats.files, topic=topic)
    rate = stats.records / stats.seconds if stats.seconds else 0.0
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
    if unbounded:
        job = fl.submit(
            fl.render_job(namespace, topic, cast(fl.Mode, mode), bounded=False),
            f"{namespace}-{mode}",
        )
        typer.echo(f"live job submitted ({job.seconds:.0f}s); run `rollup fold` to merge late rows")
        return
    stage = Stage(
        root,
        f"rollup-flink-{mode}",
        "flink",
        versions.flink(),
        [lds.topic(topic)],
        lds.flink_outputs(namespace),
        job=f"rollup.flink.{mode}",
    )
    with stage:
        job = fl.submit(
            fl.render_job(namespace, topic, cast(fl.Mode, mode), bounded=True),
            f"{namespace}-{mode}",
        )
        read, written = versions.flink_last_job_records(f"keystone-rollup-{mode}-{namespace}")
        fold = fl.submit(fl.render_fold(namespace), f"{namespace}-fold")
        stage.records_in, stage.records_out = read, written
        stage.detail.update(
            namespace=namespace, topic=topic, job_seconds=job.seconds, fold_seconds=fold.seconds
        )
    typer.echo(f"{mode} job {job.seconds:.0f}s, fold {fold.seconds:.0f}s -> ice.{namespace}")


@rollup_app.command("fold")
def rollup_fold(namespace: str = typer.Option(..., help="Iceberg namespace to fold.")) -> None:
    """Merge `<rollup>_ontime` and `<rollup>_late` into `<rollup>` (batch; live jobs need it
    periodically, bounded runs do it themselves)."""
    from keystone.rollup import flink as fl

    fold = fl.submit(fl.render_fold(namespace), f"{namespace}-fold")
    typer.echo(f"fold {fold.seconds:.0f}s -> ice.{namespace}")


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


warehouse_app = typer.Typer(no_args_is_help=True, help="dbt warehouse and Iceberg (Phase 5).")
app.add_typer(warehouse_app, name="warehouse")


@warehouse_app.command("build")
def warehouse_build(
    root: Path = typer.Option(DATA_DIR, help="Data directory with silver/ and rollup/."),
    target: str = typer.Option("duckdb", help="dbt target (profiles.yml)."),
    select: str | None = typer.Option(None, help="dbt node selection."),
) -> None:
    """`dbt build` (seeds, models, tests) on the target; then the docs-coverage gate."""
    paths = whdbt.WarehousePaths(root)
    args = ["build", *(["--select", select] if select else [])]
    with Stage(root, "warehouse", "dbt", versions.package("dbt-core")) as stage:
        ok = whdbt.run_dbt(args, paths, target)
        results = whdbt.run_results(paths.target_dir)
        if ok:
            stage.records_out = whdbt.published_rows(paths)
        stage.detail.update(
            nodes=results.nodes,
            tests_passed=results.tests_passed,
            tests_failed=results.tests_failed,
            target=target,
        )
        if not ok:
            raise typer.Exit(1)
    # `docs generate` rewrites run_results.json; keep the build's (dbt-ol and the metrics read it).
    build_results = (paths.target_dir / "run_results.json").read_bytes()
    if not whdbt.run_dbt(["docs", "generate"], paths, target):
        raise typer.Exit(1)
    (paths.target_dir / "run_results.json").write_bytes(build_results)
    cov = whcoverage.measure(paths.target_dir)
    typer.echo(
        f"docs coverage: {cov.ratio:.1%} (models {cov.models_documented}/{cov.models}, "
        f"columns {cov.columns_documented}/{cov.columns}; gate {whcoverage.GATE:.0%})"
    )
    for name in cov.missing[:20]:
        typer.echo(f"undocumented: {name}", err=True)
    if not cov.ok:
        raise typer.Exit(1)


@warehouse_app.command("publish")
def warehouse_publish(
    root: Path = typer.Option(DATA_DIR, help="Data directory holding warehouse/."),
    prefix: str = typer.Option("", help="Namespace prefix (e.g. ci_) for test runs."),
) -> None:
    """Copy the dbt models tagged `publish` into Iceberg (namespace = prefix + schema)."""
    import duckdb

    paths = whdbt.WarehousePaths(root)
    from keystone.lineage.emit import LineageRun

    stage = Stage(root, "publish", "duckdb-iceberg", duckdb.__version__)
    with stage:
        done = whlake.publish(paths.warehouse_db, paths.target_dir, prefix)
        stage.records_out = sum(p.rows for p in done)
        for p in done:  # one lineage job per table, so no table seems to feed another
            cols = list(p.columns)
            LineageRun(
                f"warehouse.publish.{p.table.label}",
                [lds.warehouse_model(p.schema, p.table.name, cols)],
                [lds.published(p.schema, p.table.name, cols, p.table.namespace)],
                root,
                engine=("duckdb-iceberg", duckdb.__version__),
            ).finish(True)
    for p in done:
        typer.echo(f"published {p.table.label}: rows={p.rows}")


@warehouse_app.command("verify")
def warehouse_verify(
    table: list[str] = typer.Option([], help="namespace.table to read (repeatable)."),
    namespace: list[str] = typer.Option([], help="Read every table of a namespace (repeatable)."),
    engine: list[str] = typer.Option(
        ["duckdb", "spark", "flink"], help="Engines; the first is the reference."
    ),
    json_out: Path | None = typer.Option(None, "--json", help="Write the results as JSON."),
) -> None:
    """Read the same Iceberg tables from each engine and compare exact fingerprints."""
    unknown = [e for e in engine if e not in whlake.READERS]
    if unknown:
        typer.echo(f"unknown engines: {unknown}", err=True)
        raise typer.Exit(2)
    tables = [whlake.Table(*t.split(".", 1)) for t in table] + whlake.tables_in(namespace)
    if not tables:
        typer.echo("no tables to read", err=True)
        raise typer.Exit(2)
    reads = whlake.verify(tables, engine)
    for r in reads:
        typer.echo(f"{r.engine:7} {r.table:45} rows={r.rows} match={r.matches}")
    if json_out is not None:
        json_out.parent.mkdir(parents=True, exist_ok=True)
        json_out.write_text(jsonlib.dumps([r.__dict__ for r in reads], indent=1), "utf-8")
    if not all(r.matches for r in reads):
        raise typer.Exit(1)


lineage_app = typer.Typer(
    no_args_is_help=True, help="OpenLineage, lineage graph, DataHub (Phase 6)."
)
app.add_typer(lineage_app, name="lineage")


@lineage_app.command("dbt")
def lineage_dbt(
    root: Path = typer.Option(DATA_DIR, help="Data directory holding warehouse/."),
    target: str = typer.Option("duckdb", help="dbt target of the last build."),
) -> None:
    """OpenLineage events for the last dbt build (dbt-ol send-events), canonical names."""
    from keystone.lineage import dbt as ldbt

    with Stage(
        root, "lineage-dbt", "openlineage-dbt", versions.package("openlineage-python")
    ) as st:
        events = ldbt.send_events(root, target)
        st.records_out = len(events)
        with_cll = sum(
            1
            for e in events
            for d in e.get("outputs", [])
            if "columnLineage" in d.get("facets", {})
        )
        st.detail["outputs_with_column_lineage"] = with_cll
    typer.echo(f"dbt-ol events={len(events)} outputs_with_column_lineage={with_cll}")


metadata_app = typer.Typer(no_args_is_help=True, help="Static metadata export (Phase 6).")
app.add_typer(metadata_app, name="metadata")


@metadata_app.command("contracts")
def metadata_contracts(
    root: Path = typer.Option(DATA_DIR, help="Data directory with rollup/ and warehouse/."),
    check: bool = typer.Option(False, help="Fail if metadata/contracts differs (CI)."),
) -> None:
    """Generate the ODCS contracts of OSV silver, rollups and dbt nodes (metadata/contracts)."""
    from keystone.export.contracts import generate
    from keystone.export.meta import Metadata
    from keystone.paths import METADATA_DIR

    out = METADATA_DIR / "contracts"
    generated = generate(Metadata(), root)
    if check:
        stale = [
            n
            for n, text in generated.items()
            if not (out / n).exists() or (out / n).read_text("utf-8") != text
        ]
        for n in stale:
            typer.echo(f"contract out of date: metadata/contracts/{n}", err=True)
        typer.echo(f"contracts={len(generated)} out_of_date={len(stale)}")
        if stale:
            raise typer.Exit(1)
        return
    out.mkdir(parents=True, exist_ok=True)
    for n, text in generated.items():
        (out / n).write_text(text, "utf-8", newline="\n")
    typer.echo(f"wrote {len(generated)} contracts to {out}")


@metadata_app.command("export")
def metadata_export(
    root: Path = typer.Option(DATA_DIR, help="Data directory (events, warehouse, rollup)."),
    out: Path = typer.Option(None, help="Output directory (default: metadata/)."),
    require_path: bool = typer.Option(
        False, help="Fail unless lineage runs from an hourly file to a published field (gate)."
    ),
) -> None:
    """Write catalog.json, glossary.json, lineage.json and contracts/; gate completeness."""
    from keystone.export.catalog import write_export
    from keystone.export.contracts import generate
    from keystone.export.meta import Metadata
    from keystone.paths import METADATA_DIR

    target = out or METADATA_DIR
    with Stage(root, "metadata-export", "keystone", __version__) as st:
        summary = write_export(root, target, generate(Metadata(), root))
        comp, lin, gate = summary["completeness"], summary["lineage"], summary["gate_path"]
        st.records_out = lin["nodes"]
        st.detail.update(
            nodes=lin["nodes"],
            edges=lin["edges"],
            field_edges=lin["field_edges"],
            documented_pct=round(comp.documented_pct * 100, 2),
        )
        typer.echo(
            f"completeness: models {comp.models_documented}/{comp.models}, columns "
            f"{comp.columns_documented}/{comp.columns} ({comp.documented_pct:.1%}), datasets "
            f"complete {comp.datasets_complete}/{comp.datasets}, owners missing "
            f"{comp.owners_missing}"
        )
        typer.echo(
            f"lineage: nodes={lin['nodes']} edges={lin['edges']} field_edges={lin['field_edges']}"
        )
        typer.echo("path: " + " -> ".join(gate["tables"]) if gate["tables"] else "path: none")
        if gate["field"]:
            typer.echo("field: " + " <- ".join(gate["field"]))
        if not comp.ok or (require_path and not (gate["tables"] and gate["field"])):
            raise typer.Exit(1)


metrics_app = typer.Typer(no_args_is_help=True, help="pipeline_metrics.json and Grafana (Phase 6).")
app.add_typer(metrics_app, name="metrics")


@metrics_app.command("emit")
def metrics_emit(
    root: Path = typer.Option(DATA_DIR, help="Data directory (telemetry/, silver/, rollup/)."),
    metadata_dir: Path = typer.Option(
        None, help="Metadata export to summarise (default metadata/)."
    ),
    out: Path = typer.Option(
        None, help="Output file (default <root>/metrics/pipeline_metrics.json)."
    ),
) -> None:
    """Build pipeline_metrics.json from measured runs, validate it, export its gauges."""
    from keystone.paths import METADATA_DIR
    from keystone.telemetry import metrics as tm

    doc = tm.build(root, metadata_dir or METADATA_DIR)
    tm.validate(doc)
    target = out or root / "metrics" / "pipeline_metrics.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(jsonlib.dumps(doc, indent=1) + "\n", "utf-8")
    tm.export_gauges(doc)
    stages = len(doc["last_run"]["stages"])
    typer.echo(
        f"valid pipeline_metrics.json: profile={doc['profile']} engines={len(doc['engines'])} "
        f"last_run_stages={stages} events_processed={doc['totals']['events_processed']} -> {target}"
    )


@metrics_app.command("check-grafana")
def metrics_check_grafana(
    root: Path = typer.Option(DATA_DIR, help="Data directory holding metrics/."),
    file: Path = typer.Option(None, help="pipeline_metrics.json (default <root>/metrics/...)."),
    url: str = typer.Option("http://127.0.0.1:3000", help="Grafana base URL."),
) -> None:
    """Compare every Grafana panel value with pipeline_metrics.json; exit 1 on a mismatch."""
    from keystone.telemetry import grafana as gf
    from keystone.telemetry import metrics as tm

    doc = jsonlib.loads((file or root / "metrics" / "pipeline_metrics.json").read_text("utf-8"))
    tm.validate(doc)
    found = gf.versions(url)
    with Stage(root, "grafana-check", "grafana", found.get("grafana", "unknown")) as st:
        rows = gf.compare(doc, url)
        bad = [r for r in rows if not r.ok]
        for r in rows:
            mark = "ok  " if r.ok else "DIFF"
            typer.echo(
                f"{mark} {r.panel:32} {r.key:28} json={r.json_value} grafana={r.grafana_value}"
            )
        st.records_in, st.records_out = len(rows), len(rows) - len(bad)
        st.detail.update(services=found, mismatches=len(bad))
        typer.echo(f"compared={len(rows)} mismatches={len(bad)}")
        if bad or not rows:
            raise typer.Exit(1)


@lineage_app.command("push")
def lineage_push(
    root: Path = typer.Option(DATA_DIR, help="Data directory holding lineage/events."),
    url: str = typer.Option("http://127.0.0.1:5000", help="Marquez base URL."),
) -> None:
    """Send the whole event log to Marquez (it de-duplicates by run id and event time)."""
    import httpx

    from keystone.lineage.graph import read_events

    events = list(read_events(root))
    with Stage(root, "lineage-push", "marquez", versions.compose_tag("marquez")) as st:
        sent = 0
        with httpx.Client(base_url=url, timeout=30) as client:
            for ev in events:
                client.post("/api/v1/lineage", json=ev).raise_for_status()
                sent += 1
        st.records_in, st.records_out = len(events), sent
    typer.echo(f"sent {sent} events to {url}")


@lineage_app.command("collect")
def lineage_collect(
    root: Path = typer.Option(DATA_DIR, help="Data directory holding lineage/events."),
    incoming: Path = typer.Option(
        DATA_DIR / "lineage" / "incoming", help="Where Spark and Flink write their events."
    ),
) -> None:
    """Canonicalise the events Spark and Flink wrote (file transport) into the event log."""
    from keystone.lineage.dbt import canonicalise
    from keystone.lineage.emit import write_events

    total = 0
    for path in sorted(incoming.glob("*.jsonl")):
        done = path.with_name(f"{path.stem}.{datetime.now(UTC):%Y%m%dT%H%M%S}.collected")
        path.rename(done)  # the writer starts a new file on its next event
        events = [
            canonicalise(jsonlib.loads(line))
            for line in done.read_text("utf-8").splitlines()
            if line.strip()
        ]
        write_events(root, events)
        total += len(events)
        typer.echo(f"{path.name}: {len(events)} events")
    typer.echo(f"collected {total} events")
