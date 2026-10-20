"""Keystone CLI. Orchestrators (GitHub Actions, Airflow) only call these commands."""

import asyncio
import json as jsonlib
from datetime import UTC, datetime, timedelta

import typer

from keystone import __version__
from keystone.fetch.client import DiskBudgetExceeded, DiskGuard, PoliteClient
from keystone.fetch.manifest import Manifest
from keystone.machine import measure
from keystone.paths import DATA_DIR, REPO_ROOT
from keystone.policy import load_policy
from keystone.sources import gharchive as gha
from keystone.sources import load_sources, osv

app = typer.Typer(no_args_is_help=True, add_completion=False)
ingest_app = typer.Typer(no_args_is_help=True, help="Polite, resumable ingestion (Phase 2).")
app.add_typer(ingest_app, name="ingest")

MANIFEST_PATH = DATA_DIR / "manifest" / "manifest.duckdb"
GHARCHIVE_DIR = DATA_DIR / "raw" / "gharchive"
OSV_DIR = DATA_DIR / "raw" / "osv"


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
) -> None:
    """Check GH Archive hours: conditional GET, MD5 check, manifest update, stream and delete."""
    begin, finish = _range(start, end)
    cfg = load_sources().gharchive
    disk = _disk_guard(max_gb)

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
                )
        finally:
            manifest.close()

    try:
        summary = asyncio.run(run())
    except DiskBudgetExceeded as exc:
        typer.echo(f"stopped: {exc}", err=True)
        raise typer.Exit(2) from exc
    typer.echo(summary.line())
    if summary.counts.get("error"):
        raise typer.Exit(1)


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
