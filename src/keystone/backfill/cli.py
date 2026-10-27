"""`keystone backfill`: plan, run and total the history backfill (Phase 9, ADR-050)."""

import json
import os
from collections import defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import typer

from keystone.backfill import plan as planmod
from keystone.paths import DATA_DIR

app = typer.Typer(no_args_is_help=True, help="History backfill on GitHub Actions (Phase 9).")


def _today() -> date:
    return datetime.now(UTC).date()


@app.command("plan")
def plan(
    published: Path | None = typer.Option(None, help="File of published asset names, one a line."),
    window: list[str] = typer.Option([], help="Only these windows (default: all)."),
    chunk_days: int = typer.Option(0, help="Split months into parts of at most N days (0: no)."),
    max_chunks: int = typer.Option(0, help="Plan at most N pending chunks (0: all)."),
    redo_incomplete: bool = typer.Option(False, help="Retry chunks published as incomplete."),
    end: str | None = typer.Option(None, help="Exclusive end of the live window (default: today)."),
    json_out: bool = typer.Option(False, "--json", help="Print a GitHub matrix as JSON."),
) -> None:
    """Pending chunks of the incident and live windows (`config/incidents.yml`)."""
    wins = planmod.windows(date.fromisoformat(end) if end else _today())
    if window:
        unknown = set(window) - set(wins)
        if unknown:
            raise typer.BadParameter(f"unknown window(s): {sorted(unknown)}")
        wins = {k: v for k, v in wins.items() if k in window}
    allc = planmod.chunks(wins, chunk_days)
    names = set(published.read_text(encoding="utf-8").split()) if published else set()
    todo = planmod.pending(allc, names, redo_incomplete=redo_incomplete)
    if max_chunks:
        todo = todo[:max_chunks]
    if json_out:
        typer.echo(json.dumps({"include": [c.matrix() for c in todo]}))
        return
    for name, (a, b) in wins.items():
        typer.echo(f"window {name}: {a} .. {b} ({(b - a).days} days)")
    typer.echo(f"chunks {len(allc)}, pending {len(todo)}")
    for c in todo:
        typer.echo(f"  {c.tag} {c.name} ({(c.end - c.start).days} days)")


@app.command("chunk")
def chunk(
    window: str = typer.Option(..., help="Window id (release data-<window>)."),
    start: str = typer.Option(..., help="First day, UTC."),
    end: str = typer.Option(..., help="Exclusive end day, UTC."),
    out: Path = typer.Option(DATA_DIR / "backfill" / "out", help="Directory for the assets."),
    work: Path = typer.Option(DATA_DIR / "backfill" / "work", help="Scratch (deleted first)."),
    workers: int = typer.Option(0, help="Parse processes (0: CPU count)."),
    overlap_hours: int = typer.Option(2, help="Hours read past the end for late events."),
) -> None:
    """Fetch, parse and roll up one chunk; write its two Parquet files and its JSON record."""
    from keystone.backfill.chunk import run_chunk

    a, b = date.fromisoformat(start), date.fromisoformat(end)
    if b <= a:
        raise typer.BadParameter("--end must be after --start")
    record = run_chunk(
        window, a, b, work, out,
        workers=workers or os.cpu_count() or 1, overlap_hours=overlap_hours,
    )  # fmt: skip
    suffix = "" if record["complete"] else ".incomplete"
    dest = out / f"chunk-{record['chunk']}{suffix}.json"
    dest.write_text(json.dumps(record, indent=1), encoding="utf-8")
    ev, s = record["events"], record["seconds"]
    typer.echo(
        f"chunk {record['chunk']} complete={record['complete']} "
        f"hours={record['hours']['fetched']} status={record['hours']['status']} "
        f"parse_failed={len(record['hours']['parse_failed'])} "
        f"downloaded_bytes={record['download']['bytes']} events={record['parse']['events']} "
        f"in_chunk={ev['in_chunk']} lost={ev['before_start_lost'] + ev['after_end_lost']} "
        f"rows={record['rows']} bytes={record['bytes']} seconds={s} "
        f"peak_rss_mb={record['peak_rss_mb']}"
    )
    typer.echo(f"wrote {dest.name}")


_SUMS = ("in_chunk", "before_start_lost", "after_end_lost")


def totals_of(records: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = defaultdict(lambda: defaultdict(int))
    for r in records:
        for key in (r["window"], "all"):
            t = out[key]
            t["chunks"] += 1
            t["complete_chunks"] += int(r["complete"])
            t["days"] += (
                date.fromisoformat(r["end"][:10]) - date.fromisoformat(r["start"][:10])
            ).days
            t["hours_fetched"] += r["hours"]["fetched"]
            t["hours_missing"] += len(r["hours"]["missing"])
            t["hours_failed"] += len(r["hours"]["parse_failed"]) + len(r["hours"]["fetch_errors"])
            t["downloaded_bytes"] += r["download"]["bytes"]
            t["events_parsed"] += r["parse"]["events"]
            t["quarantined"] += r["parse"]["quarantined"]
            for k in _SUMS:
                t[f"events_{k}"] += r["events"][k]
            for k, v in r["rows"].items():
                t[f"rows_{k}"] += v
            for k, v in r["bytes"].items():
                t[f"bytes_{k}"] += v
            t["job_seconds"] += r["seconds"]["total"]
            first, last = t.get("first"), t.get("last")
            t["first"] = min(first, r["start"]) if first else r["start"]
            t["last"] = max(last, r["end"]) if last else r["end"]
    return {k: dict(v) for k, v in out.items()}


def record_files(records: Path) -> list[Path]:
    """Chunk records to count: a chunk redone after an incomplete run has both records, and
    only the complete one counts."""
    files = sorted(records.glob("chunk-*.json"))
    done = {p.name for p in files if not p.name.endswith(".incomplete.json")}
    return [
        p
        for p in files
        if not p.name.endswith(".incomplete.json")
        or p.name.replace(".incomplete.json", ".json") not in done
    ]


@app.command("totals")
def totals(
    records: Path = typer.Option(DATA_DIR / "backfill" / "records", help="Dir of chunk-*.json."),
    json_out: bool = typer.Option(False, "--json"),
) -> None:
    """Processed totals per window from the published chunk records (exact counts)."""
    recs = [json.loads(p.read_text(encoding="utf-8")) for p in record_files(records)]
    if not recs:
        typer.echo(f"no chunk records in {records}", err=True)
        raise typer.Exit(1)
    t = totals_of(recs)
    if json_out:
        typer.echo(json.dumps(t, indent=1))
        return
    for k, v in t.items():
        typer.echo(f"{k}: " + " ".join(f"{a}={b}" for a, b in v.items()))


@app.command("drift")
def drift(
    records: Path = typer.Option(
        DATA_DIR / "backfill" / "records", help="Dir (searched recursively) of chunk-*.json."
    ),
    learn: bool = typer.Option(False, help="Write learnable findings into the drift registry."),
) -> None:
    """Summed `hours.drift` of the chunk records per era (ADR-051, ADR-069); --learn adds the
    reviewed field shapes to contracts/drift/gharchive.yml."""
    from keystone.backfill import drift as bd
    from keystone.parsers.drift import load_registry, save_registry

    rev = bd.review(bd.load_records(records))
    for (era, key), hours in sorted(rev.findings.items()):
        typer.echo(f"{era} hours={hours} chunks={rev.chunks[(era, key)]} {key}")
    if rev.ambiguous:
        typer.echo(f"not learned (chunk spans an era break): {', '.join(rev.ambiguous)}")
    if learn:
        reg = load_registry()
        added = bd.learn(rev, reg)
        save_registry(reg)
        typer.echo(f"learned {len(added)} entries into the drift registry")
