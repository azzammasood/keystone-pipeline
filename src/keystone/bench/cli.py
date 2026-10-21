"""`keystone bench`: the scale ladder and the storage measurement (Phase 9)."""

import json
from dataclasses import asdict
from pathlib import Path

import typer

from keystone.bench import ladder, storage
from keystone.paths import DATA_DIR

app = typer.Typer(no_args_is_help=True, help="Scale ladder and storage measurements (Phase 9).")


def _mb(n: int | None) -> str:
    return "-" if n is None else f"{n / 1e6:,.1f}"


@app.command("run")
def run(
    scope: list[str] = typer.Option(
        ["hour=2026-10-19-12", "day=2026-10-19"],
        help="Rungs: hour=YYYY-MM-DD-H, day=YYYY-MM-DD or month=YYYY-MM (silver must exist).",
    ),
    engine: list[str] = typer.Option(list(ladder.ENGINES), help="duckdb, spark, flink."),
    root: Path = typer.Option(DATA_DIR, help="Data directory holding silver/."),
    keep_up: bool = typer.Option(False, help="Leave the Flink cluster running afterwards."),
    json_out: bool = typer.Option(False, "--json"),
) -> None:
    """Run the same rollups on every engine and scope; record rows, bytes, time, memory, $0."""
    bad = set(engine) - set(ladder.ENGINES)
    if bad:
        raise typer.BadParameter(f"unknown engine(s): {sorted(bad)}")
    engines = [e for e in ladder.ENGINES if e in engine]
    preps = [ladder.prepare(ladder.parse_scope(s), root / "silver") for s in scope]
    results = []
    try:
        for e in engines:  # engine-major: one Compose profile up at a time
            for p in preps:
                if not json_out:
                    typer.echo(f"{e} {p.scope}: {p.rows:,} rows in ...")
                results.append(ladder.run_rung(e, p))
    finally:
        if "flink" in engines and not keep_up:
            ladder.flink_stop()
    path = ladder.record(results)
    agree = ladder.parity(results)
    if json_out:
        typer.echo(json.dumps({"results": [asdict(r) for r in results], "parity": agree}))
        return
    typer.echo(f"{'engine':7} {'scope':20} {'rows in':>10} {'MB in':>7} {'wall s':>8} "
               f"{'engine s':>8} {'peak MB':>8} {'rows out':>10} {'MB out':>7}")  # fmt: skip
    for r in results:
        rows_out = sum(o.rows for o in r.outputs.values())
        bytes_out = sum(o.bytes for o in r.outputs.values())
        eng = "-" if r.engine_seconds is None else f"{r.engine_seconds:.1f}"
        typer.echo(
            f"{r.engine:7} {r.scope:20} {r.input_rows:>10,} {_mb(r.input_bytes):>7} "
            f"{r.wall_seconds:>8.1f} {eng:>8} {_mb(r.peak_memory_bytes):>8} "
            f"{rows_out:>10,} {_mb(bytes_out):>7}"
        )
    recorded = ladder.parity_recorded()
    typer.echo(f"parity within this run: {agree}; cost $0; appended to {path}")
    typer.echo(f"engines disagreeing with the others' latest runs, per scope: {recorded}")
    if not all(agree.values()) or any(recorded.values()):
        raise typer.Exit(1)


@app.command("storage")
def storage_cmd(
    root: Path = typer.Option(DATA_DIR, help="Data directory: silver/, manifest/, spike/."),
    extra_root: list[Path] = typer.Option(
        [], help="More data directories with sample days (silver/ and manifest/)."
    ),
    json_out: bool = typer.Option(False, "--json"),
) -> None:
    """Measure raw, silver and rollup bytes on the sample hours; project per year (estimates)."""
    report = storage.build_report(root, tuple(extra_root))
    path = storage.write_report(report, ladder.BENCH_DIR / "storage.json")
    if json_out:
        typer.echo(json.dumps(report, default=str))
        return
    typer.echo(storage.render_markdown(report))
    typer.echo(f"written to {path}")
