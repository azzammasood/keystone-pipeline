"""`keystone score ...`: live score, explanations and point-in-time backtests (Phase 8, ADR-044).

Kept out of cli.py (one `add_typer` line there).
"""

import json as jsonlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import typer

from keystone.paths import DATA_DIR, DOCS_DIR
from keystone.score import backtest as bt
from keystone.score import model, signals

score_app = typer.Typer(
    no_args_is_help=True, help="Rule-based score, explanations, backtests (Phase 8)."
)

ROLLUP = DATA_DIR / "rollup" / "rollup.duckdb"
WAREHOUSE = DATA_DIR / "warehouse" / "keystone.duckdb"
OUT = DATA_DIR / "score"


def _cut(month: str | None) -> datetime:
    if month is None:
        return signals.month_start(datetime.now(UTC))
    y, m = month.split("-")
    return datetime(int(y), int(m), 1, tzinfo=UTC)


def live_scores(inp: signals.Inputs, cut: datetime) -> list[dict[str, Any]]:
    """Scores at `cut` for every repository a package maps to (status accepted or auto_exact)."""
    if not inp.vuln_data:
        raise typer.BadParameter("the live score needs the warehouse (mapped repositories)")
    w = signals.Window(cut)
    ids = [int(r[0]) for r in inp.con.execute("SELECT DISTINCT repo_id FROM mapped").fetchall()]
    sig = signals.compute(inp, w, ids)
    crit = signals.criticality_inputs(inp, ids)
    out = []
    for rid in sorted(sig):
        c = crit.get(rid, {})
        sc = model.score(
            rid, cut.isoformat(), sig[rid], model.criticality(c.get("dependent_repos"))
        )
        d = sc.as_dict()
        d["criticality_inputs"] = c
        out.append(d)
    return out


@score_app.command("live")
def score_live(
    month: str = typer.Option(None, help="Cut month YYYY-MM (default: the current UTC month)."),
    rollup: Path = typer.Option(ROLLUP, help="Rollup DuckDB file or directory of Parquet."),
    warehouse: Path = typer.Option(WAREHOUSE, help="Warehouse DuckDB file."),
    out: Path = typer.Option(OUT / "live.json", help="Output JSON."),
    allow_partial: bool = typer.Option(
        False, help="Score even when the rollup misses hours of the window (testing only)."
    ),
    extra_rollup: Path | None = typer.Option(
        None, help="A later rollup DuckDB file: its days after the base's last day are added."
    ),
) -> None:
    """Score every mapped repository on the 12 months before the cut, with explanations.
    Without --allow-partial, a window the rollup does not cover is reported, not scored."""
    cut = _cut(month)
    extra = extra_rollup if isinstance(extra_rollup, Path) else None  # direct calls
    inp = signals.open_inputs(rollup, warehouse, extra)
    try:
        cov = signals.hour_coverage(inp, signals.Window(cut))
        partial = cov < bt.MIN_HOUR_COVERAGE
        rows = [] if partial and not allow_partial else live_scores(inp, cut)
    finally:
        inp.close()
    scored = [r for r in rows if r["fragility"] is not None]
    res = {
        "score_version": model.SCORE_VERSION,
        "cut": cut.isoformat(),
        "hour_coverage": cov,
        "repos": len(rows),
        "scored": len(scored),
        "note": "signals only; not a prediction"
        if not partial
        else f"partial data: the rollup holds {cov:.1%} of the window's hours"
        + ("" if allow_partial else "; not scored"),
        "scores": rows,
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(jsonlib.dumps(res, indent=2, default=str) + "\n", encoding="utf-8")
    typer.echo(
        f"{model.SCORE_VERSION} cut {cut:%Y-%m-%d}: {len(scored)} of {len(rows)} repositories "
        f"scored; window hours covered {cov:.1%}; {res['note']} -> {out}"
    )


@score_app.command("explain")
def score_explain(
    repo_id: int = typer.Argument(..., help="Repository id."),
    path: Path = typer.Option(OUT / "live.json", help="Live score JSON."),
) -> None:
    """Print one repository's score and the component behind every point of it."""
    res = jsonlib.loads(path.read_text(encoding="utf-8"))
    row = next((r for r in res["scores"] if r["repo_id"] == repo_id), None)
    if row is None:
        raise typer.BadParameter(f"repository {repo_id} is not in {path}")
    typer.echo(
        f"repo {repo_id} {row['score_version']} cut {row['cut'][:10]}: fragility "
        f"{row['fragility']}, criticality {row['criticality']}, risk {row['risk']} "
        f"(coverage {row['coverage']:.0%}; {row['note']})"
    )
    for c in row["components"]:
        sub = "n/a" if c["sub"] is None else f"{c['sub']:.2f}"
        typer.echo(f"  {c['name']:<18} w={c['weight']:.2f} sub={sub}  {c['reason']}")


@score_app.command("backtest")
def score_backtest(
    incident: list[str] = typer.Option(None, help="Incident id(s); default all."),
    months_before: int = typer.Option(
        0, help="Monthly trajectory points before the incident cut (each needs its own window)."
    ),
    rollup: Path = typer.Option(ROLLUP, help="Rollup DuckDB file or directory of Parquet."),
    warehouse: Path = typer.Option(WAREHOUSE, help="Warehouse DuckDB file (names, OSV)."),
    out: Path = typer.Option(OUT / "backtest.json", help="Output JSON."),
    report: Path = typer.Option(DOCS_DIR / "backtest.md", help="Markdown report."),
) -> None:
    """Point-in-time backtest of every incident in config/incidents.yml; misses are reported."""
    incs = bt.load_incidents()
    if incident:
        incs = [i for i in incs if i.id in incident]
    inp = signals.open_inputs(rollup, warehouse)
    try:
        res = bt.run(inp, incs, months_before)
    finally:
        inp.close()
    bt.write(res, out, None if incident else report)  # a subset never overwrites it
    for i in res["incidents"]:
        typer.echo(f"{i['id']:<14} cut {i['cut'][:10]}  {i['outcome']}")
    leaks = [
        p for i in res["incidents"] for p in i["points"] if p["leakage_check"] not in (None, "pass")
    ]
    if leaks:
        typer.echo(f"leakage check failed at {len(leaks)} point(s)", err=True)
        raise typer.Exit(1)
