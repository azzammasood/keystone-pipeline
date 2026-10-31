"""Site exports (Phase 10, ADR-060): the radar list, project pages, backtests and benchmarks,
each one JSON document in the versioned envelope (ADR-043) with its rows checked against
`contracts/<dataset>_row.schema.json`, plus pipeline_metrics.json, the metadata export and an
index with checksums.

Inputs are files the pipeline already wrote: `keystone score live|backtest` output, the
warehouse (names and mapped packages, optional), `docs/benchmarks.md` (the measured ladder) and
`keystone metrics emit` output. Nothing is computed here that a measured file does not hold.
Component `raw` values stay internal: pages carry each signal's value and its reason.
"""

import hashlib
import json
import re
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import jsonschema

from keystone.export import envelope
from keystone.paths import CONTRACTS_DIR, DATA_DIR, DOCS_DIR, METADATA_DIR
from keystone.telemetry import metrics as tm

SCORE_DIR = DATA_DIR / "score"
WAREHOUSE = DATA_DIR / "warehouse" / "keystone.duckdb"
BENCHMARKS_MD = DOCS_DIR / "benchmarks.md"
METRICS = DATA_DIR / "metrics" / "pipeline_metrics.json"
OUT = DATA_DIR / "publish" / "site"

SCORE_SOURCES = [
    {"name": "GH Archive", "url": "https://www.gharchive.org/"},
    {"name": "OSV", "url": "https://osv.dev/", "terms": "CC-BY 4.0 (osv.dev data license)"},
    {"name": "ecosyste.ms", "url": "https://ecosyste.ms/", "terms": "CC-BY-SA 4.0"},
]
BENCH_SOURCES = [
    {
        "name": "Keystone benchmarks",
        "url": "https://github.com/azzammasood/keystone-pipeline/blob/main/docs/benchmarks.md",
    }
]
SIGNAL_KEYS = ("name", "available", "sub", "weight", "reason")
VERSION = "v1"
RADAR_VERSION = "v2"  # v2: scored, unscored_reason, rank (ADR-084)


@dataclass(frozen=True)
class Repo:
    name: str | None
    packages: list[str]


def _signals(components: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{k: c[k] for k in SIGNAL_KEYS} for c in components]


def top_signals(components: list[dict[str, Any]], n: int = 3) -> list[dict[str, Any]]:
    """The n available signals that add most to fragility (weight x value)."""
    avail = [c for c in components if c["available"] and c["sub"] is not None]
    avail.sort(key=lambda c: (-c["weight"] * c["sub"], c["name"]))
    return _signals(avail[:n])


def repo_facts(warehouse: Path, ids: list[int]) -> dict[int, Repo]:
    """Name and mapped packages per repository; empty facts without a warehouse.

    The name is the latest one GH Archive saw (`dim_repo`). A repository the warehouse's GH
    Archive window has not seen takes the name its accepted package mapping resolved for that
    id (ecosyste.ms current name, lower case; ADR-083), only when that name is unique. Else
    the name stays null: never guessed."""
    if not ids or not warehouse.exists():
        return {i: Repo(None, []) for i in ids}
    con = duckdb.connect(str(warehouse), read_only=True)
    try:
        names = dict(
            con.execute(
                "SELECT repo_id, repo_name FROM marts.dim_repo WHERE list_contains(?, repo_id)",
                [ids],
            ).fetchall()
        )
        for rid, name in con.execute(
            "SELECT repo_id, MIN(repo_full_name) FROM marts.map_package_repo "
            "WHERE status IN ('accepted', 'auto_exact') AND repo_host = 'github.com' "
            "AND repo_full_name IS NOT NULL AND list_contains(?, repo_id) "
            "GROUP BY repo_id HAVING COUNT(DISTINCT repo_full_name) = 1",
            [ids],
        ).fetchall():
            if names.get(rid) is None:
                names[rid] = name
        pkgs: dict[int, list[str]] = {}
        for rid, key in con.execute(
            "SELECT repo_id, package_key FROM marts.map_package_repo "
            "WHERE status IN ('accepted', 'auto_exact') AND list_contains(?, repo_id) "
            "ORDER BY package_key",
            [ids],
        ).fetchall():
            pkgs.setdefault(int(rid), []).append(str(key))
    finally:
        con.close()
    return {i: Repo(names.get(i), pkgs.get(i, [])) for i in ids}


def _score_row(s: dict[str, Any], repo: Repo) -> dict[str, Any]:
    return {
        "repo_id": int(s["repo_id"]),
        "repo_name": repo.name,
        "packages": repo.packages,
        "score_version": s["score_version"],
        "cut": s["cut"],
        "fragility": s["fragility"],
        "criticality": s["criticality"],
        "risk": s["risk"],
        "coverage": s["coverage"],
        "note": s["note"],
    }


def _rank(row: dict[str, Any]) -> tuple[float, float, int]:
    risk, frag = row["risk"], row["fragility"]
    return (
        -(risk if risk is not None else -1.0),
        -(frag if frag is not None else -1.0),
        row["repo_id"],
    )


def _status(note: str, scored: int, repos: int, mapped: int | None) -> tuple[str, str]:
    if note.startswith("partial data"):
        return ("partial data", note)
    of = f" of {mapped} mapped" if mapped is not None else ""
    return (
        "ok",
        f"{scored} of {repos} repositories scored ({repos}{of} had activity in the window); {note}",
    )


def radar_and_pages(
    live: dict[str, Any], repos: dict[int, Repo]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    radar, pages = [], []
    for s in live["scores"]:
        base = _score_row(s, repos.get(int(s["repo_id"]), Repo(None, [])))
        radar.append({**base, "top_signals": top_signals(s["components"])})
        pages.append(
            {
                **base,
                "components": _signals(s["components"]),
                "criticality_inputs": dict(s.get("criticality_inputs") or {}),
            }
        )
    radar.sort(key=_rank)
    # Scored rows (a fragility) are ranked 1..N in that order; the rest carry the reason
    # instead (radar v2, ADR-084), so "N scored of M" reads straight from the rows.
    n = 0
    for row in radar:
        row["scored"] = row["fragility"] is not None
        row["unscored_reason"] = None if row["scored"] else row["note"]
        n += int(row["scored"])
        row["rank"] = n if row["scored"] else None
    pages.sort(key=lambda r: r["repo_id"])
    return radar, pages


POINT_KEYS = ("cut", "hour_coverage", "status", "reference_size", "leakage_check")
REPO_KEYS = ("name", "repo_id", "status", "fragility", "percentile", "flagged")


def _point(p: dict[str, Any]) -> dict[str, Any]:
    """An allow-list projection: each scored repository keeps its signals (no `raw`)."""
    repos = []
    for r in p.get("repos") or []:
        row = {k: r.get(k) for k in REPO_KEYS if k in r}
        score = r.get("score")
        if score:
            row["signals"] = _signals(score["components"])
        repos.append(row)
    return {**{k: p.get(k) for k in POINT_KEYS}, "repos": repos}


def _sensitivity(s: dict[str, Any] | None) -> dict[str, Any] | None:
    if not s:
        return None
    keys = ("scored_variants", "percentile_min", "percentile_max", "flagged_variants")
    return {
        "variants": s["variants"],
        "per_repo": {
            str(rid): {k: v.get(k) for k in keys} for rid, v in s.get("per_repo", {}).items()
        },
    }


def backtest_rows(bt: dict[str, Any]) -> list[dict[str, Any]]:
    keys = ("id", "month", "cut", "summary", "sources", "outcome")
    return [
        {
            **{k: i.get(k) for k in keys},
            "points": [_point(p) for p in i["points"]],
            "sensitivity": _sensitivity(i.get("sensitivity")),
        }
        for i in bt["incidents"]
    ]


_NUM = re.compile(r"-?[0-9][0-9,]*(?:\.[0-9]+)?")


def _nums(cell: str) -> list[float]:
    return [float(m.replace(",", "")) for m in _NUM.findall(cell)]


def _num(cell: str, line: str) -> float:
    found = _nums(cell)
    if not found:
        raise ValueError(f"benchmarks.md: no number in {cell!r} on line: {line}")
    return found[0]


def benchmark_rows(md: str) -> list[dict[str, Any]]:
    """Rows of the scale-ladder table (section 1) of docs/benchmarks.md. Engine cells with `-`
    were not measured and stay null; a blank rung cell continues the rung above."""
    rows: list[dict[str, Any]] = []
    in_table = False
    cur: dict[str, Any] = {}
    for line in md.splitlines():
        if line.startswith("| rung |"):
            in_table = True
            continue
        if not in_table:
            continue
        if not line.startswith("|"):
            if rows:
                break
            continue
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) != 9 or set(cells[0]) <= {"-", ":"} and cells[0]:
            continue
        if cells[0]:
            cur = {
                "rung": cells[0],
                "input_rows": int(_num(cells[1], line)),
                "input_mb": _num(cells[2], line),
            }
        engine_s = _nums(cells[5])
        rows.append(
            {
                **cur,
                "engine": cells[3],
                "wall_s": _nums(cells[4]),
                "engine_s": engine_s or None,
                "peak_mb": _nums(cells[6]),
                "rows_out": int(_num(cells[7], line)),
                "mb_out": _num(cells[8], line),
            }
        )
    return rows


def _write(path: Path, doc: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes((json.dumps(doc, indent=2, default=str) + "\n").encode())


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build(
    out: Path = OUT,
    *,
    root: Path = DATA_DIR,
    score_dir: Path = SCORE_DIR,
    warehouse: Path = WAREHOUSE,
    benchmarks_md: Path = BENCHMARKS_MD,
    metrics: Path = METRICS,
    metadata_dir: Path = METADATA_DIR,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Write every site export under `out` and return the index rows. Raises when the run log
    has no profile decision (envelope.NoProfileRecorded) or any document fails its schema."""
    now = now or datetime.now(UTC)
    produced = envelope.recorded_profile(root)
    if out.exists():  # a fresh directory: nothing stale is indexed or uploaded again
        shutil.rmtree(out)
    live = json.loads((score_dir / "live.json").read_text("utf-8"))
    bt = json.loads((score_dir / "backtest.json").read_text("utf-8"))
    ids = sorted({int(s["repo_id"]) for s in live["scores"]})
    radar, pages = radar_and_pages(live, repo_facts(warehouse, ids))
    cut = datetime.fromisoformat(live["cut"])
    start = cut.replace(year=cut.year - 1)  # cuts are month starts: never 29 February
    status = _status(live["note"], int(live["scored"]), int(live["repos"]), live.get("mapped"))
    common: dict[str, Any] = {"produced": produced, "now": now}
    docs = {
        "radar.json": envelope.wrap(
            "radar",
            RADAR_VERSION,
            radar,
            sources=SCORE_SOURCES,
            window_start=start,
            window_end=cut,
            status=status,
            **common,
        ),
        "project_pages.json": envelope.wrap(
            "project_page",
            VERSION,
            pages,
            sources=SCORE_SOURCES,
            window_start=start,
            window_end=cut,
            status=status,
            **common,
        ),
    }
    bt_rows = backtest_rows(bt)
    pending = [r["id"] for r in bt_rows if r["outcome"] == "pending data"]
    docs["backtests.json"] = envelope.wrap(
        "backtest",
        VERSION,
        bt_rows,
        sources=SCORE_SOURCES,
        status=("pending data", f"windows not loaded: {', '.join(pending)}")
        if pending
        else ("ok", f"{len(bt_rows)} incidents"),
        **common,
    )
    bench = benchmark_rows(benchmarks_md.read_text("utf-8"))
    if not bench:
        raise ValueError(f"no scale-ladder rows in {benchmarks_md}")
    docs["benchmarks.json"] = envelope.wrap(
        "benchmark", VERSION, bench, sources=BENCH_SOURCES, **common
    )
    for name, doc in docs.items():
        _write(out / name, doc)
    pm = json.loads(metrics.read_text("utf-8"))
    tm.validate(pm)
    shutil.copyfile(metrics, out / "pipeline_metrics.json")
    for src in sorted(metadata_dir.glob("*.json")):
        dest = out / "metadata" / src.name
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dest)
    return write_index(out, produced, now)


def _describe(out: Path, path: Path) -> tuple[str, str | None]:
    """Dataset name and the schema that validates the file (checked again here)."""
    doc = json.loads(path.read_text("utf-8"))
    if path.name == "pipeline_metrics.json":
        tm.validate(doc)
        return "pipeline_metrics", "pipeline_metrics.schema.json"
    if isinstance(doc, dict) and "envelope_version" in doc:
        envelope.validate(doc)
        rows = envelope.row_schema_path(doc["dataset"])
        return str(doc["dataset"]), rows.name if rows.exists() else envelope.SCHEMA_PATH.name
    meta = CONTRACTS_DIR / f"metadata_{path.stem}.schema.json"
    jsonschema.validate(doc, json.loads(meta.read_text("utf-8")))  # missing schema: an error
    return f"metadata_{path.stem}", meta.name


def write_index(
    out: Path, produced: dict[str, Any], now: datetime | None = None
) -> list[dict[str, Any]]:
    """`index.json`: every other JSON file under `out` with its checksum and schema."""
    rows = []
    for path in sorted(out.rglob("*.json")):
        rel = path.relative_to(out).as_posix()
        if rel == "index.json":
            continue
        dataset, schema = _describe(out, path)
        rows.append(
            {
                "path": rel,
                "asset": rel.replace("/", "__"),  # the flat release asset name (publish.yml)
                "dataset": dataset,
                "sha256": _sha(path),
                "bytes": path.stat().st_size,
                "schema": schema,
            }
        )
    _write(
        out / "index.json",
        envelope.wrap("index", VERSION, rows, produced=produced, sources=SCORE_SOURCES, now=now),
    )
    return rows
