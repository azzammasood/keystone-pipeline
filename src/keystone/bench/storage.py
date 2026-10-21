"""Storage per era and year (Phase 9): measured on the sample hours, projected as estimates.

Measured (exact bytes on this machine):

* **raw**: the gzip size of every sample hour (manifest `files.bytes`).
* **silver**: the Parquet (zstd) written by the parser for that hour: `events` + `hour_types`.
* **rollups**: the DuckDB rollups (`rollup/sql`) of every *full* sample day (24 hours under one
  date partition), written as one zstd Parquet file per rollup and day, which is how a rolling
  pipeline would keep them.

Projected (every number labelled *estimate*):

* raw bytes per day: the mean size of the Phase 1 HEAD sweep hours (`spike/head_sweep.csv`:
  day 15 at 0/6/12/18h every month since 2011-03, weekly since 2025-06) of that month, x 24;
  a month without sweep hours takes the nearest month's mean;
* silver and rollups per day: raw x the measured ratio (silver/raw, rollups/raw) of the same
  era, pooled by bytes over its sample dates; low and high use the smallest and largest ratio
  of a single sample date of that era.

Upper-side caveats: gaps in the archive are not subtracted; the ratio of an era is applied to
all of its years although payloads (and so bytes per event) grew over time.
"""

import csv
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import duckdb

from keystone.rollup.contract import duckdb_projection
from keystone.rollup.spec import duckdb_select, input_sql, rollups

ARCHIVE_START = date(2011, 2, 12)
# Day-level era boundaries (docs/era-map.md; the boundary days themselves are split by hour there).
ERA_STARTS = (("E0", date(2011, 2, 12)), ("E1", date(2012, 3, 10)),
              ("E2", date(2015, 1, 1)), ("E3", date(2025, 10, 8)))  # fmt: skip


def era_of(d: date) -> str:
    era = ERA_STARTS[0][0]
    for name, start in ERA_STARTS:
        if d >= start:
            era = name
    return era


def windows(end: date) -> dict[str, tuple[date, date]]:
    """Half-open [start, end) date ranges to project."""
    last24 = date(end.year - 2, end.month, min(end.day, 28))
    return {
        "full archive": (ARCHIVE_START, end),
        "24 months before 2018-11": (date(2016, 11, 1), date(2018, 11, 1)),
        "24 months before 2022-01": (date(2020, 1, 1), date(2022, 1, 1)),
        "24 months before 2024-03": (date(2022, 3, 1), date(2024, 3, 1)),
        "last 24 months": (last24, end),
    }


@dataclass
class SampleDate:
    date: str
    era: str
    hours: int
    events: int
    raw_bytes: int
    silver_bytes: int
    rollup_bytes: dict[str, int] | None = None  # full days only
    rollup_rows: dict[str, int] | None = None
    # A storage candidate, not a pipeline table: each hourly rollup summed to days.
    candidate_bytes: dict[str, int] | None = None
    candidate_rows: dict[str, int] | None = None


def _sql_list(paths: list[Path]) -> str:
    return "[" + ", ".join(f"'{p.as_posix()}'" for p in paths) + "]"


def sample_dates(root: Path) -> list[SampleDate]:
    silver = root / "silver" / "gharchive"
    types = sorted((silver / "hour_types").glob("*/*.parquet"))
    con = duckdb.connect()
    con.execute(f"ATTACH '{(root / 'manifest' / 'manifest.duckdb').as_posix()}' AS m (READ_ONLY)")
    hours = con.execute(
        f"""SELECT parse_filename(filename, true) AS stem, any_value(era), sum(events)
        FROM read_parquet({_sql_list(types)}, filename = true) GROUP BY stem"""
    ).fetchall()
    raw = dict(
        con.execute(
            """SELECT regexp_extract(url, '([^/]+)\\.json\\.gz$', 1), max(bytes)
            FROM m.files WHERE source = 'gharchive' GROUP BY 1"""
        ).fetchall()
    )
    con.close()
    out: dict[str, SampleDate] = {}
    for stem, era, events in hours:
        if stem not in raw:
            raise ValueError(f"sample hour {stem} has silver but no raw size in the manifest")
        d = stem.rsplit("-", 1)[0]
        part = f"date={d}/{stem}.parquet"
        size = (silver / "events" / part).stat().st_size + (
            silver / "hour_types" / part
        ).stat().st_size
        s = out.setdefault(d, SampleDate(d, str(era), 0, 0, 0, 0))
        if s.era != era:
            s.era = f"{s.era}+{era}" if str(era) not in s.era else s.era
        s.hours += 1
        s.events += int(events)
        s.raw_bytes += int(raw[stem])
        s.silver_bytes += size
    return sorted(out.values(), key=lambda s: s.date)


def measure_rollups(root: Path, s: SampleDate, tmp: Path) -> None:
    """Roll up one full sample day and store each rollup as one zstd Parquet file."""
    files = sorted((root / "silver" / "gharchive" / "events" / f"date={s.date}").glob("*.parquet"))
    tmp.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("SET enable_progress_bar = false")
    con.execute(
        f"CREATE VIEW events AS SELECT {duckdb_projection()} "
        f"FROM read_parquet({_sql_list(files)}, union_by_name = true)"
    )
    con.execute(f"CREATE VIEW rollup_input AS {input_sql()}")
    s.rollup_bytes, s.rollup_rows = {}, {}
    s.candidate_bytes, s.candidate_rows = {}, {}
    for r in rollups():
        target = tmp / f"{s.date}-{r.name}.parquet"
        con.execute(f"COPY ({duckdb_select(r)}) TO '{target.as_posix()}' "
                    "(FORMAT parquet, COMPRESSION zstd)")  # fmt: skip
        row = con.execute(f"SELECT count(*) FROM read_parquet('{target.as_posix()}')").fetchone()
        s.rollup_rows[r.name] = int(row[0]) if row else 0
        s.rollup_bytes[r.name] = target.stat().st_size
        if r.unit == "hour":
            day = tmp / f"{s.date}-{r.name}-days.parquet"
            aggs = ", ".join(
                f"MAX({m}) AS {m}" if m in r.max_measures else f"CAST(SUM({m}) AS BIGINT) AS {m}"
                for m in r.measures
            )
            con.execute(
                f"COPY (SELECT date_trunc('day', window_start) AS window_start, "
                f"{', '.join(r.keys)}, {aggs} FROM read_parquet('{target.as_posix()}') "
                f"GROUP BY ALL) TO '{day.as_posix()}' (FORMAT parquet, COMPRESSION zstd)"
            )
            n = con.execute(f"SELECT count(*) FROM read_parquet('{day.as_posix()}')").fetchone()
            s.candidate_rows[f"{r.name} summed to days"] = int(n[0]) if n else 0
            s.candidate_bytes[f"{r.name} summed to days"] = day.stat().st_size
            day.unlink()
        target.unlink()
    con.close()


def raw_per_day_by_month(sweep_csv: Path) -> dict[str, float]:
    """Mean raw gzip bytes per hour of each swept month, x 24 (estimate)."""
    sums: dict[str, list[int]] = {}
    with sweep_csv.open(encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row["status"] == "200" and row["bytes"]:
                sums.setdefault(row["hour"][:7], []).append(int(row["bytes"]))
    return {m: 24 * sum(v) / len(v) for m, v in sums.items()}


def _nearest(months: dict[str, float], month: str) -> float:
    def dist(m: str) -> int:
        return abs((int(m[:4]) * 12 + int(m[5:7])) - (int(month[:4]) * 12 + int(month[5:7])))

    return months[min(months, key=dist)]


@dataclass
class Ratio:
    pooled: float
    low: float
    high: float
    dates: list[str]


def era_ratios(samples: list[SampleDate], value: str) -> dict[str, Ratio]:
    """`value` / raw per era: pooled by bytes, low/high over single sample dates."""
    out: dict[str, Ratio] = {}
    for era in (e for e, _ in ERA_STARTS):
        pts = [s for s in samples if s.era == era and _value(s, value) is not None]
        if not pts:
            continue
        vals = [(_value(s, value) or 0) / s.raw_bytes for s in pts]
        pooled = sum(_value(s, value) or 0 for s in pts) / sum(s.raw_bytes for s in pts)
        out[era] = Ratio(pooled, min(vals), max(vals), [s.date for s in pts])
    return out


def _value(s: SampleDate, value: str) -> int | None:
    if value == "silver":
        return s.silver_bytes
    if s.rollup_bytes is None or s.hours != 24:
        return None
    return sum(s.rollup_bytes.values())


def _days(start: date, end: date) -> list[date]:
    return [start + timedelta(n) for n in range((end - start).days)]


def project(samples: list[SampleDate], raw_months: dict[str, float], end: date) -> dict[str, Any]:
    silver_r = era_ratios(samples, "silver")
    rollup_r = era_ratios(samples, "rollup")
    per_day: dict[date, dict[str, float]] = {}
    for d in _days(ARCHIVE_START, end):
        raw = _nearest(raw_months, d.isoformat()[:7])
        sr, rr = silver_r[era_of(d)], rollup_r[era_of(d)]
        per_day[d] = {
            "raw": raw,
            "silver": raw * sr.pooled,
            "silver_low": raw * sr.low,
            "silver_high": raw * sr.high,
            "rollup": raw * rr.pooled,
            "rollup_low": raw * rr.low,
            "rollup_high": raw * rr.high,
        }
    keys = list(next(iter(per_day.values())))

    def total(days: list[date]) -> dict[str, float]:
        return {k: sum(per_day[d][k] for d in days) for k in keys}

    years = {}
    for y in range(ARCHIVE_START.year, end.year + 1):
        days = [d for d in per_day if d.year == y]
        t = total(days)
        years[str(y)] = {
            "days": len(days),
            "eras": sorted({era_of(d) for d in days}),
            "per_day": {k: v / len(days) for k, v in t.items()},
            "total": t,
        }
    wins = {}
    for name, (a, b) in windows(end).items():
        days = [d for d in _days(a, b) if d in per_day]
        wins[name] = {"start": a.isoformat(), "end": b.isoformat(), "days": len(days),
                      "total": total(days)}  # fmt: skip
    return {
        "label": "estimate",
        "ratios": {
            "silver_per_raw": _ratios_json(silver_r),
            "rollups_per_raw": _ratios_json(rollup_r),
        },
        "years": years,
        "windows": wins,
    }


def _ratios_json(r: dict[str, Ratio]) -> dict[str, Any]:
    return {e: {"pooled": x.pooled, "low": x.low, "high": x.high, "sample_dates": x.dates}
            for e, x in r.items()}  # fmt: skip


def build_report(
    root: Path, extra_roots: tuple[Path, ...] = (), end: date | None = None
) -> dict[str, Any]:
    """`root` holds the samples and the HEAD sweep; `extra_roots` add sample days parsed
    elsewhere (each with its own silver/ and manifest/)."""
    end = end or datetime.now(UTC).date()
    samples: list[SampleDate] = []
    for r in (root, *extra_roots):
        found = sample_dates(r)
        tmp = r / "bench" / "tmp-storage"
        for s in found:
            if s.hours == 24:
                measure_rollups(r, s, tmp)
        if tmp.exists() and not any(tmp.iterdir()):
            tmp.rmdir()
        samples += found
    samples.sort(key=lambda s: s.date)
    raw_months = raw_per_day_by_month(root / "spike" / "head_sweep.csv")
    return {
        "measured_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "end": end.isoformat(),
        "samples": [s.__dict__ for s in samples],
        "projection": project(samples, raw_months, end),
    }


def write_report(report: dict[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=1, default=str), encoding="utf-8")
    return path


def _gb(n: float) -> str:
    return f"{n / 1e9:,.1f}"


def _mb(n: float) -> str:
    return f"{n / 1e6:,.1f}"


def render_markdown(report: dict[str, Any]) -> str:
    out = ["### Measured: sample dates (bytes on disk)", "",
           "| date | era | hours | events | raw gz MB | silver MB | silver/raw | "
           "rollups MB (day) | rollup rows |",
           "|---|---|---:|---:|---:|---:|---:|---:|---:|"]  # fmt: skip
    for s in report["samples"]:
        rb = "-" if s["rollup_bytes"] is None else _mb(sum(s["rollup_bytes"].values()))
        rr = "-" if s["rollup_rows"] is None else f"{sum(s['rollup_rows'].values()):,}"
        out.append(
            f"| {s['date']} | {s['era']} | {s['hours']} | {s['events']:,} | "
            f"{_mb(s['raw_bytes'])} | {_mb(s['silver_bytes'])} | "
            f"{s['silver_bytes'] / s['raw_bytes']:.3f} | {rb} | {rr} |"
        )
    cands = [s for s in report["samples"] if s.get("candidate_bytes")]
    if cands:
        out += ["", "### Measured candidate: hourly rollups summed to days (full sample days)", "",
                "| date | table | rows | MB |", "|---|---|---:|---:|"]  # fmt: skip
        for s in cands:
            for name, b in s["candidate_bytes"].items():
                out.append(f"| {s['date']} | {name} | {s['candidate_rows'][name]:,} | {_mb(b)} |")
    p = report["projection"]
    out += ["", "### Ratios per era (measured)", "",
            "| era | silver/raw pooled (low-high) | rollups/raw pooled (low-high) |",
            "|---|---|---|"]  # fmt: skip
    for era, sr in p["ratios"]["silver_per_raw"].items():
        rr = p["ratios"]["rollups_per_raw"].get(era)
        rtxt = "-" if rr is None else f"{rr['pooled']:.4f} ({rr['low']:.4f}-{rr['high']:.4f})"
        out.append(f"| {era} | {sr['pooled']:.3f} ({sr['low']:.3f}-{sr['high']:.3f}) | {rtxt} |")
    out += ["", "### Per year (estimate)", "",
            "| year | eras | days | raw MB/day | silver MB/day | rollups MB/day | raw GB | "
            "silver GB (low-high) | rollups GB |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|"]  # fmt: skip
    for y, v in p["years"].items():
        d, t = v["per_day"], v["total"]
        out.append(
            f"| {y} | {'+'.join(v['eras'])} | {v['days']} | {_mb(d['raw'])} | {_mb(d['silver'])} | "
            f"{_mb(d['rollup'])} | {_gb(t['raw'])} | {_gb(t['silver'])} "
            f"({_gb(t['silver_low'])}-{_gb(t['silver_high'])}) | {_gb(t['rollup'])} |"
        )
    out += ["", "### Windows (estimate)", "",
            "| window | from | to | days | raw GB | silver GB (low-high) | rollups GB (low-high) |",
            "|---|---|---|---:|---:|---:|---:|"]  # fmt: skip
    for name, w in p["windows"].items():
        t = w["total"]
        out.append(
            f"| {name} | {w['start']} | {w['end']} | {w['days']} | {_gb(t['raw'])} | "
            f"{_gb(t['silver'])} ({_gb(t['silver_low'])}-{_gb(t['silver_high'])}) | "
            f"{_gb(t['rollup'])} ({_gb(t['rollup_low'])}-{_gb(t['rollup_high'])}) |"
        )
    return "\n".join(out)
