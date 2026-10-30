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
import yaml

from keystone.paths import CONFIG_DIR
from keystone.rollup.contract import duckdb_projection
from keystone.rollup.spec import duckdb_select, input_sql, rollups

ARCHIVE_START = date(2011, 2, 12)
INCIDENTS_PATH = CONFIG_DIR / "incidents.yml"
ACTOR_FIELDS = ("actor_key", "actor_key_kind", "pr_author_key", "issue_author_key")
SILVER_PUBLIC = "silver without actor fields"
CONTRIBUTORS = "contributors per repo and day (key-free)"
# Rates for sizing GitHub Actions jobs (estimates; measured on this machine, not on a runner):
# download 232.7 MB in 46 s with 2 connections (Phase 2 gate, 2026-10-20); parse in one process
# 4,071,556 events of 2026-08-01 in about 12 min (file times) and 166,432 events of
# 2025-10-06 12h in 25.2 s (timed): the slower, about 5,600 events/s, is used.
DOWNLOAD_BYTES_PER_S = 232.7e6 / 46
PARSE_EVENTS_PER_S = 5_600
JOB_BUDGET_S = 5 * 3600  # the 6 h job limit with an hour of margin
RUNNER_DISK_BYTES = 14e9  # standard Linux runner SSD
ASSET_LIMIT_BYTES = 2 * 2**30  # GitHub: each release asset under 2 GiB
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
        if r.name == "repo_actor_day":
            _key_free_contributors(con, s, target, tmp)
        target.unlink()
    _silver_without_actors(con, s, files, tmp)
    con.close()


def _measure(con: duckdb.DuckDBPyConnection, s: SampleDate, name: str, sql: str, out: Path) -> None:
    con.execute(f"COPY ({sql}) TO '{out.as_posix()}' (FORMAT parquet, COMPRESSION zstd)")
    n = con.execute(f"SELECT count(*) FROM read_parquet('{out.as_posix()}')").fetchone()
    assert s.candidate_rows is not None and s.candidate_bytes is not None
    s.candidate_rows[name] = int(n[0]) if n else 0
    s.candidate_bytes[name] = out.stat().st_size
    out.unlink()


def _key_free_contributors(
    con: duckdb.DuckDBPyConnection, s: SampleDate, actor_day: Path, tmp: Path
) -> None:
    """What may leave a runner from `repo_actor_day` (ADR-039): per repository and day, the
    number of human actors, their events and the largest one's share (ADR-081). No actor key."""
    from keystone.backfill.chunk import CONTRIBUTORS_SQL

    sql = CONTRIBUTORS_SQL.replace(
        "FROM repo_actor_day", f"FROM read_parquet('{actor_day.as_posix()}')"
    )
    _measure(con, s, CONTRIBUTORS, sql, tmp / f"{s.date}-contributors.parquet")


def _silver_without_actors(
    con: duckdb.DuckDBPyConnection, s: SampleDate, files: list[Path], tmp: Path
) -> None:
    """The day's silver with every actor field dropped (the publishable 30-day window)."""
    cols = ", ".join(ACTOR_FIELDS)
    sql = (
        f"SELECT * EXCLUDE ({cols}) FROM read_parquet({_sql_list(files)}, "
        "union_by_name = true, hive_partitioning = false)"
    )
    _measure(con, s, SILVER_PUBLIC, sql, tmp / f"{s.date}-silver-public.parquet")


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
    """`silver`, `events`, `rollup` (all rollups), a rollup table or a candidate name."""
    if value == "silver":
        return s.silver_bytes
    if value == "events":
        return s.events
    if s.rollup_bytes is None or s.hours != 24:
        return None
    if value == "rollup":
        return sum(s.rollup_bytes.values())
    return s.rollup_bytes.get(value, (s.candidate_bytes or {}).get(value))


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


def load_incidents(path: Path = INCIDENTS_PATH) -> dict[str, Any]:
    cfg: dict[str, Any] = yaml.safe_load(path.read_text(encoding="utf-8"))
    return cfg


def _add_months(first: date, n: int) -> date:
    m = first.year * 12 + first.month - 1 + n
    return date(m // 12, m % 12 + 1, 1)


def design_windows(cfg: dict[str, Any], end: date) -> dict[str, tuple[date, date]]:
    """Half-open download windows of ADR-039 from `config/incidents.yml`."""
    lookback = int(cfg["lookback_months"])
    out = {}
    for inc in cfg["incidents"]:
        y, m = (int(x) for x in str(inc["month"]).split("-"))
        first = date(y, m, 1)
        out[str(inc["id"])] = (_add_months(first, -lookback), _add_months(first, 1))
    trailing = int(cfg["live"]["trailing_months"])
    out["live"] = (_add_months(date(end.year, end.month, 1), -trailing), end)
    return out


DESIGN_RATIOS = ("silver", "events", SILVER_PUBLIC, CONTRIBUTORS, "repo_hour", "repo_actor_day")
_TOTALS = ("raw", "events", "silver", "repo_hour", CONTRIBUTORS, "repo_actor_day")


def design(
    samples: list[SampleDate],
    raw_months: dict[str, float],
    cfg: dict[str, Any],
    end: date,
    silver_window_days: int = 30,
) -> dict[str, Any]:
    """Size ADR-039 (estimates): what each window downloads, what it keeps where, and the
    GitHub Actions jobs it needs. Published = `repo_hour` and the key-free contributor table
    (monthly Release assets); `repo_actor_day` exists only inside a job."""
    ratios = {k: era_ratios(samples, k) for k in DESIGN_RATIOS}

    def day(d: date) -> dict[str, float]:
        raw = _nearest(raw_months, d.isoformat()[:7])
        era = era_of(d)
        missing = [k for k in DESIGN_RATIOS if era not in ratios[k]]
        if missing:
            raise ValueError(f"no full sample day of era {era} for {missing}")
        v = {k: raw * ratios[k][era].pooled for k in DESIGN_RATIOS}
        v["raw"] = raw
        v["seconds"] = raw / DOWNLOAD_BYTES_PER_S + v["events"] / PARSE_EVENTS_PER_S
        return v

    def published(v: dict[str, float]) -> float:
        return v["repo_hour"] + v[CONTRIBUTORS]

    wins: dict[str, Any] = {}
    seen: set[date] = set()
    windows_ = design_windows(cfg, end)
    for name, (a, b) in windows_.items():
        days = [d for d in _days(a, b) if d not in seen]  # an overlap is downloaded once
        seen.update(days)
        per = {d: day(d) for d in days}
        jobs: list[list[date]] = []
        for d in days:  # consecutive days per job while the job stays within budget
            spent = sum(per[x]["seconds"] for x in jobs[-1]) if jobs else JOB_BUDGET_S
            if spent + per[d]["seconds"] <= JOB_BUDGET_S:
                jobs[-1].append(d)
            else:
                jobs.append([d])
        months: dict[str, float] = {}
        for d in days:
            months[d.isoformat()[:7]] = months.get(d.isoformat()[:7], 0.0) + published(per[d])
        # One raw hour streamed, the day's silver and actor rollup, the job's published output.
        disk = max(
            per[d]["raw"] / 24 + per[d]["silver"] + per[d]["repo_actor_day"]
            + sum(published(per[x]) for x in job)
            for job in jobs for d in job
        )  # fmt: skip
        wins[name] = {
            "start": a.isoformat(),
            "end": b.isoformat(),
            "days": len(days),
            "total": {k: sum(v[k] for v in per.values()) for k in _TOTALS},
            "jobs": len(jobs),
            "job_hours": sum(v["seconds"] for v in per.values()) / 3600,
            "longest_job_hours": max(sum(per[x]["seconds"] for x in j) for j in jobs) / 3600,
            "largest_monthly_asset": max(months.values()),
            "release_assets": 2 * len(months),
            "job_disk_peak": disk,
            "max_day_raw": max(v["raw"] for v in per.values()),
        }
    live_end = windows_["live"][1]
    window = [live_end - timedelta(n + 1) for n in range(silver_window_days)]
    totals: dict[str, float] = {k: sum(w["total"][k] for w in wins.values()) for k in _TOTALS}
    totals["jobs"] = sum(w["jobs"] for w in wins.values())
    totals["job_hours"] = sum(w["job_hours"] for w in wins.values())
    return {
        "label": "estimate",
        "config": cfg,
        "rates": {
            "download_bytes_per_s": DOWNLOAD_BYTES_PER_S,
            "parse_events_per_s": PARSE_EVENTS_PER_S,
            "job_budget_s": JOB_BUDGET_S,
        },
        "ratios": {k: _ratios_json(r) for k, r in ratios.items()},
        "windows": wins,
        "silver_window": {
            "days": silver_window_days,
            "silver": sum(day(d)["silver"] for d in window),
            "published_without_actor_fields": sum(day(d)[SILVER_PUBLIC] for d in window),
        },
        "totals": totals,
    }


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
        "design": design(samples, raw_months, load_incidents(), end),
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
        out += ["",
                "### Measured candidates (full sample days; storage options, not pipeline tables)",
                "", "| date | table | rows | MB |", "|---|---|---:|---:|"]  # fmt: skip
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
    if "design" in report:
        out += ["", render_design(report["design"])]
    return "\n".join(out)


def render_design(dz: dict[str, Any]) -> str:
    out = ["### ADR-039 design: downloaded windows (estimate)", "",
           "| window | from | to | days | raw downloaded GB | events (M) | silver made GB | "
           "published repo_hour GB | published contributors GB | in-job repo_actor_day GB | "
           "jobs | job-hours | longest job h | job disk peak GB | largest monthly asset MB |",
           "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]  # fmt: skip
    for name, w in dz["windows"].items():
        t = w["total"]
        out.append(
            f"| {name} | {w['start']} | {w['end']} | {w['days']} | {_gb(t['raw'])} | "
            f"{t['events'] / 1e6:,.0f} | {_gb(t['silver'])} | {_gb(t['repo_hour'])} | "
            f"{_gb(t[CONTRIBUTORS])} | {_gb(t['repo_actor_day'])} | {w['jobs']} | "
            f"{w['job_hours']:,.0f} | {w['longest_job_hours']:.1f} | "
            f"{_gb(w['job_disk_peak'])} | {_mb(w['largest_monthly_asset'])} |"
        )
    t = dz["totals"]
    out.append(
        f"| **total** | | | | {_gb(t['raw'])} | {t['events'] / 1e6:,.0f} | {_gb(t['silver'])} | "
        f"{_gb(t['repo_hour'])} | {_gb(t[CONTRIBUTORS])} | {_gb(t['repo_actor_day'])} | "
        f"{t['jobs']} | {t['job_hours']:,.0f} | | | |"
    )
    sw = dz["silver_window"]
    out += ["", f"Rolling silver window ({sw['days']} days ending today): {_gb(sw['silver'])} GB "
            f"as parsed; {_gb(sw['published_without_actor_fields'])} GB without actor fields "
            "(estimate)."]  # fmt: skip
    return "\n".join(out)
