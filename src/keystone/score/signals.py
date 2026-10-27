"""Raw signals per repository at a point-in-time cut (score_v1, ADR-044).

A cut is the first instant of a UTC month. The window is the 12 months before it, half-open
[cut - 12 months, cut): nothing at or after the cut is read. Per-actor inputs are monthly, so a
month counts only when it ends at or before the cut. All timestamps are UTC (the connection's
time zone is set to UTC).

Inputs (DuckDB views on one connection):
- `repo_hour` (rollup, required): window_start, repo_id, era, event_type, human_events, opened,
  closed, published, added.
- `repo_actor_month` (optional, internal: salted actor keys, never published): month, repo_id,
  actor_key, events, pushes, prs_merged, prs_closed, releases, issues_closed. Derived from
  `repo_actor_day` when only that exists.
- `vulns`, `mapped` (optional, warehouse): OSV advisories per mapped repository; mapped packages.
- `criticality` (optional, warehouse, live only): dependent repositories per repository.
"""

import csv
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import duckdb

from keystone.paths import REPO_ROOT

SEEDS = REPO_ROOT / "dbt" / "seeds"
MAPPED_STATUSES = ("accepted", "auto_exact")

# Maintenance actions per era set (era-map.md): the PR merged flag exists only in E1-E2.
MAINT_SQL = {
    "maint_v1": "pushes + prs_merged + releases + issues_closed",  # E1-E2 windows
    "maint_v2": "pushes + prs_closed + releases + issues_closed",  # E3 windows
    "maint_v0": "pushes + releases + issues_closed",  # windows mixing eras, or E0
}


def month_start(d: date | datetime) -> datetime:
    return datetime(d.year, d.month, 1, tzinfo=UTC)


def add_months(d: datetime, n: int) -> datetime:
    y, m = divmod(d.month - 1 + n, 12)
    return d.replace(year=d.year + y, month=m + 1, day=1)


@dataclass(frozen=True)
class Window:
    cut: datetime

    def __post_init__(self) -> None:
        c = self.cut
        if (c.day, c.hour, c.minute, c.second, c.microsecond) != (1, 0, 0, 0, 0):
            raise ValueError(f"a cut is the first instant of a UTC month, not {c.isoformat()}")
        if c.utcoffset() != UTC.utcoffset(None):
            raise ValueError("a cut must be in UTC")

    @property
    def start(self) -> datetime:
        return add_months(self.cut, -12)

    @property
    def half(self) -> datetime:
        return add_months(self.cut, -6)

    @property
    def recent(self) -> datetime:
        return add_months(self.cut, -3)

    @property
    def hours(self) -> int:
        return int((self.cut - self.start).total_seconds() // 3600)


def _boundaries(seed: str, key: str) -> list[tuple[str, datetime]]:
    with (SEEDS / seed).open(newline="", encoding="utf-8") as fh:
        rows = [(r[key], datetime.fromisoformat(r["start_at"])) for r in csv.DictReader(fh)]
    return sorted(rows, key=lambda r: r[1])


def spans(seed: str, key: str, start: datetime, end: datetime) -> list[str]:
    """Labels of the periods (eras or completeness periods) that intersect [start, end)."""
    b = _boundaries(seed, key)
    out = []
    for i, (label, s) in enumerate(b):
        e = b[i + 1][1] if i + 1 < len(b) else datetime.max.replace(tzinfo=UTC)
        if s < end and e > start:
            out.append(label)
    return out


def maint_version(eras: list[str]) -> str:
    if eras and set(eras) <= {"E1", "E2"}:
        return "maint_v1"
    if eras == ["E3"]:
        return "maint_v2"
    return "maint_v0"


@dataclass
class Inputs:
    con: duckdb.DuckDBPyConnection
    actor_data: bool
    vuln_data: bool
    crit_data: bool
    names: bool  # dim_repo_name_history available

    def close(self) -> None:
        self.con.close()


def _tables(con: duckdb.DuckDBPyConnection, catalog: str) -> set[str]:
    rows = con.execute(
        "SELECT table_schema || '.' || table_name FROM information_schema.tables "
        "WHERE table_catalog = ?",
        [catalog],
    ).fetchall()
    return {r[0] for r in rows}


ACTOR_MONTH_FROM_DAY = """
SELECT CAST(date_trunc('month', window_start) AS DATE) AS month, repo_id, actor_key,
       SUM(events) AS events, SUM(pushes) AS pushes, SUM(prs_merged) AS prs_merged,
       SUM(prs_closed) AS prs_closed, SUM(releases) AS releases,
       SUM(issues_closed) AS issues_closed
FROM {src} GROUP BY ALL
"""


def open_inputs(rollup: Path, warehouse: Path | None = None) -> Inputs:
    """Views over a rollup DuckDB file or a directory of Parquet (`repo_hour/`, and optionally
    `repo_actor_month/` or `repo_actor_day/`), plus the warehouse when given."""
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    # Bounded memory with spill to disk: the backtest reads every window's Parquet (GBs) and a
    # runner was shut down at the default limit (80% of RAM, no temp directory; ADR-070).
    spill = Path(tempfile.gettempdir()) / "keystone-duckdb-spill"
    spill.mkdir(parents=True, exist_ok=True)
    limit = os.environ.get("KEYSTONE_DUCKDB_MEMORY_LIMIT", "6GB")
    con.execute(f"SET memory_limit = '{limit}'; SET temp_directory = '{spill.as_posix()}'")
    con.execute("SET preserve_insertion_order = false; SET max_temp_directory_size = '10GB'")
    actor = False
    if rollup.is_file():
        con.execute(f"ATTACH '{rollup.as_posix()}' AS r (READ_ONLY)")
        t = _tables(con, "r")
        con.execute("CREATE VIEW repo_hour AS SELECT * FROM r.main.repo_hour")
        if "main.repo_actor_month" in t:
            con.execute("CREATE VIEW repo_actor_month AS SELECT * FROM r.main.repo_actor_month")
            actor = True
        elif "main.repo_actor_day" in t:
            src = "r.main.repo_actor_day"
            con.execute(f"CREATE VIEW repo_actor_month AS {ACTOR_MONTH_FROM_DAY.format(src=src)}")
            actor = True
    else:

        def pq(name: str) -> str | None:
            d = rollup / name
            if d.is_dir() and any(d.rglob("*.parquet")):
                return f"read_parquet('{(d / '**' / '*.parquet').as_posix()}', union_by_name=true)"
            return None

        rh = pq("repo_hour")
        if rh is None:
            raise FileNotFoundError(f"no repo_hour Parquet under {rollup}")
        con.execute(f"CREATE VIEW repo_hour AS SELECT * FROM {rh}")
        if (am := pq("repo_actor_month")) is not None:
            con.execute(f"CREATE VIEW repo_actor_month AS SELECT * FROM {am}")
            actor = True
        elif (ad := pq("repo_actor_day")) is not None:
            con.execute(f"CREATE VIEW repo_actor_month AS {ACTOR_MONTH_FROM_DAY.format(src=ad)}")
            actor = True
    vuln = crit = names = False
    if warehouse is not None and warehouse.is_file():
        con.execute(f"ATTACH '{warehouse.as_posix()}' AS w (READ_ONLY)")
        t = _tables(con, "w")
        st = ", ".join(f"'{s}'" for s in MAPPED_STATUSES)
        if "marts.map_package_repo" in t:
            con.execute(
                "CREATE VIEW mapped AS SELECT repo_id, package_key FROM w.marts.map_package_repo "
                f"WHERE status IN ({st}) AND repo_id IS NOT NULL"
            )
            if "marts.fct_vuln" in t:
                con.execute(
                    "CREATE VIEW vulns AS SELECT m.repo_id, v.vuln_id, v.published_at, "
                    "v.withdrawn_at, v.severity_label FROM w.marts.fct_vuln v "
                    "JOIN mapped m USING (package_key) WHERE NOT COALESCE(v.is_malicious, false)"
                )
                vuln = True
            if "staging.stg_registry__packages" in t:
                con.execute(
                    "CREATE VIEW criticality AS SELECT m.repo_id, "
                    "MAX(p.dependent_repos) AS dependent_repos, MAX(p.fetched_at) AS fetched_at "
                    "FROM w.staging.stg_registry__packages p JOIN mapped m USING (package_key) "
                    "WHERE p.found GROUP BY m.repo_id"
                )
                crit = True
        names = "marts.dim_repo_name_history" in t
    return Inputs(con, actor, vuln, crit, names)


def hour_coverage(inp: Inputs, w: Window, recent: bool = False) -> float:
    """Share of the window's hours (or of its last 3 months) with any rollup row."""
    start = w.recent if recent else w.start
    (n,) = inp.con.execute(
        "SELECT COUNT(DISTINCT date_trunc('hour', window_start)) FROM repo_hour "
        "WHERE window_start >= ? AND window_start < ?",
        [start, w.cut],
    ).fetchone() or (0,)
    return round(int(n) / ((w.cut - start).total_seconds() // 3600), 4)


def resolve_name(inp: Inputs, name: str, w: Window) -> int | None:
    """Repository id that held `name` before the cut (latest spell), from the name history."""
    if not inp.names:
        return None
    row = inp.con.execute(
        "SELECT repo_id FROM w.marts.dim_repo_name_history WHERE repo_name_lower = ? "
        "AND valid_from < ? ORDER BY valid_from DESC LIMIT 1",
        [name.lower(), w.cut],
    ).fetchone()
    return int(row[0]) if row else None


def _scope(inp: Inputs, repo_ids: list[int] | None) -> str:
    inp.con.execute("CREATE OR REPLACE TEMP TABLE scope (repo_id BIGINT)")
    if repo_ids is None:
        return ""
    if not repo_ids:
        return "AND false"
    inp.con.executemany("INSERT INTO scope VALUES (?)", [[r] for r in repo_ids])
    return "AND repo_id IN (SELECT repo_id FROM scope)"


def compute(inp: Inputs, w: Window, repo_ids: list[int] | None = None) -> dict[int, dict[str, Any]]:
    """Raw signals for `repo_ids` (all repositories with activity in the window when None)."""
    scope = _scope(inp, repo_ids)
    eras = spans("schema_eras.csv", "era", w.start, w.cut)
    periods = spans("completeness_periods.csv", "period", w.start, w.cut)
    mv = maint_version(eras)
    base = {
        "window_start": w.start.isoformat(),
        "cut": w.cut.isoformat(),
        "eras": eras,
        "completeness_periods": periods,
        "one_completeness_period": len(periods) == 1,
        "maint_version": mv,
        "actor_data": inp.actor_data,
        "vuln_data": inp.vuln_data,
    }
    out: dict[int, dict[str, Any]] = {}
    act = inp.con.execute(
        f"""
        SELECT repo_id,
          COALESCE(SUM(human_events) FILTER (WHERE window_start >= $recent), 0),
          COALESCE(SUM(human_events) FILTER (WHERE window_start < $recent), 0),
          COALESCE(SUM(human_events), 0),
          COALESCE(SUM(opened) FILTER (
            WHERE event_type IN ('IssuesEvent', 'PullRequestEvent')), 0),
          COALESCE(SUM(closed) FILTER (
            WHERE event_type IN ('IssuesEvent', 'PullRequestEvent')), 0),
          COALESCE(SUM(published) FILTER (WHERE event_type = 'ReleaseEvent'), 0),
          MAX(window_start) FILTER (WHERE event_type = 'ReleaseEvent' AND published > 0),
          COALESCE(SUM(added) FILTER (
            WHERE event_type = 'MemberEvent' AND window_start >= $half), 0),
          COALESCE(SUM(added) FILTER (WHERE event_type = 'MemberEvent'), 0)
        FROM repo_hour
        WHERE window_start >= $start AND window_start < $cut AND repo_id IS NOT NULL {scope}
        GROUP BY repo_id
        """,
        {"start": w.start, "cut": w.cut, "recent": w.recent, "half": w.half},
    ).fetchall()
    for r in act:
        last = r[7]
        out[int(r[0])] = base | {
            "recent_events": int(r[1]),
            "prior_events": int(r[2]),
            "human_events": int(r[3]),
            "opened": int(r[4]),
            "closed": int(r[5]),
            "releases": int(r[6]),
            "days_since_release": None if last is None else (w.cut - last).days,
            "members_added_6m": int(r[8]),
            "members_added_12m": int(r[9]),
        }
    if inp.actor_data:
        rows = inp.con.execute(
            f"""
            WITH am AS (
              SELECT * FROM repo_actor_month
              WHERE month >= CAST($start AS DATE)
                AND month + INTERVAL 1 MONTH <= CAST($cut AS DATE) {scope}
            ), per AS (
              SELECT repo_id, actor_key, SUM({MAINT_SQL[mv]}) AS m,
                SUM({MAINT_SQL[mv]}) FILTER (WHERE month < CAST($half AS DATE)) AS e1,
                SUM({MAINT_SQL[mv]}) FILTER (WHERE month >= CAST($half AS DATE)) AS e2
              FROM am GROUP BY repo_id, actor_key
            ), ranked AS (
              SELECT *, row_number() OVER (PARTITION BY repo_id ORDER BY m DESC, actor_key) AS rn
              FROM per
            )
            SELECT repo_id, SUM(m), COUNT(*) FILTER (WHERE m > 0), MAX(m),
              SUM(m) FILTER (WHERE rn <= 3),
              COUNT(*) FILTER (WHERE COALESCE(e1, 0) > 0),
              COUNT(*) FILTER (WHERE COALESCE(e1, 0) > 0 AND COALESCE(e2, 0) = 0),
              COUNT(*) FILTER (WHERE COALESCE(e1, 0) = 0 AND COALESCE(e2, 0) > 0)
            FROM ranked GROUP BY repo_id
            """,
            {"start": w.start, "cut": w.cut, "half": w.half},
        ).fetchall()
        for r in rows:
            s = out.setdefault(int(r[0]), dict(base))
            total = int(r[1] or 0)
            s |= {
                "maint_actions": total,
                "maint_actors": int(r[2]),
                "top1": round(int(r[3] or 0) / total, 4) if total else None,
                "top3": round(int(r[4] or 0) / total, 4) if total else None,
                "first_half_actors": int(r[5]),
                "departed": int(r[6]),
                "new_actors": int(r[7]),
            }
    if inp.vuln_data:
        mapped = inp.con.execute(
            f"SELECT repo_id, COUNT(DISTINCT package_key) FROM mapped WHERE true {scope} "
            "GROUP BY repo_id"
        ).fetchall()
        vulns = dict(
            ((int(r[0])), (int(r[1]), int(r[2])))
            for r in inp.con.execute(
                f"""
                SELECT repo_id, COUNT(DISTINCT vuln_id),
                  COUNT(DISTINCT vuln_id) FILTER (WHERE severity_label IN ('HIGH', 'CRITICAL'))
                FROM vulns
                WHERE published_at < $cut AND (withdrawn_at IS NULL OR withdrawn_at >= $cut)
                  {scope}
                GROUP BY repo_id
                """,
                {"cut": w.cut},
            ).fetchall()
        )
        for repo, n in mapped:
            if int(repo) in out:
                v, h = vulns.get(int(repo), (0, 0))
                out[int(repo)] |= {"mapped_packages": int(n), "vulns": v, "vulns_high": h}
    return out


def criticality_inputs(inp: Inputs, repo_ids: list[int]) -> dict[int, dict[str, Any]]:
    """Current dependents per repository (a snapshot: never used for a past cut, ADR-044)."""
    if not inp.crit_data:
        return {}
    scope = _scope(inp, repo_ids)
    rows = inp.con.execute(
        f"SELECT repo_id, dependent_repos, fetched_at FROM criticality WHERE true {scope}"
    ).fetchall()
    return {
        int(r[0]): {"dependent_repos": r[1], "fetched_at": r[2].isoformat() if r[2] else None}
        for r in rows
    }
