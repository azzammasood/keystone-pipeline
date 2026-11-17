"""`free` rollup engine: DuckDB, incremental and idempotent (Phase 4, ADR-025).

State lives in one DuckDB file: the rollup tables and `rollup_files`, the silver hours already
rolled up (with a fingerprint of their silver files and their event-time range). A run
compares silver with that state; for every new, rewritten (re-parsed) or removed hour it
recomputes each *window* the hour touches (old range and new), reading every silver hour whose
event-time range overlaps those windows (from the `hour_types` table, so a late hour corrects
earlier windows), then deletes and re-inserts those windows in one transaction. Recomputing
from silver, never adding deltas, makes a re-run a no-op and the result independent of arrival
order.

Retention (ADR-089): `retire` deletes silver hours older than a cutoff once they are rolled up.
Their `rollup_files` rows stay, flagged `retired_at`, so a retired hour is never taken for a
removed one: the windows it touched keep their aggregates and are frozen. A new hour (never
rolled up, e.g. a late or caught-up file) that touches a frozen window is merged into it: every
measure is a SUM or a MAX of row-level contributions, so adding the new hour's contribution is
exact. A re-parsed hour (already counted once) cannot be taken out of a frozen window, so it is
skipped and recorded in `rollup_late_skipped`. Before silver goes, the name history the warehouse
needs (`stg_gharchive__repo_names`) is kept in compact form, in the same transaction:
`repo_name_runs` (consecutive sightings of one exact name per repository id merged) and
`repo_name_hour_max_id` (the largest repository id seen per hour).
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from keystone.rollup.contract import duckdb_projection
from keystone.rollup.spec import Rollup, duckdb_select, input_sql, rollups

_STATE = """
CREATE TABLE IF NOT EXISTS rollup_files (
    source_file VARCHAR PRIMARY KEY,
    source_sha256 VARCHAR NOT NULL,
    events_path VARCHAR NOT NULL,
    created_min TIMESTAMPTZ NOT NULL,
    created_max TIMESTAMPTZ NOT NULL,
    rolled_at TIMESTAMPTZ NOT NULL
);
ALTER TABLE rollup_files ADD COLUMN IF NOT EXISTS fingerprint VARCHAR;
ALTER TABLE rollup_files ADD COLUMN IF NOT EXISTS retired_at TIMESTAMPTZ;
ALTER TABLE rollup_files ADD COLUMN IF NOT EXISTS events BIGINT;
CREATE TABLE IF NOT EXISTS rollup_late_skipped (
    source_file VARCHAR PRIMARY KEY,
    source_sha256 VARCHAR NOT NULL,
    fingerprint VARCHAR NOT NULL,
    created_min TIMESTAMPTZ NOT NULL,
    created_max TIMESTAMPTZ NOT NULL,
    skipped_at TIMESTAMPTZ NOT NULL,
    retired_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS repo_name_runs (
    repo_id BIGINT NOT NULL,
    repo_name VARCHAR NOT NULL,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    era_min VARCHAR NOT NULL,
    era_max VARCHAR NOT NULL,
    events BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS repo_name_hour_max_id (
    seen_hour TIMESTAMPTZ PRIMARY KEY,
    max_id BIGINT NOT NULL
);
"""

# The overlap rule of ADR-020, as the name-history models apply it (int_repo_name_hours).
_E1_OVERLAP = "era = 'E1' AND created_at >= TIMESTAMPTZ '2015-01-01 00:00:00+00'"


@dataclass
class RunStats:
    changed_files: int = 0
    removed_files: int = 0
    input_files: int = 0
    windows: dict[str, int] = field(default_factory=dict)
    rows_deleted: dict[str, int] = field(default_factory=dict)
    rows_written: dict[str, int] = field(default_factory=dict)
    late_skipped: int = 0  # re-parsed hours not recomputed: they touch a retired window
    late_merged: int = 0  # new hours merged into retired (frozen) windows
    seconds: float = 0.0


@dataclass
class RetireStats:
    cutoff: datetime
    retired_files: int = 0  # rolled-up hours retired now
    retired_skipped: int = 0  # late-skipped hours retired now (never in the aggregates)
    retired_events: int = 0
    kept_unrolled: int = 0  # older than the cutoff but not rolled up yet: kept
    kept_unresolved: int = 0  # names without a repository id (E0, E1): kept in silver
    name_runs: int = 0
    deleted_files: int = 0
    deleted_bytes: int = 0


def _sql_list(values: list[str]) -> str:
    return "[" + ", ".join("'" + v.replace("'", "''") + "'" for v in values) + "]"


class DuckRollup:
    def __init__(self, db_path: Path, silver_dir: Path) -> None:
        self.silver_dir = silver_dir
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(db_path))
        self.con.execute("SET TimeZone = 'UTC'")
        self.con.execute("SET enable_progress_bar = false")
        self.con.execute(_STATE)
        for r in rollups():
            self._ensure_table(r)

    def close(self) -> None:
        self.con.close()

    def _ensure_table(self, r: Rollup) -> None:
        # Typed by the query itself over an empty input, so the table cannot drift from the SQL.
        self._input_view([])
        self.con.execute(f"CREATE TABLE IF NOT EXISTS {r.name} AS {duckdb_select(r)} LIMIT 0")

    def _input_view(self, files: list[str]) -> None:
        if files:
            src = f"read_parquet({_sql_list(files)}, union_by_name = true)"
        else:  # an empty relation with the contract's columns and silver's types
            src = "(SELECT * FROM _empty_events)"
            self._empty_events()
        self.con.execute(
            f"CREATE OR REPLACE TEMP VIEW events AS SELECT {duckdb_projection()} FROM {src}"
        )
        self.con.execute(f"CREATE OR REPLACE TEMP VIEW rollup_input AS {input_sql()}")

    def _empty_events(self) -> None:
        self.con.execute(
            """CREATE TEMP TABLE IF NOT EXISTS _empty_events (
                source_file VARCHAR, source_sha256 VARCHAR, fetched_at TIMESTAMPTZ,
                source_line BIGINT, era VARCHAR, period VARCHAR, event_type VARCHAR,
                created_at TIMESTAMPTZ, actor_key VARCHAR, actor_key_kind VARCHAR,
                actor_is_bot BOOLEAN, repo_id BIGINT, action VARCHAR, pr_merged BOOLEAN)"""
        )

    def _silver_hours(self) -> None:
        """Temp table `silver_hours`: one row per silver hour (from hour_types), with a
        fingerprint of the silver output itself. The source SHA-256 alone is not enough: a
        re-parse (parser fix, bot list, salt) rewrites silver from the same raw file. Silver
        files are replaced atomically, so their size and mtime change on every rewrite."""
        self.con.execute(
            """CREATE OR REPLACE TEMP TABLE silver_hours (
                source_file VARCHAR, source_sha256 VARCHAR, events_path VARCHAR,
                created_min TIMESTAMPTZ, created_max TIMESTAMPTZ, fingerprint VARCHAR)"""
        )
        types = sorted((self.silver_dir / "hour_types").glob("*/*.parquet"))
        if not types:
            return
        rows = self.con.execute(
            f"""SELECT source_file, any_value(source_sha256), any_value(filename),
                   min(created_at_min), max(created_at_max)
            FROM read_parquet({_sql_list([t.as_posix() for t in types])}, filename = true)
            GROUP BY source_file"""
        ).fetchall()
        out = []
        for source_file, sha, fname, cmin, cmax in rows:
            t = Path(fname)
            events = self.silver_dir / "events" / t.parent.name / t.name
            stamps = [
                f"{x.stat().st_size}:{x.stat().st_mtime_ns}" if x.exists() else "-"
                for x in (events, t)
            ]
            out.append((source_file, sha, events.as_posix(), cmin, cmax, ":".join([sha, *stamps])))
        self.con.executemany("INSERT INTO silver_hours VALUES (?, ?, ?, ?, ?, ?)", out)

    def run(self) -> RunStats:
        started = datetime.now(UTC)
        stats = RunStats()
        self._silver_hours()
        con = self.con
        # Hours whose content changed: new, rewritten (fingerprint differs) or gone from silver.
        con.execute(
            """CREATE OR REPLACE TEMP TABLE changed AS
            SELECT s.source_file, s.created_min, s.created_max FROM silver_hours s
            LEFT JOIN rollup_files f USING (source_file)
            WHERE f.source_file IS NULL OR f.fingerprint IS DISTINCT FROM s.fingerprint
            UNION ALL
            SELECT f.source_file, f.created_min, f.created_max FROM rollup_files f
            LEFT JOIN silver_hours s USING (source_file)
            WHERE f.retired_at IS NULL
              AND (s.source_file IS NULL OR f.fingerprint IS DISTINCT FROM s.fingerprint)"""
        )
        con.execute(
            """DELETE FROM changed WHERE source_file IN (
                SELECT k.source_file FROM rollup_late_skipped k JOIN silver_hours s
                USING (source_file) WHERE k.fingerprint = s.fingerprint
                AND k.source_file IN (SELECT source_file FROM rollup_files))"""
        )
        stats.late_skipped, stats.late_merged = self._skip_frozen(started)
        stats.changed_files = con.execute(
            "SELECT count(DISTINCT source_file) FROM changed WHERE source_file IN "
            "(SELECT source_file FROM silver_hours)"
        ).fetchone()[0]  # type: ignore[index]
        stats.removed_files = con.execute(
            "SELECT count(*) FROM rollup_files WHERE retired_at IS NULL AND source_file IN "
            "(SELECT source_file FROM changed) AND source_file NOT IN "
            "(SELECT source_file FROM silver_hours)"
        ).fetchone()[0]  # type: ignore[index]
        if stats.changed_files == 0 and stats.removed_files == 0:
            stats.seconds = (datetime.now(UTC) - started).total_seconds()
            return stats

        inputs: set[str] = set()
        con.execute("BEGIN TRANSACTION")
        try:
            for r in rollups():
                n_windows, files = self._recompute(r, stats)
                stats.windows[r.name] = n_windows
                inputs.update(files)
            con.execute(
                "DELETE FROM rollup_files WHERE source_file IN (SELECT source_file FROM changed)"
            )
            con.execute(
                """INSERT INTO rollup_files (source_file, source_sha256, events_path, created_min,
                    created_max, rolled_at, fingerprint)
                SELECT source_file, source_sha256, events_path, created_min, created_max, ?,
                       fingerprint
                FROM silver_hours WHERE source_file IN (SELECT source_file FROM changed)""",
                [started],
            )
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
        stats.input_files = len(inputs)
        stats.seconds = (datetime.now(UTC) - started).total_seconds()
        return stats

    def _skip_frozen(self, now: datetime) -> tuple[int, int]:
        """Sort the changed hours whose windows (any rollup) overlap a retired hour. A new hour
        (no `rollup_files` row) stays in `changed`, listed in `merge_files`: its contribution
        is merged into the frozen windows. Any other is dropped from `changed`: a re-parsed hour
        still in silver is recorded in `rollup_late_skipped` (its fingerprint, so it is not
        retried); a rolled-up hour gone from silver is retired in place, its aggregates kept.
        Returns (skipped, merged)."""
        con = self.con
        hits = " UNION ".join(
            f"""SELECT c.source_file FROM (
                SELECT source_file, unnest(generate_series(
                    date_trunc('{r.unit}', created_min), date_trunc('{r.unit}', created_max),
                    INTERVAL 1 {r.unit})) AS w FROM changed) c
            JOIN rollup_files f ON f.retired_at IS NOT NULL
                AND f.created_min < c.w + INTERVAL 1 {r.unit} AND f.created_max >= c.w"""
            for r in rollups()
        )
        con.execute(f"CREATE OR REPLACE TEMP TABLE frozen_hit AS {hits}")
        con.execute(
            """CREATE OR REPLACE TEMP TABLE merge_files AS
            SELECT DISTINCT h.source_file FROM frozen_hit h
            JOIN silver_hours s USING (source_file)
            WHERE h.source_file NOT IN (SELECT source_file FROM rollup_files)"""
        )
        con.execute(
            "DELETE FROM rollup_late_skipped WHERE source_file IN "
            "(SELECT source_file FROM merge_files)"
        )
        con.execute(
            "DELETE FROM frozen_hit WHERE source_file IN (SELECT source_file FROM merge_files)"
        )
        con.execute(
            """INSERT OR REPLACE INTO rollup_late_skipped
            SELECT source_file, source_sha256, fingerprint, created_min, created_max, ?, NULL
            FROM silver_hours WHERE source_file IN (SELECT source_file FROM frozen_hit)""",
            [now],
        )
        con.execute(
            """UPDATE rollup_files SET retired_at = ? WHERE retired_at IS NULL
            AND source_file IN (SELECT source_file FROM frozen_hit)
            AND source_file NOT IN (SELECT source_file FROM silver_hours)""",
            [now],
        )
        n = con.execute(
            "SELECT count(*) FROM frozen_hit WHERE source_file IN "
            "(SELECT source_file FROM silver_hours)"
        ).fetchone()
        con.execute("DELETE FROM changed WHERE source_file IN (SELECT source_file FROM frozen_hit)")
        merged = _one(con, "SELECT count(*) FROM merge_files")
        return (int(n[0]) if n else 0), merged

    def _recompute(self, r: Rollup, stats: RunStats) -> tuple[int, list[str]]:
        con = self.con
        step = f"INTERVAL 1 {r.unit}"
        con.execute(
            f"""CREATE OR REPLACE TEMP TABLE affected AS
            SELECT DISTINCT unnest(generate_series(
                date_trunc('{r.unit}', created_min), date_trunc('{r.unit}', created_max), {step}
            )) AS w FROM changed"""
        )
        # Frozen windows (overlapping a retired hour) are merged into, never recomputed; only
        # hours in `merge_files` can reach them (_skip_frozen dropped the others).
        con.execute(
            f"""CREATE OR REPLACE TEMP TABLE frozen AS
            SELECT DISTINCT a.w FROM affected a JOIN rollup_files f ON f.retired_at IS NOT NULL
                AND f.created_min < a.w + {step} AND f.created_max >= a.w"""
        )
        n_affected = _one(con, "SELECT count(*) FROM affected")
        con.execute("DELETE FROM affected WHERE w IN (SELECT w FROM frozen)")
        merged_files: list[str] = []
        if _one(con, "SELECT count(*) FROM frozen"):
            merged_files = self._merge_frozen(r, stats)
        files = [
            row[0]
            for row in con.execute(
                f"""SELECT DISTINCT s.events_path FROM silver_hours s, affected a
                WHERE s.created_min < a.w + {step} AND s.created_max >= a.w
                AND s.source_file NOT IN (
                    SELECT k.source_file FROM rollup_late_skipped k JOIN silver_hours h
                    USING (source_file) WHERE k.fingerprint = h.fingerprint)
                ORDER BY 1"""
            ).fetchall()
        ]
        deleted = con.execute(
            f"DELETE FROM {r.name} WHERE window_start IN (SELECT w FROM affected)"
        ).fetchone()
        stats.rows_deleted[r.name] = int(deleted[0]) if deleted else 0
        written = 0
        if files:
            self._input_view(files)
            where = f"WHERE date_trunc('{r.unit}', created_at) IN (SELECT w FROM affected)"
            res = con.execute(f"INSERT INTO {r.name} {duckdb_select(r, where)}").fetchone()
            written = int(res[0]) if res else 0
        stats.rows_written[r.name] = written + stats.rows_written.get(r.name, 0)
        return n_affected, sorted(set(files) | set(merged_files))

    def _merge_frozen(self, r: Rollup, stats: RunStats) -> list[str]:
        """Add the contribution of the `merge_files` hours to the frozen windows: SUM measures
        add, MAX measures take the larger value (spec: every measure is one or the other)."""
        con = self.con
        files = [
            row[0]
            for row in con.execute(
                """SELECT events_path FROM silver_hours
                WHERE source_file IN (SELECT source_file FROM merge_files) ORDER BY 1"""
            ).fetchall()
        ]
        if not files:
            return []
        self._input_view(files)
        where = f"WHERE date_trunc('{r.unit}', created_at) IN (SELECT w FROM frozen)"
        con.execute(f"CREATE OR REPLACE TEMP TABLE frozen_delta AS {duckdb_select(r, where)}")
        cols = ", ".join(r.columns)
        keys = ", ".join(("window_start", *r.keys))
        aggs = ", ".join(
            f"MAX({m}) AS {m}" if m in r.max_measures else f"CAST(SUM({m}) AS BIGINT) AS {m}"
            for m in r.measures
        )
        con.execute(
            f"""CREATE OR REPLACE TEMP TABLE frozen_merged AS
            SELECT {keys}, {aggs} FROM (
                SELECT {cols} FROM {r.name} WHERE window_start IN (SELECT w FROM frozen)
                UNION ALL SELECT {cols} FROM frozen_delta
            ) AS both_parts GROUP BY {keys}"""
        )
        con.execute(f"DELETE FROM {r.name} WHERE window_start IN (SELECT w FROM frozen)")
        res = con.execute(f"INSERT INTO {r.name} ({cols}) SELECT {cols} FROM frozen_merged")
        row = res.fetchone()
        stats.rows_written[r.name] = int(row[0]) if row else 0
        return files

    def counts(self) -> dict[str, int]:
        out = {}
        for r in rollups():
            row = self.con.execute(f"SELECT count(*) FROM {r.name}").fetchone()
            out[r.name] = int(row[0]) if row else 0
        row = self.con.execute("SELECT count(*) FROM rollup_files").fetchone()
        out["rollup_files"] = int(row[0]) if row else 0
        return out

    def retire(self, cutoff: datetime) -> RetireStats:
        """Retire the silver hours whose events all precede `cutoff` (ADR-089): rolled-up hours
        (fingerprint unchanged) and late-skipped ones. Their name history goes into
        `repo_name_runs` and `repo_name_hour_max_id` and they are flagged `retired_at`, in one
        transaction; then their silver files are deleted (a crash before that leaves files the
        next call deletes). Hours with names lacking a repository id stay in silver: the name
        resolution (map_repo_name_id) needs them per event hour."""
        stats = RetireStats(cutoff=cutoff)
        self._silver_hours()
        con = self.con
        con.execute(
            """CREATE OR REPLACE TEMP TABLE retire_cand AS
            SELECT s.source_file, s.events_path, FALSE AS skipped FROM silver_hours s
            JOIN rollup_files f USING (source_file)
            WHERE f.retired_at IS NULL AND f.fingerprint = s.fingerprint AND s.created_max < ?
            UNION ALL
            SELECT s.source_file, s.events_path, TRUE FROM silver_hours s
            JOIN rollup_late_skipped k USING (source_file)
            WHERE k.retired_at IS NULL AND k.fingerprint = s.fingerprint AND s.created_max < ?
              AND s.source_file IN (SELECT source_file FROM rollup_files)""",
            [cutoff, cutoff],
        )
        n_old = _one(con, "SELECT count(*) FROM silver_hours WHERE created_max < ?", [cutoff])
        paths = [r[0] for r in con.execute("SELECT events_path FROM retire_cand").fetchall()]
        paths = [p for p in paths if Path(p).exists()]
        n_cand = 0
        if paths:
            con.execute(
                f"""CREATE OR REPLACE TEMP VIEW retire_events AS
                SELECT source_file, era, created_at, repo_id, repo_name
                FROM read_parquet({_sql_list(paths)}, union_by_name = true)"""
            )
            con.execute(
                f"""CREATE OR REPLACE TEMP TABLE retire_unresolved AS
                SELECT DISTINCT source_file FROM retire_events
                WHERE repo_id IS NULL AND repo_name IS NOT NULL AND NOT ({_E1_OVERLAP})"""
            )
            con.execute(
                "DELETE FROM retire_cand WHERE source_file IN "
                "(SELECT source_file FROM retire_unresolved)"
            )
            stats.kept_unresolved = _one(con, "SELECT count(*) FROM retire_unresolved")
            n_cand = _one(con, "SELECT count(*) FROM retire_cand")
        stats.kept_unrolled = n_old - n_cand - stats.kept_unresolved
        if n_cand:
            self._retire_names(stats)
        stats.name_runs = _one(con, "SELECT count(*) FROM repo_name_runs")
        self._delete_retired(stats)
        return stats

    def _retire_names(self, stats: RetireStats) -> None:
        con = self.con
        now = datetime.now(UTC)
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(
                """CREATE OR REPLACE TEMP TABLE retire_rows AS
                SELECT * FROM retire_events
                WHERE source_file IN (SELECT source_file FROM retire_cand)"""
            )
            # The hourly rows of int_repo_name_hours, for the retiring hours only.
            con.execute(
                f"""CREATE OR REPLACE TEMP TABLE retire_name_hours AS
                SELECT repo_id, repo_name, min(created_at) AS first_seen_at,
                       max(created_at) AS last_seen_at, min(era) AS era_min,
                       max(era) AS era_max, CAST(count(*) AS BIGINT) AS events
                FROM retire_rows
                WHERE repo_id IS NOT NULL AND repo_name IS NOT NULL AND NOT ({_E1_OVERLAP})
                GROUP BY repo_id, repo_name, date_trunc('hour', created_at)"""
            )
            # Merge consecutive sightings (per id, in the order dim_repo_name_history uses) of
            # one exact name into a run: min, max and sum carry over, so the spells match.
            con.execute(
                """CREATE OR REPLACE TEMP TABLE merged_runs AS
                WITH touched AS (
                    SELECT * FROM repo_name_runs
                    WHERE repo_id IN (SELECT repo_id FROM retire_name_hours)
                    UNION ALL SELECT * FROM retire_name_hours
                ), flagged AS (
                    SELECT *, CASE WHEN lag(repo_name) OVER (PARTITION BY repo_id
                        ORDER BY first_seen_at, repo_name) = repo_name THEN 0 ELSE 1 END AS s
                    FROM touched
                ), grouped AS (
                    SELECT *, sum(s) OVER (PARTITION BY repo_id ORDER BY first_seen_at,
                        repo_name ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS g
                    FROM flagged
                )
                SELECT repo_id, any_value(repo_name) AS repo_name,
                       min(first_seen_at) AS first_seen_at, max(last_seen_at) AS last_seen_at,
                       min(era_min) AS era_min, max(era_max) AS era_max,
                       CAST(sum(events) AS BIGINT) AS events
                FROM grouped GROUP BY repo_id, g"""
            )
            con.execute(
                "DELETE FROM repo_name_runs WHERE repo_id IN (SELECT repo_id FROM merged_runs)"
            )
            con.execute("INSERT INTO repo_name_runs SELECT * FROM merged_runs")
            con.execute(
                """INSERT INTO repo_name_hour_max_id
                SELECT date_trunc('hour', created_at), max(repo_id) FROM retire_rows
                WHERE repo_id IS NOT NULL AND repo_name IS NOT NULL GROUP BY 1
                ON CONFLICT (seen_hour) DO UPDATE
                SET max_id = greatest(max_id, excluded.max_id)"""
            )
            con.execute(
                """CREATE OR REPLACE TEMP TABLE retire_counts AS
                SELECT c.source_file, c.skipped, CAST(count(r.source_file) AS BIGINT) AS events
                FROM retire_cand c LEFT JOIN retire_rows r USING (source_file)
                GROUP BY c.source_file, c.skipped"""
            )
            con.execute(
                """UPDATE rollup_files f SET retired_at = ?, events = c.events
                FROM retire_counts c
                WHERE c.source_file = f.source_file AND f.retired_at IS NULL""",
                [now],
            )
            con.execute(
                """UPDATE rollup_late_skipped SET retired_at = ? WHERE retired_at IS NULL
                AND source_file IN (SELECT source_file FROM retire_counts WHERE skipped)""",
                [now],
            )
            stats.retired_files = _one(con, "SELECT count(*) FROM retire_counts WHERE NOT skipped")
            stats.retired_skipped = _one(con, "SELECT count(*) FROM retire_counts WHERE skipped")
            stats.retired_events = _one(
                con, "SELECT coalesce(sum(events), 0) FROM retire_counts WHERE NOT skipped"
            )
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise

    def _delete_retired(self, stats: RetireStats) -> None:
        """Delete the silver files of every retired hour still on disk."""
        rows = self.con.execute(
            """SELECT events_path FROM silver_hours WHERE source_file IN (
                SELECT source_file FROM rollup_files WHERE retired_at IS NOT NULL
                UNION SELECT source_file FROM rollup_late_skipped WHERE retired_at IS NOT NULL)
            AND source_file NOT IN (
                SELECT source_file FROM rollup_late_skipped WHERE retired_at IS NULL)"""
        ).fetchall()
        for (events_path,) in rows:
            events = Path(events_path)
            types = self.silver_dir / "hour_types" / events.parent.name / events.name
            for f in (events, types):  # events first: `is_done` then says not done
                if f.exists():
                    stats.deleted_bytes += f.stat().st_size
                    f.unlink()
                    stats.deleted_files += 1
            for d in (events.parent, types.parent):
                if d.is_dir() and not any(d.iterdir()):
                    d.rmdir()

    def retention(self) -> dict[str, int]:
        """Retention counts for pipeline_metrics (ADR-089)."""
        con = self.con
        return {
            "retired_hours": _one(
                con, "SELECT count(*) FROM rollup_files WHERE retired_at IS NOT NULL"
            ),
            "retired_events": _one(
                con,
                "SELECT coalesce(sum(events), 0) FROM rollup_files WHERE retired_at IS NOT NULL",
            ),
            "late_skipped_hours": _one(con, "SELECT count(*) FROM rollup_late_skipped"),
            "name_runs": _one(con, "SELECT count(*) FROM repo_name_runs"),
        }


def _one(con: duckdb.DuckDBPyConnection, sql: str, params: list[object] | None = None) -> int:
    row = con.execute(sql, params or []).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def retired_sources(db_path: Path) -> set[str]:
    """Source files of retired hours (rolled up or late-skipped): `is_done` for the ingest, so
    a long re-check never downloads and parses them again."""
    if not db_path.exists():
        return set()
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        tables = {r[0] for r in con.execute("SELECT table_name FROM duckdb_tables()").fetchall()}
        if "rollup_late_skipped" not in tables:
            return set()
        rows = con.execute(
            """SELECT source_file FROM rollup_files WHERE retired_at IS NOT NULL
            UNION SELECT source_file FROM rollup_late_skipped WHERE retired_at IS NOT NULL"""
        ).fetchall()
    finally:
        con.close()
    return {r[0] for r in rows}
