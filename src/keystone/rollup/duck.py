"""`free` rollup engine: DuckDB, incremental and idempotent (Phase 4, ADR-025).

State lives in one DuckDB file: the rollup tables and `rollup_files`, the silver hours already
rolled up (with a fingerprint of their silver files and their event-time range). A run
compares silver with that state; for every new, rewritten (re-parsed) or removed hour it
recomputes each *window* the hour touches (old range and new), reading every silver hour whose
event-time range overlaps those windows (from the `hour_types` table, so a late hour corrects
earlier windows), then deletes and re-inserts those windows in one transaction. Recomputing
from silver, never adding deltas, makes a re-run a no-op and the result independent of arrival
order.
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
"""


@dataclass
class RunStats:
    changed_files: int = 0
    removed_files: int = 0
    input_files: int = 0
    windows: dict[str, int] = field(default_factory=dict)
    rows_deleted: dict[str, int] = field(default_factory=dict)
    rows_written: dict[str, int] = field(default_factory=dict)
    seconds: float = 0.0


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
            WHERE s.source_file IS NULL OR f.fingerprint IS DISTINCT FROM s.fingerprint"""
        )
        stats.changed_files = con.execute(
            "SELECT count(DISTINCT source_file) FROM changed WHERE source_file IN "
            "(SELECT source_file FROM silver_hours)"
        ).fetchone()[0]  # type: ignore[index]
        stats.removed_files = con.execute(
            "SELECT count(*) FROM rollup_files WHERE source_file NOT IN "
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
                """INSERT INTO rollup_files
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

    def _recompute(self, r: Rollup, stats: RunStats) -> tuple[int, list[str]]:
        con = self.con
        step = f"INTERVAL 1 {r.unit}"
        con.execute(
            f"""CREATE OR REPLACE TEMP TABLE affected AS
            SELECT DISTINCT unnest(generate_series(
                date_trunc('{r.unit}', created_min), date_trunc('{r.unit}', created_max), {step}
            )) AS w FROM changed"""
        )
        files = [
            row[0]
            for row in con.execute(
                f"""SELECT DISTINCT s.events_path FROM silver_hours s, affected a
                WHERE s.created_min < a.w + {step} AND s.created_max >= a.w
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
        stats.rows_written[r.name] = written
        n = con.execute("SELECT count(*) FROM affected").fetchone()
        return (int(n[0]) if n else 0), files

    def counts(self) -> dict[str, int]:
        out = {}
        for r in rollups():
            row = self.con.execute(f"SELECT count(*) FROM {r.name}").fetchone()
            out[r.name] = int(row[0]) if row else 0
        row = self.con.execute("SELECT count(*) FROM rollup_files").fetchone()
        out["rollup_files"] = int(row[0]) if row else 0
        return out
