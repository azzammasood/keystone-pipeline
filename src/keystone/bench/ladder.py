"""Scale ladder (Phase 9): one rollup workload, every engine, growing scopes.

Every rung reads the same input and runs the same rollups (`rollup/sql`, written once):

* **input**: the scope's silver hours projected once to the Avro event contract (the columns
  every engine reads; timestamps UTC milliseconds) as one Parquet file under
  `data/bench/in/<scope>/`. Preparing it is not timed; every engine then reads identical bytes.
* **duckdb**: in a child process (`duck_worker`), so its peak memory is its own.
* **spark**: Spark SQL 4.1.3 in a fresh `spark` container (local[2], the image's defaults).
* **flink**: Flink 2.2.1 SQL in batch mode on a freshly created session cluster (jobmanager and
  taskmanager of the `streaming` profile only), file source and file sink, the same TUMBLE
  wrapper as the full engine (`rollup/flink.py`).

Each engine writes Parquet to `data/bench/out/<engine>/<scope>/<rollup>/`; the harness then
counts rows, bytes and the sum of every count measure with DuckDB and checks that the engines
agree. Wall time is measured from the host and includes engine start-up (the JVM and container
for Spark, the SQL client for Flink); `engine_seconds` is measured inside the engine's process
or container. Peak memory: the child's peak working set / max RSS (DuckDB) and the containers'
cgroup v2 `memory.peak` (Spark: its container; Flink: jobmanager plus taskmanager, recreated
before each run). Cost is $0: everything runs on this machine, no billed service is touched.
"""

import json
import re
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, cast

import duckdb
import httpx
import psutil

from keystone.paths import DATA_DIR, REPO_ROOT
from keystone.rollup.contract import fields
from keystone.rollup.flink import _agg, _unit_sql
from keystone.rollup.spec import Rollup, duckdb_select, input_sql, rollups, strip_comments

Engine = Literal["duckdb", "spark", "flink"]
ENGINES: tuple[Engine, ...] = ("duckdb", "spark", "flink")
Kind = Literal["hour", "day", "month"]
BENCH_DIR = DATA_DIR / "bench"  # mounted at /bench in the Flink containers (compose)
COMPOSE = ["docker", "compose", "-f", str(REPO_ROOT / "compose" / "docker-compose.yml")]
FLINK_REST = "http://127.0.0.1:8081"
_KEYS = {"hour": r"\d{4}-\d{2}-\d{2}-\d{1,2}", "day": r"\d{4}-\d{2}-\d{2}", "month": r"\d{4}-\d{2}"}
_DUCK_TYPES = {"string": "VARCHAR", "long": "BIGINT", "int": "INTEGER", "boolean": "BOOLEAN"}


@dataclass(frozen=True)
class Scope:
    kind: Kind
    key: str  # hour: 2026-10-19-12, day: 2026-10-19, month: 2026-09

    @property
    def name(self) -> str:
        return f"{self.kind}-{self.key}"

    def files(self, silver_dir: Path) -> list[Path]:
        """Silver event files of the scope (partitions are the source file's date)."""
        events = silver_dir / "gharchive" / "events"
        if self.kind == "hour":
            date = self.key.rsplit("-", 1)[0]
            return sorted((events / f"date={date}").glob(f"{self.key}.parquet"))
        pattern = f"date={self.key}" if self.kind == "day" else f"date={self.key}-*"
        return sorted(events.glob(f"{pattern}/*.parquet"))


def parse_scope(text: str) -> Scope:
    kind, _, key = text.partition("=")
    if kind not in _KEYS or not re.fullmatch(_KEYS[kind], key):
        raise ValueError(
            f"scope must be hour=YYYY-MM-DD-H, day=YYYY-MM-DD or month=YYYY-MM: {text}"
        )
    return Scope(cast(Kind, kind), key)


def peak_rss_bytes() -> int:
    """Peak memory of this process: peak working set (Windows) or max RSS (POSIX)."""
    if sys.platform == "win32":
        return int(psutil.Process().memory_info().peak_wset)
    import resource

    scale = 1 if sys.platform == "darwin" else 1024  # ru_maxrss is KiB on Linux
    return int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss) * scale


@dataclass
class Prepared:
    scope: str
    path: Path
    silver_files: int
    silver_bytes: int
    rows: int
    bytes: int


def _contract_projection() -> str:
    cols = []
    for f in fields():
        if f.is_timestamp:
            cols.append(f"CAST(date_trunc('millisecond', {f.name}) AS TIMESTAMP) AS {f.name}")
        else:
            cols.append(f"CAST({f.name} AS {_DUCK_TYPES[f.base]}) AS {f.name}")
    return ", ".join(cols)


def prepare(scope: Scope, silver_dir: Path, bench_dir: Path = BENCH_DIR) -> Prepared:
    files = scope.files(silver_dir)
    if not files:
        raise FileNotFoundError(f"no silver hours for {scope.name} under {silver_dir}")
    out = bench_dir / "in" / scope.name
    out.mkdir(parents=True, exist_ok=True)
    for old in out.glob("*"):
        old.unlink()
    target = out / "part-0.parquet"
    src = "[" + ", ".join(f"'{p.as_posix()}'" for p in files) + "]"
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("SET enable_progress_bar = false")
    # Snappy: the codec every engine's Parquet reader has built in.
    con.execute(
        f"COPY (SELECT {_contract_projection()} FROM read_parquet({src}, union_by_name = true)) "
        f"TO '{target.as_posix()}' (FORMAT parquet, COMPRESSION snappy)"
    )
    rows = con.execute(f"SELECT count(*) FROM read_parquet('{target.as_posix()}')").fetchone()
    con.close()
    return Prepared(
        scope.name,
        out,
        len(files),
        sum(p.stat().st_size for p in files),
        int(rows[0]) if rows else 0,
        target.stat().st_size,
    )


@dataclass
class Output:
    files: int
    bytes: int
    rows: int
    sums: dict[str, int]  # sum of each count measure: the cross-engine check


@dataclass
class RunResult:
    engine: Engine
    scope: str
    measured_at: str
    input_rows: int
    input_bytes: int
    silver_files: int
    silver_bytes: int
    wall_seconds: float
    engine_seconds: float | None
    peak_memory_bytes: int | None
    peak_memory_method: str
    outputs: dict[str, Output]
    cost_usd: float = 0.0
    config: dict[str, Any] = field(default_factory=dict)


def _data_files(d: Path) -> list[Path]:
    return sorted(
        p
        for p in d.rglob("*")
        if p.is_file() and not p.name.startswith((".", "_")) and not p.name.endswith(".crc")
    )


def summarize(out_dir: Path) -> dict[str, Output]:
    result: dict[str, Output] = {}
    con = duckdb.connect()
    for r in rollups():
        files = _data_files(out_dir / r.name)
        counts = [m for m in r.measures if m not in r.max_measures]
        if not files:
            result[r.name] = Output(0, 0, 0, {m: 0 for m in counts})
            continue
        src = "[" + ", ".join(f"'{p.as_posix()}'" for p in files) + "]"
        sums = ", ".join(f"CAST(coalesce(sum({m}), 0) AS BIGINT)" for m in counts)
        row = con.execute(f"SELECT count(*), {sums} FROM read_parquet({src})").fetchone()
        assert row is not None
        result[r.name] = Output(
            len(files),
            sum(p.stat().st_size for p in files),
            int(row[0]),
            {m: int(v) for m, v in zip(counts, row[1:], strict=True)},
        )
    con.close()
    return result


def _clean(d: Path) -> None:
    if d.exists():
        for p in sorted(d.rglob("*"), reverse=True):
            p.unlink() if p.is_file() else p.rmdir()
    d.mkdir(parents=True, exist_ok=True)


def _run(cmd: list[str], timeout: int) -> str:
    proc = subprocess.run(
        cmd, capture_output=True, text=True, encoding="utf-8", timeout=timeout, check=False
    )
    out = proc.stdout + proc.stderr
    if proc.returncode != 0 or "[ERROR]" in out:
        raise RuntimeError(
            f"{' '.join(cmd[:6])}... failed (exit {proc.returncode}):\n{out[-3000:]}"
        )
    return out


# --- duckdb -----------------------------------------------------------------------------------


def run_duckdb(prep: Prepared, out: Path, timeout: int) -> tuple[float, float, int, str]:
    started = time.perf_counter()
    stdout = _run(
        [sys.executable, "-m", "keystone.bench.duck_worker", str(prep.path), str(out)], timeout
    )
    wall = time.perf_counter() - started
    info = json.loads(stdout.strip().splitlines()[-1])
    method = "child process peak working set" if sys.platform == "win32" else "child max RSS"
    return wall, float(info["engine_seconds"]), int(info["peak_bytes"]), method


# --- spark ------------------------------------------------------------------------------------


def render_spark(scope: str) -> str:
    out = [
        f"CREATE TEMPORARY VIEW events USING parquet OPTIONS (path '/bench/in/{scope}');",
        f"CREATE TEMPORARY VIEW rollup_input AS\n{strip_comments(input_sql())};",
    ]
    for r in rollups():
        # duckdb_select is portable to Spark SQL: date_trunc(unit, ts), CASE, CAST, SUM, MAX.
        select = strip_comments(duckdb_select(r))
        out.append(
            f"INSERT OVERWRITE DIRECTORY '/bench/out/spark/{scope}/{r.name}' USING parquet\n"
            f"{select};"
        )
    return "\n\n".join(out) + "\n"


_MARK = re.compile(r"KEYSTONE_BENCH (\d+) (\S+)")
# The container's peak memory: cgroup v2 `memory.peak`, else v1 `max_usage_in_bytes` (Docker
# Desktop's WSL2 kernel 5.15 is cgroup v1; v1 usage includes the page cache).
PEAK_SH = (
    "cat /sys/fs/cgroup/memory.peak 2>/dev/null"
    " || cat /sys/fs/cgroup/memory/memory.max_usage_in_bytes 2>/dev/null || echo -"
)
PEAK_METHOD = "container cgroup peak (v2 memory.peak or v1 max_usage_in_bytes, with page cache)"


def _peak(text: str) -> int | None:
    return int(text) if text.strip().isdigit() else None


def run_spark(prep: Prepared, bench_dir: Path, timeout: int) -> tuple[float, float, int | None]:
    jobs = bench_dir / "jobs"
    jobs.mkdir(parents=True, exist_ok=True)
    job = jobs / f"spark-{prep.scope}.sql"
    job.write_text(render_spark(prep.scope), encoding="utf-8", newline="\n")
    script = (
        "s=$(date +%s%N); /opt/spark/bin/spark-sql -S"
        # No OpenLineage events from bench runs; zstd like silver.
        " --conf spark.extraListeners= --conf spark.sql.parquet.compression.codec=zstd"
        f" -f /bench/jobs/{job.name}; rc=$?; e=$(date +%s%N);"
        f' echo "KEYSTONE_BENCH $(( (e - s) / 1000000 )) $({PEAK_SH})"; exit $rc'
    )
    cmd = [*COMPOSE, "--profile", "spark", "run", "--rm", "-T"]
    cmd += ["-v", f"{bench_dir.resolve()}:/bench", "spark", "sh", "-c", script]
    started = time.perf_counter()
    stdout = _run(cmd, timeout)
    wall = time.perf_counter() - started
    m = _MARK.search(stdout)
    if not m:
        raise RuntimeError(f"spark run printed no timing line:\n{stdout[-2000:]}")
    return wall, int(m.group(1)) / 1000, _peak(m.group(2))


# --- flink ------------------------------------------------------------------------------------


def _flink_type(m: str, r: Rollup) -> str:
    return "TIMESTAMP_LTZ(3)" if m in r.max_measures else "BIGINT"


def render_flink(scope: str) -> str:
    key_types = {f.name: f.flink_type.removesuffix(" NOT NULL") for f in fields()}
    cols = [f"  `{f.name}` {f.flink_type}" for f in fields()]
    cols.append("  WATERMARK FOR created_at AS created_at - INTERVAL '1' HOUR")
    out = [
        "SET 'execution.runtime-mode' = 'batch';",
        "SET 'table.dml-sync' = 'true';",
        "SET 'table.local-time-zone' = 'UTC';",
        f"SET 'pipeline.name' = 'keystone-bench-{scope}';",
        "CREATE TEMPORARY TABLE events (\n"
        + ",\n".join(cols)
        + f"\n) WITH ('connector' = 'filesystem', 'path' = 'file:///bench/in/{scope}', "
        "'format' = 'parquet');",
        f"CREATE TEMPORARY VIEW rollup_input AS\n{strip_comments(input_sql())};",
    ]
    inserts = []
    for r in rollups():
        keys = ", ".join(r.keys)
        sink_cols = ["window_start TIMESTAMP(3)"] + [f"{k} {key_types[k]}" for k in r.keys]
        sink_cols += [f"{m} {_flink_type(m, r)}" for m in r.measures]
        out.append(f"CREATE TEMPORARY VIEW {r.name}_contrib AS\n{strip_comments(r.sql)};")
        out.append(
            f"CREATE TEMPORARY TABLE {r.name}_out ({', '.join(sink_cols)}) WITH ("
            f"'connector' = 'filesystem', 'path' = 'file:///bench/out/flink/{scope}/{r.name}', "
            "'format' = 'parquet');"
        )
        aggs = ", ".join(_agg(r, m) for m in r.measures)
        inserts.append(
            f"INSERT INTO {r.name}_out\nSELECT window_start, {keys}, {aggs}\n"
            f"FROM TABLE(TUMBLE(TABLE {r.name}_contrib, DESCRIPTOR(created_at), "
            f"INTERVAL '1' {_unit_sql(r)}))\nGROUP BY window_start, window_end, {keys};"
        )
    out.append("EXECUTE STATEMENT SET BEGIN\n" + "\n".join(inserts) + "\nEND;")
    return "\n\n".join(out) + "\n"


def flink_fresh(timeout: int = 180) -> None:
    """Recreate the session cluster so its cgroups' memory.peak covers this run only."""
    _run([*COMPOSE, "--profile", "streaming", "up", "-d", "--force-recreate",
          "jobmanager", "taskmanager"], timeout)  # fmt: skip
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            r = httpx.get(f"{FLINK_REST}/taskmanagers", timeout=5)
            if r.status_code == 200 and r.json().get("taskmanagers"):
                return
        except httpx.HTTPError:
            pass
        time.sleep(2)
    raise TimeoutError("Flink taskmanager did not register")


def flink_stop() -> None:
    _run([*COMPOSE, "--profile", "streaming", "stop", "jobmanager", "taskmanager"], 180)


def _container_peak(service: str) -> int | None:
    out = _run([*COMPOSE, "exec", "-T", service, "sh", "-c", PEAK_SH], 60)
    return _peak(out)


def run_flink(prep: Prepared, bench_dir: Path, timeout: int) -> tuple[float, int | None]:
    jobs = bench_dir / "jobs"
    jobs.mkdir(parents=True, exist_ok=True)
    job = jobs / f"flink-{prep.scope}.sql"
    job.write_text(render_flink(prep.scope), encoding="utf-8", newline="\n")
    flink_fresh()
    cmd = [*COMPOSE, "exec", "-T", "jobmanager", "/opt/flink/bin/sql-client.sh"]
    started = time.perf_counter()
    _run([*cmd, "-f", f"/bench/jobs/{job.name}"], timeout)
    wall = time.perf_counter() - started
    peaks = [_container_peak(s) for s in ("jobmanager", "taskmanager")]
    total = sum(p for p in peaks if p is not None) if all(p is not None for p in peaks) else None
    return wall, total


# --- the ladder -------------------------------------------------------------------------------


def run_rung(
    engine: Engine, prep: Prepared, bench_dir: Path = BENCH_DIR, timeout: int = 1800
) -> RunResult:
    out = bench_dir / "out" / engine / prep.scope
    _clean(out)
    engine_s: float | None
    peak: int | None
    if engine == "duckdb":
        wall, engine_s, peak, method = run_duckdb(prep, out, timeout)
        config: dict[str, Any] = {"duckdb": duckdb.__version__, "threads": psutil.cpu_count()}
    elif engine == "spark":
        wall, engine_s, peak = run_spark(prep, bench_dir, timeout)
        method = PEAK_METHOD
        config = {"spark": "4.1.3", "master": "local[2]", "driver_memory": "768m",
                  "container_limit": "1200m"}  # fmt: skip
    else:
        wall, peak = run_flink(prep, bench_dir, timeout)
        engine_s = None
        method = f"jobmanager + taskmanager (fresh containers): {PEAK_METHOD}"
        config = {"flink": "2.2.1", "mode": "batch", "slots": 4,
                  "container_limits": "1200m + 1800m"}  # fmt: skip
    return RunResult(
        engine=engine,
        scope=prep.scope,
        measured_at=datetime.now(UTC).isoformat(timespec="seconds"),
        input_rows=prep.rows,
        input_bytes=prep.bytes,
        silver_files=prep.silver_files,
        silver_bytes=prep.silver_bytes,
        wall_seconds=round(wall, 3),
        engine_seconds=None if engine_s is None else round(engine_s, 3),
        peak_memory_bytes=peak,
        peak_memory_method=method,
        outputs=summarize(out),
        config=config,
    )


def parity(results: list[RunResult]) -> dict[str, bool]:
    """Per scope: do all engines that ran agree on rows and every count measure's sum?"""
    by_scope: dict[str, list[RunResult]] = {}
    for r in results:
        by_scope.setdefault(r.scope, []).append(r)
    return {
        s: all(
            (o.rows, o.sums) == (rs[0].outputs[n].rows, rs[0].outputs[n].sums)
            for r in rs
            for n, o in r.outputs.items()
        )
        for s, rs in by_scope.items()
    }


def latest(bench_dir: Path = BENCH_DIR) -> dict[tuple[str, str], dict[str, Any]]:
    """The latest recorded result per (engine, scope), from `results.jsonl`."""
    path = bench_dir / "results.jsonl"
    out: dict[tuple[str, str], dict[str, Any]] = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            r = json.loads(line)
            out[(r["engine"], r["scope"])] = r
    return out


def parity_recorded(bench_dir: Path = BENCH_DIR) -> dict[str, list[str]]:
    """Per scope, the engines whose latest run disagrees with the first engine's (empty: all
    agree on rows and count sums), across every run recorded so far."""
    by_scope: dict[str, list[dict[str, Any]]] = {}
    for (_, scope), r in sorted(latest(bench_dir).items()):
        by_scope.setdefault(scope, []).append(r)

    def sig(r: dict[str, Any]) -> list[tuple[str, int, dict[str, int]]]:
        return sorted((n, o["rows"], o["sums"]) for n, o in r["outputs"].items())

    return {s: [r["engine"] for r in rs if sig(r) != sig(rs[0])] for s, rs in by_scope.items()}


def record(results: list[RunResult], bench_dir: Path = BENCH_DIR) -> Path:
    path = bench_dir / "results.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        for r in results:
            fh.write(json.dumps(asdict(r), default=str) + "\n")
    return path
