"""`full` rollup engine: Flink SQL from Redpanda to Iceberg (Phase 4, ADR-024 and ADR-026).

Rendered from the same contribution queries as DuckDB (`spec.py`). One job per run:

* **stream** (live hours; also a bounded replay): Kafka source with an event-time watermark
  (`created_at - INTERVAL W`), TUMBLE window TVF per rollup, exactly-once into Iceberg on
  checkpoints. A row the window would drop as late (its window already closed by the watermark)
  is routed, with the same CURRENT_WATERMARK test, to a `<rollup>_late` table instead of being
  lost; nothing is dropped.
* **batch** (backfill): the same SQL in batch mode over the bounded topic; nothing is late.
* **fold** (batch): `<rollup>` = SUM/MAX over `<rollup>_ontime` UNION ALL `<rollup>_late`, which
  is how late events correct earlier windows in this engine. Overwrites the final table.

Jobs run through the SQL client inside the `jobmanager` container (`docker compose exec`).
"""

import re
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from keystone.paths import DATA_DIR, REPO_ROOT
from keystone.rollup.contract import fields
from keystone.rollup.spec import Rollup, input_sql, rollups, strip_comments

Mode = Literal["stream", "batch"]
COMPOSE = ["docker", "compose", "-f", str(REPO_ROOT / "compose" / "docker-compose.yml")]
JOBS_DIR = DATA_DIR / "flink" / "jobs"  # mounted at /jobs in the jobmanager
NAMESPACE = re.compile(r"[a-z][a-z0-9_]{0,62}")
WATERMARK_DELAY = "1"  # hours; ADR-024 (sampled files hold only their own hour's events)

CATALOG = """CREATE CATALOG ice WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://lakekeeper:8181/catalog',
  'warehouse' = 'keystone',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://rustfs:9000',
  's3.path-style-access' = 'true',
  'client.region' = 'local-01'
);"""

_KEY_TYPES = {f.name: f.flink_type.removesuffix(" NOT NULL") for f in fields()}


def _unit_sql(r: Rollup) -> str:
    return "HOUR" if r.unit == "hour" else "DAY"


def _table_ddl(r: Rollup, table: str, if_not_exists: bool = False) -> str:
    cols = ["  window_start TIMESTAMP_LTZ(6)"]
    cols += [f"  {k} {_KEY_TYPES[k]}" for k in r.keys]
    cols += [
        f"  {m} TIMESTAMP_LTZ(6)" if m in r.max_measures else f"  {m} BIGINT" for m in r.measures
    ]
    create = "CREATE TABLE IF NOT EXISTS" if if_not_exists else "CREATE TABLE"
    return f"{create} {table} (\n" + ",\n".join(cols) + "\n) WITH ('format-version' = '2');"


def _measure_cast(r: Rollup, m: str) -> str:
    return f"CAST({m} AS TIMESTAMP_LTZ(3))" if m in r.max_measures else f"CAST({m} AS BIGINT)"


def _agg(r: Rollup, m: str) -> str:
    if m in r.max_measures:
        return f"CAST(MAX({m}) AS TIMESTAMP_LTZ(3)) AS {m}"
    return f"CAST(SUM({m}) AS BIGINT) AS {m}"


def source_ddl(topic: str, group: str, bounded: bool) -> str:
    cols = [f"  `{f.name}` {f.flink_type}" for f in fields()]
    cols.append(f"  WATERMARK FOR created_at AS created_at - INTERVAL '{WATERMARK_DELAY}' HOUR")
    opts = {
        "connector": "kafka",
        "topic": topic,
        "properties.bootstrap.servers": "redpanda:9092",
        "properties.group.id": group,
        # A replay reads the whole topic; a live job resumes from its committed offsets.
        "scan.startup.mode": "earliest-offset" if bounded else "group-offsets",
        "properties.auto.offset.reset": "earliest",
        "scan.watermark.emit.strategy": "on-event",
        "scan.watermark.idle-timeout": "30 s",
        "format": "avro-confluent",
        "avro-confluent.url": "http://redpanda:8081",
    }
    if bounded:
        opts["scan.bounded.mode"] = "latest-offset"
    with_ = ",\n".join(f"  '{k}' = '{v}'" for k, v in opts.items())
    return "CREATE TEMPORARY TABLE events (\n" + ",\n".join(cols) + f"\n) WITH (\n{with_}\n);"


def _late_flag(r: Rollup, mode: Mode) -> str:
    if mode == "batch":
        return "FALSE"
    # Same test as the window operator's: a row is late once its window's last millisecond
    # is at or behind the watermark.
    end = f"FLOOR(created_at TO {_unit_sql(r)}) + INTERVAL '1' {_unit_sql(r)}"
    return f"COALESCE(TIMESTAMPADD(MILLISECOND, -1, {end}) <= CURRENT_WATERMARK(created_at), FALSE)"


def check_namespace(namespace: str) -> str:
    if not NAMESPACE.fullmatch(namespace):
        raise ValueError(f"namespace must match {NAMESPACE.pattern}: {namespace!r}")
    return namespace


def render_job(namespace: str, topic: str, mode: Mode, bounded: bool = True) -> str:
    """A bounded run (replay, backfill) rebuilds its tables; a live (unbounded) run keeps them
    and resumes from its consumer group, so a restart does not replay or lose history."""
    check_namespace(namespace)
    ns = f"ice.`{namespace}`"
    out = [
        f"SET 'execution.runtime-mode' = '{'streaming' if mode == 'stream' else 'batch'}';",
        "SET 'execution.checkpointing.interval' = '10 s';",
        # Bounded: wait for the job to finish. Live: submit and return (fold it periodically).
        f"SET 'table.dml-sync' = '{'true' if bounded else 'false'}';",
        "SET 'table.local-time-zone' = 'UTC';",
        f"SET 'pipeline.name' = 'keystone-rollup-{mode}-{namespace}';",
        CATALOG,
        f"CREATE DATABASE IF NOT EXISTS {ns};",
        source_ddl(topic, f"keystone-{namespace}-{mode}", bounded),
        f"CREATE TEMPORARY VIEW rollup_input AS\n{strip_comments(input_sql())};",
    ]
    inserts = []
    for r in rollups():
        keys = ", ".join(r.keys)
        unit = _unit_sql(r)
        out.append(f"CREATE TEMPORARY VIEW {r.name}_contrib AS\n{strip_comments(r.sql)};")
        out.append(
            f"CREATE TEMPORARY VIEW {r.name}_flagged AS SELECT *, {_late_flag(r, mode)} "
            f"AS is_late FROM {r.name}_contrib;"
        )
        out.append(
            f"CREATE TEMPORARY VIEW {r.name}_ontime_in AS "
            f"SELECT * FROM {r.name}_flagged WHERE NOT is_late;"
        )
        for suffix in ("ontime", "late"):
            if bounded:
                out.append(f"DROP TABLE IF EXISTS {ns}.{r.name}_{suffix};")
            out.append(_table_ddl(r, f"{ns}.{r.name}_{suffix}", if_not_exists=not bounded))
        aggs = ", ".join(_agg(r, m) for m in r.measures)
        inserts.append(
            f"INSERT INTO {ns}.{r.name}_ontime\n"
            f"SELECT CAST(window_start AS TIMESTAMP_LTZ(3)), {keys}, {aggs}\n"
            f"FROM TABLE(TUMBLE(TABLE {r.name}_ontime_in, DESCRIPTOR(created_at), "
            f"INTERVAL '1' {unit}))\n"
            f"GROUP BY window_start, window_end, {keys};"
        )
        casts = ", ".join(_measure_cast(r, m) for m in r.measures)
        inserts.append(
            f"INSERT INTO {ns}.{r.name}_late\n"
            f"SELECT CAST(FLOOR(created_at TO {unit}) AS TIMESTAMP_LTZ(3)), {keys}, {casts}\n"
            f"FROM {r.name}_flagged WHERE is_late;"
        )
    out.append("EXECUTE STATEMENT SET BEGIN\n" + "\n".join(inserts) + "\nEND;")
    return "\n\n".join(out) + "\n"


def render_fold(namespace: str) -> str:
    check_namespace(namespace)
    ns = f"ice.`{namespace}`"
    out = [
        "SET 'execution.runtime-mode' = 'batch';",
        "SET 'table.dml-sync' = 'true';",
        "SET 'table.local-time-zone' = 'UTC';",
        f"SET 'pipeline.name' = 'keystone-rollup-fold-{namespace}';",
        CATALOG,
    ]
    inserts = []
    for r in rollups():
        keys = ", ".join(r.keys)
        cols = ", ".join(r.columns)
        aggs = ", ".join(
            f"MAX({m}) AS {m}" if m in r.max_measures else f"CAST(SUM({m}) AS BIGINT) AS {m}"
            for m in r.measures
        )
        out.append(_table_ddl(r, f"{ns}.{r.name}", if_not_exists=True))
        inserts.append(
            f"INSERT OVERWRITE {ns}.{r.name}\n"
            f"SELECT window_start, {keys}, {aggs} FROM (\n"
            f"  SELECT {cols} FROM {ns}.{r.name}_ontime\n"
            f"  UNION ALL SELECT {cols} FROM {ns}.{r.name}_late\n"
            f") GROUP BY window_start, {keys};"
        )
    out.append("EXECUTE STATEMENT SET BEGIN\n" + "\n".join(inserts) + "\nEND;")
    return "\n\n".join(out) + "\n"


@dataclass
class JobResult:
    name: str
    seconds: float
    output: str


class FlinkError(RuntimeError):
    pass


def submit(sql: str, name: str) -> JobResult:
    """Write the job to the mounted jobs dir and run it with the SQL client (blocking)."""
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    path = JOBS_DIR / f"{name}.sql"
    path.write_text(sql, encoding="utf-8", newline="\n")
    cmd = [*COMPOSE, "exec", "-T", "jobmanager", "/opt/flink/bin/sql-client.sh", "-f"]
    started = time.perf_counter()
    proc = subprocess.run(
        [*cmd, f"/jobs/{path.name}"], capture_output=True, text=True, encoding="utf-8"
    )
    seconds = time.perf_counter() - started
    output = proc.stdout + proc.stderr
    if proc.returncode != 0 or "[ERROR]" in output:
        raise FlinkError(f"{name} failed (exit {proc.returncode}):\n{output[-4000:]}")
    return JobResult(name, seconds, output)


def job_path(name: str) -> Path:
    return JOBS_DIR / f"{name}.sql"
