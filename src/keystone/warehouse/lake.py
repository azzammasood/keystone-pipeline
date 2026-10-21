"""Iceberg side of the warehouse: publish dbt marts, and read the same tables from every engine.

`publish` copies each dbt model tagged `publish` (marts and metrics) from the DuckDB warehouse
into the Lakekeeper catalog (namespace = `<prefix><schema>`), written by DuckDB's Iceberg
extension. A table is dropped and re-created, so a reader can briefly miss it during a publish.

`verify` runs one fingerprint query per table in each engine and compares the results, all
exact integers: the row count, and per column the non-null count, plus the sum of an integer or
boolean column, or the distinct count of any other column and the summed character length of
a string column. A different row set or a different integer, boolean or string value makes the
engines disagree; a timestamp or date read with a constant shift (a time-zone slip) would not,
because only its distinct count is compared (there is no engine-neutral epoch function).
The SQL is the common subset of DuckDB, Spark, Flink and Trino (COUNT, SUM, CAST, CASE,
CHAR_LENGTH; Trino spells it LENGTH).
"""

import json
import re
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

import duckdb

from keystone.rollup.compare import iceberg_connection
from keystone.rollup.flink import CATALOG as FLINK_CATALOG
from keystone.rollup.flink import COMPOSE, JOBS_DIR

NAME = re.compile(r"[a-z][a-z0-9_]{0,62}")
# Published layers hold no person-level columns (CLAUDE.md privacy); dbt tests this too.
PERSON = re.compile(r"actor|login|email|author|owner", re.IGNORECASE)
INTEGER_TYPES = ("BIGINT", "INTEGER", "SMALLINT", "TINYINT", "HUGEINT")
STRING_TYPES = ("VARCHAR",)

Fingerprint = tuple[int | None, ...]


@dataclass(frozen=True)
class Table:
    namespace: str
    name: str

    @property
    def label(self) -> str:
        return f"{self.namespace}.{self.name}"


@dataclass(frozen=True)
class Published:
    table: Table
    rows: int
    schema: str = ""  # dbt schema of the source model
    columns: tuple[tuple[str, str], ...] = ()  # (name, DuckDB type)


def _check(name: str) -> str:
    if not NAME.fullmatch(name):
        raise ValueError(f"not a safe identifier: {name!r}")
    return name


def published_models(target_dir: Path) -> list[tuple[str, str]]:
    """(schema, model) of every model tagged `publish` in the dbt manifest."""
    manifest = json.loads((target_dir / "manifest.json").read_text("utf-8"))
    out = [
        (n["schema"], n["name"])
        for n in manifest["nodes"].values()
        if n["resource_type"] == "model" and "publish" in n.get("tags", [])
    ]
    return sorted(out)


def publish(warehouse_db: Path, target_dir: Path, prefix: str = "") -> list[Published]:
    con = iceberg_connection()
    con.execute(f"ATTACH '{warehouse_db.as_posix()}' AS wh (READ_ONLY)")
    out = []
    for schema, model in published_models(target_dir):
        ns = _check(prefix + schema)
        _check(model)
        described = con.execute(f"DESCRIBE wh.{schema}.{model}").fetchall()
        cols = [r[0] for r in described]
        bad = [c for c in cols if PERSON.search(c)]
        if bad:
            raise ValueError(f"{schema}.{model}: person-like columns {bad}; not published")
        con.execute(f'CREATE SCHEMA IF NOT EXISTS ice."{ns}"')
        con.execute(f'DROP TABLE IF EXISTS ice."{ns}".{model}')
        con.execute(f'CREATE TABLE ice."{ns}".{model} AS SELECT * FROM wh.{schema}.{model}')
        rows = con.execute(f'SELECT COUNT(*) FROM ice."{ns}".{model}').fetchone()
        out.append(
            Published(
                Table(ns, model),
                int(rows[0]) if rows else 0,
                schema,
                tuple((str(r[0]), str(r[1])) for r in described),
            )
        )
    con.close()
    return out


def tables_in(namespaces: Sequence[str]) -> list[Table]:
    """Every table in the given Iceberg namespaces, sorted."""
    con = iceberg_connection()
    try:
        out: list[Table] = []
        for ns in namespaces:
            rows = con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_catalog = 'ice' AND table_schema = ? ORDER BY 1",
                [_check(ns)],
            ).fetchall()
            if not rows:  # a publish that wrote nothing must not pass as "all match"
                raise ValueError(f"namespace {ns!r} has no tables")
            out.extend(Table(ns, r[0]) for r in rows)
        return out
    finally:
        con.close()


def fingerprint_sql(con: duckdb.DuckDBPyConnection, table: Table, label: str) -> str:
    """One-row fingerprint query, its first column a short label (Flink's table view cuts long
    strings). Columns are read from the Iceberg schema through DuckDB, so every engine gets the
    same query text."""
    ns, name = _check(table.namespace), _check(table.name)
    described = con.execute(f'DESCRIBE ice."{ns}".{name}').fetchall()
    parts = [f"'{_check(label)}' AS t", "COUNT(*) AS n"]
    for col, dtype, *_ in described:
        _check(col)
        parts.append(f"COUNT({col})")
        kind = str(dtype).upper()
        if kind in INTEGER_TYPES:
            parts.append(f"SUM(CAST({col} AS BIGINT))")
        elif kind == "BOOLEAN":
            parts.append(f"SUM(CASE WHEN {col} THEN 1 ELSE 0 END)")
        else:
            parts.append(f"COUNT(DISTINCT {col})")
            if kind in STRING_TYPES:
                parts.append(f"SUM(CAST(CHAR_LENGTH({col}) AS BIGINT))")
    return f"SELECT {', '.join(parts)} FROM ice.{ns}.{name}"


def _value(raw: str) -> int | None:
    raw = raw.strip()
    return None if raw in ("", "NULL", "null", "<NULL>") else int(raw)


def _rows(lines: Sequence[str], labels: set[str], sep: str) -> dict[str, Fingerprint]:
    out: dict[str, Fingerprint] = {}
    for line in lines:
        cells = [c.strip() for c in line.strip().strip("|").split(sep)]
        if cells and cells[0] in labels:
            out[cells[0]] = tuple(_value(c) for c in cells[1:])
    return out


def _run(cmd: list[str], timeout: int = 900) -> str:
    proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False)
    if proc.returncode != 0:
        tail = (proc.stdout + proc.stderr)[-1500:]
        raise RuntimeError(f"{' '.join(cmd[:6])} ... exit {proc.returncode}: {tail}")
    return proc.stdout


def read_duckdb(queries: dict[str, str]) -> dict[str, Fingerprint]:
    con = iceberg_connection()
    out = {}
    for label, sql in queries.items():
        got = con.execute(sql).fetchone()
        if got is not None:
            out[label] = tuple(None if v is None else int(v) for v in got[1:])
    con.close()
    return out


def read_spark(queries: dict[str, str]) -> dict[str, Fingerprint]:
    sql = ";\n".join(queries.values()) + ";"
    cmd = [*COMPOSE, "--profile", "spark", "run", "--rm", "-T", "spark"]
    stdout = _run([*cmd, "/opt/spark/bin/spark-sql", "-S", "-e", sql])
    return _rows(stdout.splitlines(), set(queries), "\t")


def read_flink(queries: dict[str, str]) -> dict[str, Fingerprint]:
    """Batch SQL through the SQL client of the running session cluster (`streaming` profile)."""
    script = "\n".join(
        [
            FLINK_CATALOG,
            "SET 'execution.runtime-mode' = 'batch';",
            "SET 'sql-client.execution.result-mode' = 'tableau';",
            *(q + ";" for q in queries.values()),
        ]
    )
    JOBS_DIR.mkdir(parents=True, exist_ok=True)
    (JOBS_DIR / "lake_verify.sql").write_text(script, "utf-8")
    cmd = [*COMPOSE, "exec", "-T", "jobmanager", "/opt/flink/bin/sql-client.sh"]
    stdout = _run([*cmd, "-f", "/jobs/lake_verify.sql"])
    return _rows(stdout.splitlines(), set(queries), "|")


def read_trino(queries: dict[str, str]) -> dict[str, Fingerprint]:
    """Through the CLI inside the running `trino` service (Tier 2, `trino` profile)."""
    sql = ";\n".join(q.replace("CHAR_LENGTH(", "LENGTH(") for q in queries.values()) + ";"
    cmd = [*COMPOSE, "--profile", "trino", "exec", "-T", "trino", "trino"]
    stdout = _run([*cmd, "--output-format", "TSV", "--execute", sql])
    return _rows(stdout.splitlines(), set(queries), "\t")


READERS: dict[str, Callable[[dict[str, str]], dict[str, Fingerprint]]] = {
    "duckdb": read_duckdb,
    "spark": read_spark,
    "flink": read_flink,
    "trino": read_trino,
}


@dataclass(frozen=True)
class EngineRead:
    engine: str
    table: str
    rows: int | None
    matches: bool


def verify(tables: Sequence[Table], engines: Sequence[str]) -> list[EngineRead]:
    """Fingerprint every table in every engine; the first engine is the reference."""
    con = iceberg_connection()
    labels = {f"q{i:03d}": t for i, t in enumerate(tables)}
    queries = {label: fingerprint_sql(con, t, label) for label, t in labels.items()}
    con.close()
    results = {engine: READERS[engine](queries) for engine in engines}
    reference = results[engines[0]]
    out = []
    for engine in engines:
        for label, table in labels.items():
            got = results[engine].get(label)
            out.append(
                EngineRead(
                    engine,
                    table.label,
                    got[0] if got else None,
                    got is not None and got == reference.get(label),
                )
            )
    return out
