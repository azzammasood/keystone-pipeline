"""Parity: the DuckDB rollup tables against the Flink-written Iceberg tables, row for row.

DuckDB reads Iceberg through the Lakekeeper REST catalog and the S3 store (both on localhost),
so the comparison runs in one engine over both results: EXCEPT ALL in each direction (bag
semantics, NULLs compare equal) must be empty for every rollup.
"""

import os
from dataclasses import dataclass
from pathlib import Path

import duckdb
import httpx

from keystone.rollup.flink import check_namespace
from keystone.rollup.spec import rollups

CATALOG_URL = "http://127.0.0.1:8181/catalog"
S3_ENDPOINT = "127.0.0.1:9000"


def _lit(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


@dataclass(frozen=True)
class TableParity:
    table: str
    free_rows: int
    full_rows: int
    only_free: int
    only_full: int

    @property
    def ok(self) -> bool:
        return self.only_free == 0 and self.only_full == 0 and self.free_rows == self.full_rows


def iceberg_connection(con: duckdb.DuckDBPyConnection | None = None) -> duckdb.DuckDBPyConnection:
    con = con or duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("SET enable_progress_bar = false")
    for ext in ("httpfs", "iceberg"):
        con.execute(f"INSTALL {ext}; LOAD {ext};")
    key = os.environ.get("KEYSTONE_S3_ACCESS_KEY", "keystone-local")
    secret = os.environ.get("KEYSTONE_S3_SECRET_KEY", "keystone-local-only")
    con.execute(
        f"""CREATE OR REPLACE SECRET keystone_s3 (TYPE s3, KEY_ID {_lit(key)},
        SECRET {_lit(secret)}, ENDPOINT '{S3_ENDPOINT}', URL_STYLE 'path', USE_SSL false,
        REGION 'local-01')"""
    )
    con.execute(
        f"""ATTACH OR REPLACE 'keystone' AS ice (TYPE iceberg, ENDPOINT '{CATALOG_URL}',
        AUTHORIZATION_TYPE 'none', ACCESS_DELEGATION_MODE 'none')"""
    )
    return con


def _count(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = con.execute(sql).fetchone()
    return int(row[0]) if row else 0


def compare(free_db: Path, namespace: str, table_suffix: str = "") -> list[TableParity]:
    check_namespace(namespace)
    con = iceberg_connection()
    con.execute(f"ATTACH {_lit(free_db.as_posix())} AS free (READ_ONLY)")
    out = []
    for r in rollups():
        cols = ", ".join(r.columns)
        free = f"SELECT {cols} FROM free.main.{r.name}"
        full = f'SELECT {cols} FROM ice."{namespace}".{r.name}{table_suffix}'
        out.append(
            TableParity(
                r.name,
                _count(con, f"SELECT count(*) FROM ({free})"),
                _count(con, f"SELECT count(*) FROM ({full})"),
                _count(con, f"SELECT count(*) FROM ({free} EXCEPT ALL {full})"),
                _count(con, f"SELECT count(*) FROM ({full} EXCEPT ALL {free})"),
            )
        )
    con.close()
    return out


def late_rows(namespace: str) -> dict[str, int]:
    """Rows the stream job routed to `<rollup>_late` (late events corrected by the fold)."""
    check_namespace(namespace)
    con = iceberg_connection()
    out = {
        r.name: _count(con, f'SELECT count(*) FROM ice."{namespace}".{r.name}_late')
        for r in rollups()
    }
    con.close()
    return out


def contributed_events(namespace: str) -> dict[str, int]:
    """sum(events) over `_ontime` plus `_late`: every input row lands in exactly one of them."""
    check_namespace(namespace)
    con = iceberg_connection()
    out = {}
    for r in rollups():
        ns = f'ice."{namespace}".{r.name}'
        out[r.name] = _count(
            con,
            f"SELECT (SELECT coalesce(sum(events), 0) FROM {ns}_ontime)"
            f" + (SELECT coalesce(sum(events), 0) FROM {ns}_late)",
        )
    con.close()
    return out


def drop_namespace(namespace: str, base: str = "http://127.0.0.1:8181/catalog/v1") -> None:
    """Purge a run's tables and namespace through the REST catalog (test and replay cleanup)."""
    check_namespace(namespace)
    with httpx.Client(timeout=30) as http:
        cfg = http.get(f"{base}/config", params={"warehouse": "keystone"}).raise_for_status()
        body = cfg.json()
        prefix = {**body.get("defaults", {}), **body.get("overrides", {})}["prefix"]
        root = f"{base}/{prefix}/namespaces/{namespace}"
        tables = http.get(f"{root}/tables")
        if tables.status_code == 404:
            return
        for ident in tables.raise_for_status().json().get("identifiers", []):
            http.delete(
                f"{root}/tables/{ident['name']}", params={"purgeRequested": "true"}
            ).raise_for_status()
        http.delete(root).raise_for_status()
