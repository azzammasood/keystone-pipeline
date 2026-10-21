"""Column lineage of the rollup SQL, derived by sqlglot from the SQL the engines run.

Each rollup is the shared input filter over silver events plus one contribution query
(ADR-024); sqlglot traces every output column back to the silver event columns it reads,
with the silver schema supplied for `SELECT *`. A column computed from a constant (`events`
counts rows) has no source column and gets no edge.
"""

from functools import cache

from keystone.rollup.spec import Rollup, duckdb_select, input_sql

SOURCE = "silver_events"


@cache
def rollup_columns(r: Rollup) -> dict[str, list[str]]:
    """Output column -> silver event columns it derives from."""
    from sqlglot.lineage import lineage

    from keystone.parsers.parquet import EVENT_SCHEMA

    schema = {SOURCE: {f.name: "VARCHAR" for f in EVENT_SCHEMA}}
    sql = (
        f"WITH events AS (SELECT * FROM {SOURCE}), rollup_input AS ({input_sql()}) "
        f"{duckdb_select(r)}"
    )
    out: dict[str, list[str]] = {}
    for col in r.columns:
        node = lineage(col, sql, schema=schema, dialect="duckdb")
        leaves = {n.name for n in node.walk() if not n.downstream}
        sources = sorted(x.split(".", 1)[1] for x in leaves if x.startswith(SOURCE + "."))
        if sources:
            out[col] = sources
    return out
