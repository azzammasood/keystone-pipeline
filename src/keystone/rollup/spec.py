"""Rollup logic, written once (Phase 4, ADR-024).

Each `sql/<name>.sql` is a portable, ANSI-only *contribution* query: one row per input event with
the event time (`created_at`), the group keys and one column per measure. A header declares the
tumbling window, the keys and which measures take MAX (all others are SUM). The engines only
wrap it: DuckDB truncates `created_at` to the window and groups; Flink feeds it to a TUMBLE
window TVF. Neither engine has its own copy of a measure, so parity is a property of the wrappers.
"""

import re
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Literal

SQL_DIR = Path(__file__).parent / "sql"
Unit = Literal["hour", "day"]

_HEADER = re.compile(r"^--\s*(keystone-rollup|window|keys|max):\s*(.+?)\s*$", re.MULTILINE)
_ALIAS = re.compile(r"\bAS\s+([a-z_][a-z0-9_]*)\s*,?\s*$", re.IGNORECASE)


@dataclass(frozen=True)
class Rollup:
    name: str
    unit: Unit
    keys: tuple[str, ...]
    measures: tuple[str, ...]
    max_measures: frozenset[str]
    sql: str  # the contribution query; reads `rollup_input`

    def agg(self, measure: str) -> str:
        return "MAX" if measure in self.max_measures else "SUM"

    @property
    def columns(self) -> tuple[str, ...]:
        """Output columns of the rollup table, in order."""
        return ("window_start", *self.keys, *self.measures)


def strip_comments(sql: str) -> str:
    return "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("--"))


def _measures(sql: str, keys: tuple[str, ...]) -> tuple[str, ...]:
    """Aliases of the select list after the keys (each measure ends in `AS name`)."""
    body = strip_comments(sql)
    select = body[body.upper().index("SELECT") + 6 : body.upper().index("FROM ROLLUP_INPUT")]
    names = [m.group(1) for line in select.splitlines() if (m := _ALIAS.search(line))]
    plain = [c.strip().rstrip(",") for c in select.splitlines() if c.strip().rstrip(",") in keys]
    if tuple(plain) != keys:
        raise ValueError(f"select list must start with created_at and the keys {keys}")
    return tuple(names)


def parse_rollup(path: Path) -> Rollup:
    text = path.read_text(encoding="utf-8")
    header = {k: v for k, v in _HEADER.findall(text)}
    count, unit = header["window"].split()
    if count != "1" or unit not in ("hour", "day"):
        raise ValueError(f"{path.name}: windows are `1 hour` or `1 day`, got {header['window']}")
    keys = tuple(k.strip() for k in header["keys"].split(","))
    measures = _measures(text, keys)
    max_measures = frozenset(m.strip() for m in header.get("max", "").split(",") if m.strip())
    if not max_measures <= set(measures):
        raise ValueError(f"{path.name}: max measures not in the select list")
    return Rollup(header["keystone-rollup"], unit, keys, measures, max_measures, text)


@cache
def rollups() -> tuple[Rollup, ...]:
    return tuple(parse_rollup(p) for p in sorted(SQL_DIR.glob("*.sql")) if p.stem != "input")


@cache
def input_sql() -> str:
    return (SQL_DIR / "input.sql").read_text(encoding="utf-8")


def _agg_sql(r: Rollup, m: str) -> str:
    """Counts are BIGINT in every engine (DuckDB widens SUM to HUGEINT, Flink keeps INT)."""
    return f"MAX({m}) AS {m}" if m in r.max_measures else f"CAST(SUM({m}) AS BIGINT) AS {m}"


def duckdb_select(r: Rollup, where: str = "") -> str:
    """The rollup as one DuckDB query over the `rollup_input` view (TimeZone must be UTC)."""
    measures = ",\n    ".join(_agg_sql(r, m) for m in r.measures)
    keys = ", ".join(r.keys)
    return (
        f"SELECT date_trunc('{r.unit}', created_at) AS window_start, {keys},\n    {measures}\n"
        f"FROM (\n{r.sql}\n) AS contrib\n{where}\nGROUP BY window_start, {keys}"
    )
