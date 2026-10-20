"""The Avro event contract (`contracts/avro/gharchive_event.avsc`): the columns every rollup
engine reads, in one place. DuckDB projects silver Parquet to it; the producer serialises it;
the Flink source table is derived from it."""

import json
from dataclasses import dataclass
from functools import cache
from typing import Any

from keystone.paths import CONTRACTS_DIR

AVRO_SCHEMA_PATH = CONTRACTS_DIR / "avro" / "gharchive_event.avsc"

_FLINK = {"string": "STRING", "long": "BIGINT", "int": "INT", "boolean": "BOOLEAN"}


@dataclass(frozen=True)
class Field:
    name: str
    base: str  # avro primitive
    logical: str | None
    nullable: bool

    @property
    def is_timestamp(self) -> bool:
        return self.logical == "timestamp-millis"

    @property
    def flink_type(self) -> str:
        t = "TIMESTAMP(3)" if self.is_timestamp else _FLINK[self.base]
        return t if self.nullable else f"{t} NOT NULL"


def _field(raw: dict[str, Any]) -> Field:
    t = raw["type"]
    nullable = isinstance(t, list)
    if nullable:
        (t,) = [x for x in t if x != "null"]
    if isinstance(t, dict):
        return Field(raw["name"], t["type"], t.get("logicalType"), nullable)
    return Field(raw["name"], t, None, nullable)


@cache
def schema_text() -> str:
    return AVRO_SCHEMA_PATH.read_text(encoding="utf-8")


@cache
def fields() -> tuple[Field, ...]:
    return tuple(_field(f) for f in json.loads(schema_text())["fields"])


def duckdb_projection() -> str:
    """Select list over silver Parquet: timestamps cut to milliseconds, as the Avro contract."""
    cols = [
        f"date_trunc('millisecond', {f.name}) AS {f.name}" if f.is_timestamp else f.name
        for f in fields()
    ]
    return ", ".join(cols)
