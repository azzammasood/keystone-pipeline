"""The datasets each CLI stage reads and writes, as OpenLineage datasets (naming.py).

Column lineage is emitted where the CLI knows it exactly: the Kafka topic is a projection of
silver events onto the Avro contract (same names, timestamps cut to milliseconds), and the
Iceberg publish copies dbt models column for column. The parser (Python projection of JSON
paths) and the rollup SQL are table level here; DataHub derives the rollup's column lineage
from the SQL (lineage/datahub.py).
"""

import pyarrow as pa

from keystone.lineage import naming
from keystone.lineage.emit import Ds


def _cols(schema: pa.Schema) -> list[tuple[str, str]]:
    return [(f.name, str(f.type)) for f in schema]


def hour_files(stems: list[str]) -> list[Ds]:
    return [Ds(*naming.hour_file(s)) for s in stems]


def silver_gharchive() -> list[Ds]:
    from keystone.parsers.parquet import EVENT_SCHEMA, HOUR_TYPE_SCHEMA, QUARANTINE_SCHEMA

    return [
        Ds(*naming.silver("silver/gharchive/events"), columns=_cols(EVENT_SCHEMA)),
        Ds(*naming.silver("silver/gharchive/hour_types"), columns=_cols(HOUR_TYPE_SCHEMA)),
        Ds(*naming.silver("quarantine/gharchive"), columns=_cols(QUARANTINE_SCHEMA)),
    ]


def osv_zip(name: str) -> Ds:
    return Ds(naming.OSV_NS, f"/{name}/all.zip")


def silver_osv() -> list[Ds]:
    from keystone.parsers.parquet import OSV_QUARANTINE_SCHEMA, OSV_SCHEMAS

    out = [Ds(*naming.silver(f"silver/osv/{t}"), columns=_cols(s)) for t, s in OSV_SCHEMAS.items()]
    out.append(Ds(*naming.silver("quarantine/osv"), columns=_cols(OSV_QUARANTINE_SCHEMA)))
    return out


def events_input() -> Ds:
    return silver_gharchive()[0]


def rollup_tables() -> list[Ds]:
    from keystone.rollup.spec import rollups

    return [
        Ds(*naming.duck(f"rollup.main.{r.name}"), columns=[(c, "") for c in r.columns])
        for r in rollups()
    ]


def topic(name: str) -> Ds:
    from keystone.rollup.contract import fields

    src = events_input()
    cols = [(f.name, f.base if f.logical is None else f.logical) for f in fields()]
    return Ds(
        *naming.topic(name),
        columns=cols,
        lineage={c: [(src.namespace, src.name, c)] for c, _ in cols},
    )


def flink_outputs(namespace: str) -> list[Ds]:
    """The fold writes `<rollup>`; the job writes `_ontime` and `_late` (ADR-026)."""
    from keystone.rollup.spec import rollups

    out = []
    for r in rollups():
        for suffix in ("", "_ontime", "_late"):
            out.append(
                Ds(
                    *naming.iceberg(f"{namespace}.{r.name}{suffix}"),
                    columns=[(c, "") for c in r.columns],
                )
            )
    return out


def published(schema: str, model: str, columns: list[tuple[str, str]], namespace: str) -> Ds:
    src = naming.duck(f"keystone.{schema}.{model}")
    return Ds(
        *naming.iceberg(f"{namespace}.{model}"),
        columns=columns,
        lineage={c: [(*src, c)] for c, _ in columns},
        identity=True,
    )


def warehouse_model(schema: str, model: str, columns: list[tuple[str, str]]) -> Ds:
    return Ds(*naming.duck(f"keystone.{schema}.{model}"), columns=columns)
