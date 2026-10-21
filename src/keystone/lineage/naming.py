"""Dataset identifiers, one scheme for every tool (OpenLineage naming conventions).

Paths and hosts differ between machines (a laptop, a CI runner, a container), so local
datasets get stable logical namespaces instead of absolute paths:

| What | namespace | name |
| --- | --- | --- |
| GH Archive hourly file | `https://data.gharchive.org` | `/2026-10-19-12.json.gz` |
| OSV ecosystem zip | `https://osv-vulnerabilities.storage.googleapis.com` | `/PyPI/all.zip` |
| silver / quarantine Parquet | `file://keystone` | `silver/gharchive/events` |
| DuckDB tables (rollup, dbt) | `duckdb://keystone` | `rollup.main.repo_hour`, `keystone.marts.x` |
| Kafka topic | `kafka://redpanda:9092` | topic |
| Iceberg table (Lakekeeper) | `iceberg://lakekeeper/keystone` | `marts.fct_repo_day` |

`canonical` maps identifiers written by other tools (dbt-ol, Spark, Flink) onto this scheme;
the rewrite is recorded in the event (`keystone_canonical` facet), never hidden.
"""

import re

GHARCHIVE_NS = "https://data.gharchive.org"
OSV_NS = "https://osv-vulnerabilities.storage.googleapis.com"
FILE_NS = "file://keystone"
DUCKDB_NS = "duckdb://keystone"
KAFKA_NS = "kafka://redpanda:9092"
ICEBERG_NS = "iceberg://lakekeeper/keystone"
JOB_NS = "keystone"

# dbt source -> silver directory (dbt/models/staging/_sources.yml; tests/test_lineage.py checks).
SILVER_SOURCES = {
    "gharchive_events": "silver/gharchive/events",
    "osv_vulns": "silver/osv/vulns",
    "osv_affected": "silver/osv/affected",
    "osv_repo_refs": "silver/osv/repo_refs",
}

_DUCKDB_FILE = re.compile(r"^duckdb://.*keystone\.duckdb$")
_ICEBERG_LOCATION = re.compile(r"^s3a?://keystone(/.*)?$")


def hour_file(stem: str) -> tuple[str, str]:
    return GHARCHIVE_NS, f"/{stem}.json.gz"


def silver(rel: str) -> tuple[str, str]:
    return FILE_NS, rel


def duck(name: str) -> tuple[str, str]:
    return DUCKDB_NS, name


def topic(name: str) -> tuple[str, str]:
    return KAFKA_NS, name


def iceberg(table: str) -> tuple[str, str]:
    return ICEBERG_NS, table


def canonical(namespace: str, name: str) -> tuple[str, str]:
    """Map another tool's identifier onto the Keystone scheme (unchanged when unknown)."""
    if _DUCKDB_FILE.match(namespace):
        parts = name.split(".")
        if len(parts) == 3 and parts[1] == "silver" and parts[2] in SILVER_SOURCES:
            return FILE_NS, SILVER_SOURCES[parts[2]]
        return DUCKDB_NS, name
    if namespace.startswith("kafka://"):
        return KAFKA_NS, name
    if namespace == "" and name.startswith("ice."):
        # openlineage-flink names an Iceberg table `<catalog>.<namespace>.<table>` with an empty
        # namespace; `ice` is the Lakekeeper catalog in the Flink and Spark configs.
        return ICEBERG_NS, name.removeprefix("ice.")
    if _ICEBERG_LOCATION.match(namespace):
        # Iceberg data location `s3://keystone/<warehouse-id>/<ns-id>/<table-id>` carries ids,
        # not names; the table name comes from the symlink (see graph.py).
        return namespace, name
    return namespace, name
