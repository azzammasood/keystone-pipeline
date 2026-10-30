# Keystone pipeline

Keystone asks: **which open-source projects does the world quietly depend on, and which are fragile?**

It ingests GH Archive (the public GitHub event timeline), joins OSV vulnerability data and a
package-to-repository mapping, and computes a transparent, versioned, rule-based fragility and risk
signal per critical project. Signals are descriptive, never predictions about any project or person.
Repository and package level only: no person is published, ranked or labelled.

**Status: backend complete (Phases 0 to 10), except the backtest rerun, which waits on the history
backfill running on GitHub Actions.** Until the backfill lands, the radar reports "partial data" and
the backtests "pending data" rather than scores. The site comes later, in its own repository.
Details and the resume point: `docs/PROGRESS.md`; decisions: `docs/DECISIONS.md`.

Not affiliated with GitHub, GH Archive, OSV or any project maintainer.

## What runs

- **Production (`free` profile):** GitHub Actions calls the `keystone` CLI. Ingest runs every 6 hours
  and publish runs daily (DuckDB rollup, dbt warehouse, contracts, score, site exports, weekly brief).
  Before each job, `keystone profile check` probes the `full` stack. When it is down (always, on a
  runner), the job runs in `free`, records `produced_by` in every export, and keeps one issue open about
  it. First green publish: run 41027674711 (2026-10-30).
- **Cloud rungs at $0 (`cloud.yml`):** BigQuery sandbox (dry runs, `maximumBytesBilled` on every
  job, an in-place rollup and its export) and Databricks Free Edition (dbt on Unity Catalog, a
  Lakeflow Declarative Pipeline, Unity Catalog lineage), each on a manual dispatch.
- **Showcase (`full` profile, local Docker):** Redpanda + Flink to Iceberg (Lakekeeper, RustFS), read
  back by Spark and DuckDB. Airflow runs the same CLI as a DAG.
- **Exports:** radar, project pages, backtests, benchmarks, the weekly brief, `pipeline_metrics.json` and
  the metadata export. Each one is a versioned JSON document validated by a JSON Schema in `contracts/`.
  Publish uploads them to the pre-release `site-data`.

## Measured (sources in `docs/`)

| What | Value |
| --- | --- |
| Sample hours parsed, every schema era (2011 to 2026) | 152 hours, 2,363,426 events, 0 quarantined, 0 drift |
| One day (2026-10-19) | 1,042,693 events; DuckDB rollup 2.7 s; Flink stream and batch match it row for row |
| Rollup, 1 day, three engines (wall s, run 1) | DuckDB 4.7, Spark 64.9, Flink 92.5 (`docs/benchmarks.md`) |
| Package-to-repository match rate | 94.1% (4,456 of 4,736 critical PyPI and npm packages) |
| Lineage graph | 103 nodes, 118 edges, 294 column edges |
| Docs coverage of the warehouse | 100% (gate 95%) |
| Simulated full outage | 5 of 5 probes down; free rollup; all site exports validate (`produced_by: free, fallback: true`) |
| Backfill (GitHub Actions, 78 chunks) | 4,574,059,729 events, 1,873.2 GB downloaded, 37.88 job hours |
| Live score, cut 2026-10-01 | 4,421 of 7,792 mapped repositories scored; 100.0% of the window's hours |
| Backtests (event-stream, colors-faker, xz) | no incident flagged: two misses, one not scored (`docs/backtest.md`) |
| BigQuery: rollup columns vs payload, one day | 29.1 MB vs 588.9 MB dry run; 29.4 MB billed per run |
| Databricks: `dbt build --target databricks` | 123 nodes, 94 tests passed; Unity Catalog lineage 104 edges |
| Cost | $0 (no billing account exists for any service used) |

## Tools (each one ran; evidence in `engines[]` of `pipeline_metrics.json`)

DuckDB 1.5.6 (rollup, Iceberg writes), dbt 1.12.6, datacontract-cli 1.2.7, Redpanda v26.2.5,
Flink 2.2.1, Spark 4.1.3, OpenLineage (dbt-ol 1.54.0, Flink, Airflow provider), Marquez 0.51.1,
DataHub v1.7.0.1, OpenTelemetry Collector 0.163.1, Prometheus 3.15.0, Grafana 13.2.4,
Airflow 3.3.2, a free OpenRouter model (resolution ties and the brief, at $0).
Cloud (fixtures, as in CI): BigQuery sandbox, Databricks SQL on Unity Catalog, dbt-databricks 1.10.9,
a Lakeflow Declarative Pipeline.
Supply chain: Scorecard, zizmor, gitleaks, pip-audit, Syft SBOM, actions pinned by SHA,
dependencies locked by `uv.lock`. Build attestations are switched on for `v*` releases; none has
been cut yet.

## Develop

```
uv sync
uv run pre-commit install
uv run keystone --help
uv run ruff check . ; uv run mypy ; uv run pytest
```

Corrections: open an issue. License: MIT.
