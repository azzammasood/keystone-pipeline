# Decisions (ADRs)

Newest first. Each entry: context, options, choice, why, date. Scope changes are recorded only here.

## ADR-035 (2026-10-21) Telemetry: stage run log, OpenTelemetry, pipeline_metrics.json
- Every data command is a measured `Stage` (telemetry/runlog.py): wall-clock seconds, records
  and bytes it counted (null when it cannot count), status, one line in
  `<root>/telemetry/runs.jsonl`. A pipeline run is the set of stages sharing `KEYSTONE_RUN_ID`
  (an orchestrator sets it; else one id per process). Rate = records out / the stage's seconds.
  Phase 4's `rollup/runs.jsonl` is replaced by this log.
- `keystone metrics emit` builds `pipeline_metrics.json` (schema:
  `contracts/pipeline_metrics.schema.json`, JSON Schema 2020-12, validated on every emit) from
  the log, the data (silver and rollup counts, newest event, quarantine rows) and the metadata
  export. A value nothing measured is null, never 0 (`bytes_downloaded` before any ingest,
  `checks_passed` before any check; the Flink stage's record counts, which the SQL client does
  not report). A run's status and `last_run.stages` use the last attempt of each stage.
  Definitions: `events_processed` sums parse work (a re-parsed hour counts again);
  `events_stored` counts silver rows; an engine is `active` when it ran in the last run;
  `cost.usd` is 0 because no service used has a billing account; the one free quota measured
  so far is the disk budget (GitHub Actions minutes are listed with null until the production
  workflow can read them). Timestamps are UTC (DuckDB hands TIMESTAMPTZ back in the local
  zone; converted). Nothing is estimated.
- OpenTelemetry (`obs` profile): each stage records gauges (`keystone_stage_*`, labels stage
  and service) via OTLP/HTTP to the collector 0.163.1, which exposes them to Prometheus 3.15.0;
  Grafana 13.2.4 provisions the `keystone-pipeline` dashboard. The resource has a fixed
  `service.instance.id` (the SDK would start a new series per process, and `max` over stale
  series showed old values; found in the first gate check), and resource attributes are not
  copied into labels, so a version bump keeps the same series (review); series key on stage
  and service. `metrics emit` also exports the
  pipeline totals as gauges.
- Gate check (`keystone metrics check-grafana`): reads the dashboard from Grafana's API, runs
  each panel's query through Grafana's `/api/ds/query`, and compares it with the JSON path in
  the panel description (stage panels per stage of `last_run`); tolerance 0.001 (the JSON
  rounds rates and durations to 3 decimals). The stage gauges are written by each stage as it
  ends, independently of the JSON, which is built later from the run log. Order: emit, then
  check (a check is itself a stage, so a check before a new emit compares older JSON).
- Measured 2026-10-21 (final gate run on the local data, after the review): 50 of 50 values
  equal. 42 are stage values (14 stages x records out, duration, rate), written by each stage
  independently of the JSON; the other 8 are pipeline totals exported from the JSON itself, so
  they check only the transport (OTLP, collector, Prometheus, Grafana). 15 engines with
  versions read from the running service or its pinned tag. Stage numbers are in
  PROGRESS.md. RAM: collector 31 MB, Prometheus 42 MB, Grafana 296 to 323 MB (limit 512 MB).

## ADR-034 (2026-10-21) Catalog metadata, generated contracts, static export, completeness
- One source: `config/metadata.yml` (owner group, domains, classifications, freshness SLAs,
  glossary, column docs, datasets outside dbt) and dbt `meta` per folder and model (owner,
  domain, layer, classification, SLA). The owner is the maintainers group, never a person.
- Contracts: hand-written ODCS contracts stay in `contracts/` (silver GH Archive). The others
  are generated into `metadata/contracts/` (`keystone metadata contracts`): OSV silver (Arrow
  schemas), the free rollup tables (DuckDB) and every dbt model and seed (types from
  `catalog.json`, descriptions from dbt YAML, `required` from not_null tests).
  datacontract-cli 1.2.7 tests them, DuckDB-file contracts with its `duckdb` server type. CI
  regenerates them (`--check`) and fails on any difference, so a schema or description change
  needs a reviewed contract change. Exception: `stg_rollup__repo_hour` is a view over the
  attached rollup database, which datacontract-cli cannot attach; dbt build checks it.
- Static export (`keystone metadata export`): `metadata/catalog.json` (44 datasets with owner,
  description, domain, layer, SLA, contract, classification, columns and glossary terms),
  `glossary.json` (the six terms and where they are attached), `lineage.json` (ADR-031) and
  `contracts/` (all contracts). Completeness gate: models and columns documented >= 95% each,
  every dataset with every catalog field, no owner missing; measured 19/19 models, 483/483
  columns, 44/44 datasets. Published tables are classified public (tested).
- "Published export field" (gate wording) is, until the site export exists (Phase 8), a
  column of a table in the published Iceberg namespaces (`marts`, `metrics`); the site export
  will add one hop to the same graph.

## ADR-033 (2026-10-21) What Spark and Flink report to OpenLineage (measured)
- openlineage-flink 1.54.0 (job-status listener, FLIP-314, jar pinned by SHA-256) works with
  Flink 2.2.1: for the batch job it reported the Kafka topic as input and the four Iceberg
  `_ontime`/`_late` tables as outputs, and the fold job's inputs and outputs, all table level,
  with schemas, no column lineage. It emitted START events only (no COMPLETE) in our batch
  runs, and named Iceberg tables `ice.<namespace>.<table>` with an empty namespace;
  `naming.canonical` maps them, and the graph takes edges from START events too (run counts
  use COMPLETE only).
- openlineage-spark 1.54.0 (listener in spark-defaults.conf) ran in every Spark read but, for
  the read-only fingerprint queries, emitted only application START/COMPLETE without datasets.
  Spark is therefore listed as a lakehouse reader that ran, not as a lineage source.
- Both write to the mounted `data/lineage/incoming/` (file transport); `keystone lineage
  collect` canonicalises the events into the log. The parity workflow checks that Flink wrote
  lineage on every run.

## ADR-032 (2026-10-21) DataHub: lightest viable setup, sources plus Keystone's own aspects
- Setup (`datahub` profile): DataHub v1.7.0.1 GMS (consumers embedded, no frontend), the
  system-update job, OpenSearch 2.19.3 (512 MB heap) and Postgres 17.6 as the store; the
  message bus is the Redpanda broker already in the stack. DataHub's Kafka-free `pgqueue`
  transport was tried first and failed: it needs the `pg_partman` extension, absent from the
  stock Postgres image. Measured: GMS 1.22 to 1.58 GB (limit 1.75 GB), OpenSearch 0.93 GB,
  Postgres 40 MB; images about 3 GB compressed, so free disk fell from 15 GB to 9.4 GB. It runs
  with the Flink containers stopped.
- Sources (acryl-datahub 1.7, `metadata` group): `dbt` (451 events: models, sources, column
  lineage from the compiled SQL, 57 assertions from the tests and their last results),
  `iceberg` (Lakekeeper REST, 261 events) and `kafka` (the topic and its registry schema).
- Keystone's own aspects (lineage/datahub.py): 5 domains, the owner group, the glossary (node
  plus 6 terms), and per dataset ownership, domain, tags (layer, classification, freshness
  SLA), description, a link to its contract and column glossary terms; every non-dbt job of
  the lineage graph (CLI and Flink) as a DataJob with inputs, outputs and column lineage
  (including the rollup's, ADR-031). DataJobs of the flow that the current graph no longer has
  are soft-deleted (the first emit left a combined `warehouse.publish` job; review).
- Check: every dataset of the gate path is upstream of the published table in DataHub's
  lineage search (dbt nodes count by either sibling, dbt or duckdb; indexing is asynchronous,
  so the check retries for up to 2 minutes), and every hop of the gate's column chain has a
  fine-grained lineage edge stored in DataHub (read from the UpstreamLineage and
  DataJobInputOutput aspects; lineage search on a schemaField returns datasets only). Databricks and Unity Catalog lineage: skipped, no
  token (ADR-030).

## ADR-031 (2026-10-21) OpenLineage from the CLI and dbt; one naming scheme; Marquez
- The CLI emits OpenLineage 2-0-2 events with openlineage-python 1.54 (START at a stage's
  start, COMPLETE or FAIL at its end, a parent facet pointing at the pipeline run) to
  `<root>/lineage/events/YYYY-MM-DD.jsonl`, and posts them to Marquez when `OPENLINEAGE_URL` is
  set (`keystone lineage push` replays the log; Marquez de-duplicates). Column lineage is
  emitted where the CLI knows it: the topic (projection of silver events), the Iceberg publish
  (identity copies) and the rollups, derived from their SQL by sqlglot 30.12
  (`lineage/sqllineage.py`: 13 columns of repo_hour and 15 of repo_actor_day trace to silver
  columns; `events` counts rows and has no source column). The parse hop (hourly file to
  silver) is table level: the file has no schema to name fields against. The publish is one job per table, so no table seems to
  feed another (the first version listed all tables in one event; caught in the gate run).
- Naming (lineage/naming.py): stable logical namespaces instead of machine paths
  (`file://keystone`, `duckdb://keystone`, `kafka://redpanda:9092`,
  `iceberg://lakekeeper/keystone`, the GH Archive and OSV URLs). Other tools' identifiers are
  mapped onto it and the rewrite is counted in a `keystone_canonical` run facet.
- dbt: `dbt-ol send-events` (openlineage-dbt 1.54) over the build's artifacts (the build's
  run_results.json is restored after `docs generate`). openlineage-sql has no Windows wheel,
  so on Windows it runs in a pinned uv container (the `lineage` group of the same lock); on
  Linux (CI) natively. Measured: 76 events, column lineage on 17 models; 61 column inputs name
  CTEs or `read_parquet` and are dropped and counted, not guessed.
- Graph and gate path (`lineage.json`): datasets, jobs, fields; the gate path from an hourly
  file to a published table (table level) and a published field whose column chain follows
  that path dataset by dataset; `--require-path` requires the chain to reach the table the
  hourly file is parsed into (`complete`; the first version passed with a chain ending in
  staging, review). Measured: `metrics.metric_maint_v1_repo_day.activity_date` back through
  the metric, `fct_repo_day`, `stg_rollup__repo_hour`, `rollup.main.repo_hour` to silver
  `created_at`. The full-engine branch (topic, Flink job, Iceberg rollup) is beside it.
- Marquez 0.51.1 (`lineage` profile, Postgres 17.6, search off, no web UI): the gate path's 13
  nodes and the full-engine path are present in its lineage API (job names compared by
  `simpleName`, since Marquez prefixes child jobs with the parent's name), and the 6 fields of
  the column chain in its column-lineage API.
- Who could do what (table / column): CLI yes / where exact; dbt-ol yes / yes (parsed SQL);
  DataHub dbt source yes / yes; sqlglot on the rollup SQL - / yes; Flink yes / no;
  Spark ran, no datasets for reads; Airflow not run (deferred to Phase 10, when DAGs exist:
  the provider adds nothing until a DAG calls the CLI); Databricks not run (no token).
- Retention: past days are gzipped (appended as another gzip member when the day's archive
  exists, so a late event never replaces it) and capped (`keystone lineage compact`, default 50 MB); the
  oldest days beyond the cap are summarised into `lineage/summary.jsonl` and removed. The push
  of the compressed log to a data branch belongs to the production workflow and is deferred
  to the live refresh (Phase 10).

## ADR-030 (2026-10-20) Phase 5 rungs: Databricks skipped, Trino done (Tier 2)
- Databricks (dbt-databricks, Unity Catalog, a Lakeflow Declarative Pipeline for marts) needs a
  workspace token, which does not exist yet: skipped and logged in PROGRESS.md. The dbt SQL is
  written for both (ANSI casts, CASE, window functions, `DATE_TRUNC('hour', ...)`, `MAX_BY`,
  all in DuckDB and Databricks SQL); sources, the profile and the seed column type (switched on
  `target.type`) are the only per-target parts. Not yet run on Databricks. "`dbt build`
  passes on every available target" therefore means the `duckdb` target today.
- Trino ran within the RAM budget (container limit 1.5 GB, 0.91 GB idle, 1.45 GB peak during the
  fingerprint queries), so the Tier 2 rung is in: `trino` profile, Iceberg REST connector,
  native S3. The default per-node query memory (357 MB) failed on the distinct counts of the
  800k-row tables; `compose/trino/config.properties` gives queries 768 MB within the same limit.

## ADR-029 (2026-10-20) Lakehouse reads: DuckDB writes the marts, four engines read them
- Publish: `keystone warehouse publish` copies every dbt model tagged `publish` (schemas `marts`
  and `metrics`) into Iceberg through DuckDB's Iceberg extension (REST catalog, CTAS); the
  namespace is the schema, with a prefix for test runs (`ci_`). Drop and re-create, so a reader
  can miss a table for the seconds of a publish (12 s for the nine tables); an atomic swap
  (branch or `replace` commit) is a Phase 10 item. Options: dbt-duckdb materialising into the
  attached catalog (not tried: dbt's table materialisation builds a temporary relation and
  renames it, and the explicit copy keeps the Iceberg write in one place), Spark or Flink as
  the writer (rejected: a JVM in the free path).
- Readers: DuckDB (in-process), Spark 4.1.3 (newest line with an Iceberg 1.12.0 runtime; 4.2 has
  none yet), Flink 2.2.1 (the Phase 4 image, SQL client in batch mode) and Trino 483. Spark and
  Trino run only for the check (profiles `spark`, `trino`); images pinned by digest, jars by
  SHA-256. Every port binds to 127.0.0.1.
- Check (`keystone warehouse verify`): one fingerprint query per table, built from the Iceberg
  schema: row count; per column the non-null count, plus the sum of an integer or boolean
  column, or the distinct count of any other column and the summed character length of a
  string. All exact integers, the same SQL in every engine (Trino spells CHAR_LENGTH as
  LENGTH), compared against the first engine. Limit: a timestamp or date read with a constant
  shift (a time-zone slip) keeps its distinct count, so it would not be caught; there is no
  engine-neutral epoch function. A requested namespace with no tables fails the check.
  `period` was renamed `completeness_period` because it is reserved in Flink SQL.
- Measured 2026-10-20: the nine marts (sample hours of every era, the day 2026-10-19 and the
  PyPI OSV zip) and the Flink-written `replay_20261019_batch.repo_hour` match in all four
  engines (after the review fixes: one run DuckDB+Spark+Flink, 30 of 30 reads, 5.2 min; one run
  DuckDB+Trino, 20 of 20, 2.2 min; the distinct counts make it slower than the first version's
  52 s). The parity workflow runs all four together on fixtures with a fresh Flink batch table.

## ADR-028 (2026-10-20) Warehouse: dbt on DuckDB over silver and the free rollup
- dbt-core 1.12.6 with dbt-duckdb 1.11.0 (`warehouse` dependency group); anonymous usage
  stats off. The CLI (`keystone warehouse build`) sets every path from `--root`, runs
  `dbt build` and `dbt docs generate`, then a docs-coverage gate (>= 95%) that compares the
  real columns in `catalog.json` with the YAML descriptions (100%: 19 models and seeds, 174
  columns). Orchestrators call the CLI, never dbt.
- Inputs: silver Parquet (GH Archive events, OSV) read in place and the free engine's DuckDB
  file attached read-only. `repo_actor_day` (salted actor keys) is not declared as a source,
  so no model can read it; a singular test (itself tested with a planted column) fails on any person-like column (actor, login,
  email, author, owner) in `marts` or `metrics`, and the publish refuses such columns again.
  No owner column is derived from repository names.
- OSV silver (`keystone parse osv`): vulns, affected packages and repository pointers, an
  allow-list like ADR-019 (no `details`, `summary`, `credits`, IOCs). Pointers are GIT ranges
  (belong to one package) and FIX references (belong to the record); advisory, web and package
  links are not repositories. OSV repeats a record in the zip of every ecosystem it affects;
  staging keeps one copy per id (latest `modified_at`, then `source_file`) and only that copy's
  affected rows and pointers (tested with a duplicate zip). PyPI: 26,393 records (11,852 malicious-package reports), 31,997
  affected entries, 3,797 pointers, 0 quarantined.
- Models: `dim_repo` and `dim_repo_name_history` (spells of a name per repository id, ordered
  by first sighting, compared case-insensitively since GitHub names are; a name taken again is
  a new spell; the E1/E2 overlap rule of ADR-020 applies here as in the rollups; ids missing in
  E1 2012 and some E0 are left to Phase 7), `dim_package`
  (ecosystem + name as OSV spells it), `bridge_package_repo` (confidence is a rule: high = GIT
  range, medium = FIX reference in a one-package record, low = FIX reference in a multi-package
  record; `repo_id` only when exactly one id ever carried the name, else `ambiguous_name`),
  `fct_vuln` (record x package), `fct_repo_day` (UTC day x repo x era x completeness period:
  era and period are keys, so nothing mixes) and metric inputs per version: `maint_v1` (E1,
  E2: merged flag), `maint_v2` (E3: closes only), `release_v1` (E1 to E3). Accepted-values tests
  keep other eras out. dbt unit tests pin the spell logic, the overlap rule, the UTC day cut and
  the per-type pivots. The periods and eras seeds are copied from `config/sources.yml`; a
  pytest test fails if they differ.
- Measured on the local data (151 sample hours of every era plus 2026-10-19, PyPI OSV), after
  the review fixes: 73 nodes pass (3 of them unit tests), build plus docs 22 s; 809,212
  repositories (812,300 name spells; their events equal the 2,094,765 of `fct_repo_day`),
  867,596 repo-days, 13,846 packages, 526 package-repo candidates (242 with a unique repository
  id), 27,894 vuln-package rows. The
  event total of 2026-10-19 in `fct_repo_day` (1,042,689) is the day's 1,042,693 events minus 4
  without a repository id.

## ADR-027 (2026-10-20) Lakehouse components confirmed for Phase 4 (updates ADR-014)
- Catalog: **Lakekeeper v0.14.1** (Iceberg REST) on Postgres 17.6; object store: **RustFS 1.0.3**
  (Apache-2.0, single binary, S3 API). Both run in the `catalog` compose profile; a one-shot
  `catalog-init` creates the bucket (SigV4 via curl), bootstraps Lakekeeper and registers the
  `keystone` warehouse (S3 profile, path style, STS off). SeaweedFS was the alternative; RustFS
  has stable 1.0 releases and needs no master/volume split. MinIO stays out (ADR-014).
- Flink writes through the REST catalog with S3FileIO; DuckDB reads the same tables through the
  REST catalog (`ATTACH ... TYPE iceberg`) with its own S3 secret. Credentials are local-only
  defaults, overridable in `.env`; every port binds to 127.0.0.1.
- Open owner question: Lakekeeper's bootstrap call requires `"accept-terms-of-use": true`.
  `catalog-init` sent it for the local stack and the parity CI does too. Accepting terms is an
  owner decision, so it is listed under Blockers in PROGRESS.md until the owner confirms.
- Measured (docker stats, Docker VM 8.2 GB): both profiles use about 1.4 GB idle and about
  3.0 GB after the one-day replay (taskmanager 1.37 GB, jobmanager 0.68 GB, RustFS 0.45 GB).

## ADR-026 (2026-10-20) `full` rollup: Redpanda + Avro + Flink 2.2.1 to Iceberg, late rows kept
- Versions: Flink 2.2.1 (newest supported by both flink-connector-kafka 5.0.0-2.2 and
  iceberg-flink-runtime-2.2 1.12.0, ADR-014), Redpanda v26.2.5 (Kafka API and schema registry).
  Connector jars are pinned by SHA-256 in `compose/flink/Dockerfile`; images by digest. License:
  Redpanda is BSL 1.1 (source-available, not OSI open source), allowed for this non-commercial,
  self-hosted use; Flink, Iceberg, Lakekeeper and RustFS are Apache-2.0. Redpanda Console was
  dropped: nothing used it (tool-credibility rule).
- Topic value: Avro (`contracts/avro/gharchive_event.avsc`, the columns the rollups read plus
  lineage), Confluent wire format, subject `<topic>-value` with BACKWARD compatibility; the
  producer checks compatibility before registering, `keystone stream check-schema` checks a
  candidate. Tested live: an optional field passes, a required field or a narrowed type fails.
- Replay: GH Archive is hourly files, not a firehose. Hours are produced in arrival order (event
  time of their content by default) to a one-partition topic, so the topic order is the arrival
  order. Watermark `created_at - 1 h`, emitted per event (`scan.watermark.emit.strategy`).
- Late data: Flink SQL window TVFs have no allowed lateness and drop late rows silently. This is
  a deliberate deviation from the brief's "allowed lateness": instead of keeping windows open,
  the job applies the window operator's own lateness test (`window_end - 1 ms <= watermark`) with
  `CURRENT_WATERMARK` in the Calc after the source and routes those rows to `<rollup>_late`
  (append) while the rest go through TUMBLE to `<rollup>_ontime`. Why it is exact: the Calc's
  watermark is never behind the window operator's (same chain at parallelism 1; after a hash
  exchange the window operator takes the minimum over its inputs), so a row flagged on time is
  never dropped, and the two filters split every row exactly once. The parity test checks this
  conservation (`sum(events)` over both tables equals the filtered input). Both inserts run in one statement set (one job, one
  source). A batch fold writes `<rollup>` = SUM/MAX over both. So nothing is dropped and the
  result does not depend on when the watermark moved. The parity test checks the late path ran
  and that `_ontime` alone differs (the test can see the difference).
- Exactly-once is *configured*, not yet failure-tested: checkpoints every 10 s (default
  exactly-once mode), Kafka offsets live in the checkpoint and the Iceberg sink commits per
  checkpoint (final checkpoint at the end of a bounded replay). A kill-the-taskmanager recovery
  test is a Phase 9 benchmark item.
- Modes: `stream` (bounded replay: rebuilds its tables and waits; live with `--unbounded`: keeps
  its tables, resumes from its consumer group, returns after submit, and needs `rollup fold`
  periodically, so corrections appear at fold time, not in the streaming output), `batch`
  (backfill; the same SQL, nothing is late), `fold` (batch). Limit: the stream is append-only, so a re-parsed hour that was already
  produced needs a batch rebuild of its windows in this engine (DuckDB handles it in place);
  Phase 9/10 wire that rebuild into the backfill and the profile fallback.

## ADR-025 (2026-10-20) `free` rollup: DuckDB recomputes touched windows from silver
- State: one DuckDB file (`data/rollup/rollup.duckdb`) with the rollup tables and
  `rollup_files` (silver hour, event-time range and a fingerprint: source SHA-256 plus size and
  mtime of the hour's silver files, which are replaced atomically). The source SHA alone would
  miss a re-parse of the same raw file (parser fix, bot list, salt); the test re-parses with an
  unchanged source SHA. A run finds new, rewritten and removed hours, takes every window their old and new ranges touch, reads every
  silver hour whose range overlaps those windows (from `hour_types`, so a late hour corrects
  earlier windows), and deletes and re-inserts those windows in one transaction.
- Options: add deltas per hour (rejected: not idempotent on re-parse), MERGE per key (rejected:
  a re-parse would leave stale keys), recompute touched windows (chosen).
- Why: idempotent (a re-run is a no-op), independent of arrival order (tested: incremental in
  arrival and reversed order equals a full run), and bounded: a run reads only overlapping hours.

## ADR-024 (2026-10-20) Rollup logic once, in portable SQL
- Each rollup is one ANSI contribution query in `src/keystone/rollup/sql/` (one row per event:
  event time, keys, 0/1 measure columns) with a header naming the window (`1 hour` or `1 day`),
  the keys and the MAX measures (all others SUM). `input.sql` holds the shared filter. DuckDB
  wraps a query with `date_trunc` and GROUP BY; Flink with TUMBLE. Neither engine has its own
  copy of a measure. SQLFluff (ANSI dialect) lints the files; a test rejects engine functions.
- Tables: `repo_hour` (repo, era, event type: events, human events, opened, closed, reopened,
  merged and `merge_known`, published, added, newest fetch) and `repo_actor_day` (repo, salted
  actor key, era: pushes, PRs opened/closed/merged, issues opened/closed, releases, members
  added, comments; bots excluded; internal only, never published). Era is a key everywhere, so
  eras never mix; `merged` counts only where `merge_known` does (E1, E2).
- Rules: windows are UTC; ADR-020's overlap rule (E1 rows from 2015-01-01 00:00Z dropped) is in
  `input.sql`; timestamps are cut to milliseconds (the Avro contract) in both engines.
- Watermark delay 1 h. Measured on the 151 sampled silver hours (hour_types): every E0, E2 and
  E3 file holds only its own UTC hour; E1 files (Pacific names) span at most 60.1 minutes and 68
  of 72 cross a UTC hour boundary by seconds. Replayed in event-time order, nothing is late.
  Late arrival (a healed gap, an out-of-order replay) is handled by ADR-025/026, not by a longer
  delay.

## ADR-023 (2026-10-20) Contracts with datacontract-cli, expectations with Great Expectations
- Contracts: ODCS v3.2.0 YAML in `contracts/` (events, hour_types, quarantine), tested by
  datacontract-cli 1.2.7 (DuckDB over the Parquet files). Column-level rules (required, patterns,
  valid values) plus SQL rules (one row per source line, key kind per era, ids per era, repo name
  form, the payload-trim rule). datacontract-cli does not check column types on Parquet (it warns);
  `tests/test_contracts.py` checks names, types, required and descriptions against the Arrow
  schemas instead. Any other warning, failure or error fails the run.
- Expectations: Great Expectations 1.24 (ephemeral context, one Parquet file per batch), for the
  per-era rules with row conditions (E1 keys by login, offsets only in E1, trimmed pushes by event
  time). GX usage analytics are switched off in code before import.
- Both live in the `quality` dependency group. CI: `contracts` job (lint every contract, parse the
  scrubbed fixtures, run contracts and GX) and the pytest suite (seeded violations must fail).
- Why both: contracts are the published interface (ODCS, portable to dbt and Databricks later);
  GX holds cross-column and per-era rules. Each does real work and each caught a seeded violation.

## ADR-022 (2026-10-20) Schema drift: learned registry per era, fail the hour loudly
- The registry `contracts/drift/gharchive.yml` lists key paths up to 3 levels deep (array
  elements as `[]`) and JSON types per era and event type; paths outside the payload are shared
  per era. `null` is accepted anywhere. It holds paths and type names only, never values.
- An unknown event type, field or type, or a record whose shape does not match its era (or a
  push whose shape contradicts the payload-trim instant), fails the whole hour: nothing is
  written, a value-free report goes to `data/quality/drift/`, and the manifest marks the hour
  `error` so the next run retries it. `keystone parse learn-drift` adds a reviewed sample to the
  registry; its git diff is the review.
- Measured: a 6-hour baseline flagged 49 of the 123 hours kept at that point (rare types,
  null-only fields). A
  backfill will hit drift in hours unlike the samples; that is the intended stop, not a bug.
  Phase 9 learns from a stratified sample (one day per quarter) before the run.
- Known edge (review): a late event inside an E3 file created before the trim instant but
  without `payload.size` would fail its hour on every retry. None was seen in the sampled E3
  hours; Phase 9 measures late pushes before the backfill and either handles them or keeps them
  as drift with an ADR. `learn-drift` refuses hours with quarantined records.

## ADR-021 (2026-10-20) Completeness boundaries refined from parsed counts
- C2 starts 2025-05-23 16h (was 18h): capture falls inside 16h from about 16:35Z (per-minute
  counts); C1 ends at the last complete hour, 15h. GAP ends at 2025-10-14 20h and C3 starts at
  21h (was 2025-10-15 00h): 21h is 30.5 MB, 20h 0.25 MB.
- C4 and C5 starts are unchanged: the oracle hours show non-push types falling gradually from
  2026-03 to 2026-08, not a step, so a cheap exact boundary does not exist. They stay bracketed.
- `tests/test_ingest.py::test_hour_names_and_periods` pins config boundaries, not a docs oracle;
  it was updated to the measured values. Spike oracles and data-audit numbers are unchanged.

## ADR-020 (2026-10-20) Schema eras measured; E1/E2 overlap rule
- E1 starts at file 2012-03-10 09h (E0 ends at 08h, which holds one event; 09h to 21h are empty;
  the first E1 data is 22h). E1 file names are Pacific-local hours (DST included), so the E1 files
  of 2014-12-31 16h-23h cover 2015-01-01 00:00 to 08:00 UTC, which E2 files also cover.
- Rule for rollups (the first phase that aggregates events): event-time windows take E2 rows
  from 2015-01-01 00:00Z and drop E1 rows with `created_at >= 2015-01-01 00:00Z`. Silver keeps
  both, faithful to the source.
- E3 starts at file 2025-10-08 17h. GitHub's payload trim is an instant, 2025-10-08 16:59:22Z
  (`payload_trim_at`): push shape is checked against each event's created_at, not the file era.
- Early E1 `created_at` uses `YYYY/MM/DD HH:MM:SS -0800` until 2012-06-01 09h, and `public` may be
  the integer 0 in 2012-03; both are parsed faithfully, never guessed.

## ADR-019 (2026-10-20) Parser: allow-list projection to typed Parquet, privacy at read time
- One pydantic model per table (strict, extra fields forbidden); the Arrow schema is derived from
  the model, so Parquet types cannot drift from it. One Parquet file per source hour and table
  (`data/silver/gharchive/{events,hour_types}/date=YYYY-MM-DD/`), written atomically, so a re-run
  overwrites (idempotent). Every row keeps `source_file`, `source_sha256`, `fetched_at` and
  `source_line`.
- Privacy: the event row copies only repository-level and event facts. People appear only as
  `HMAC-SHA256(KEYSTONE_ACTOR_SALT, kind:value)`, first 32 hex characters, by account id (E0, E2,
  E3) or lower-cased login (E1, `actor_key_kind = login`); PR and issue authors use the same kind.
  No salt, no run (no default). Bots are flagged (`[bot]` suffix, `config/bots.yml`).
- Quarantine rows hold the reason, line number, line hash and length, never record content.
- Ingest hook: `GhArchiveSink` is `on_file`; `is_done` makes ingest fetch and parse hours that
  were verified before parsing existed (Phase 2), instead of skipping them as unchanged.
- E0 `repo.name = "/"` (no repository) is stored as null; other malformed names fail the contract.

## ADR-018 (2026-10-20) OSV increments: head of modified_id.csv plus ecosystem zips as baseline
- Context: `modified_id.csv` is 151 MB, sorted newest first, and is rewritten often (its ETag
  changed within an hour on 2026-10-20). `all.zip` is 2.78 GB.
- Choice: read the head in 256 KB Range chunks until the stored cursor (nanosecond timestamp
  plus the keys at that timestamp, so ties are handled), using `If-Range` on the first chunk's
  ETag and restarting (at most 3 times) if the file changes mid-read. On the first run, read at
  most 1 MB; the per-ecosystem `all.zip` files (conditional GET, MD5-verified, kept) are the
  baseline. Increments are written to Parquet, named by their modified range; unparseable lines
  are quarantined.
- Measured: the first run read 1 MB (19,144 rows); later runs read one chunk.

## ADR-017 (2026-10-20) GAP period starts at 2025-10-08 17h (measured)
- A Phase 2 HEAD-only scan found that 2025-10-08 17h, 18h and 19h are already near-empty
  (166 to 250 KB), while 16h was 97 MB (era-map). `config/sources.yml` now starts GAP at 17h
  instead of the bracketed "about 20h". Spike oracles are unchanged; spike-report section 4
  carries a dated note.

## ADR-016 (2026-10-20) Ingestion manifest in DuckDB; stream and delete by default
- Options: JSONL log, SQLite, DuckDB. Choice: **DuckDB** (`data/manifest/manifest.duckdb`, already
  a dependency): `files` holds one row per URL, idempotent `INSERT OR REPLACE`, and `checks` is an
  append-only log. Each check commits on its own, so an interrupted run resumes. Timestamps are
  naive UTC.
- Status values: new, unchanged, changed, missing, empty, and also **error** (verification or
  transport failure; earlier verified facts are kept, and the hour is retried every run). Flags
  `near_empty` (at least 2015 and under 1 MB) and `late` (Last-Modified more than 6 h after the
  hour) persist across re-checks; `gaps` also reports hours under 10% of the range median.
- Conditional GET (If-None-Match) only after a verified SHA-256 is stored; a HEAD-only scan
  (`--head-only`) maps sizes cheaply without downloading. Files are deleted after the `on_file`
  hook unless `--keep`; the disk guard (budget = 70% of free disk, optionally lower) is a hard stop.
- Limitation: DuckDB holds a file lock, so `gaps`/`status` cannot run during an ingest.

## ADR-015 (2026-10-20) gh CLI allowed (owner)
- Owner amended the brief: `gh` may be used on Keystone repos (CI status, runs, issues, PRs).
- Guard: run `gh api user --jq .login` first; proceed only if it prints `azzammasood` (the keyring
  also holds azzamverse). Still never `gh repo create`, visibility or settings changes without asking.

## ADR-014 (2026-10-20) Provisional lakehouse components (confirm in Phases 4-5)
- Catalog: **Lakekeeper** over Polaris (Rust single binary plus Postgres, smaller memory footprint on a
  16.8 GB machine). Object store: **RustFS or SeaweedFS** over MinIO (MinIO's last community
  release tag is 2025-10-15). Flink: the newest version that both flink-connector-kafka and the
  Iceberg Flink runtime support (Kafka 5.0.0 supports 2.1/2.2; Flink 2.3.0 is the latest). Each is
  tested before it is fixed; the ADR is then updated.

## ADR-013 (2026-10-20) Timeline-era privacy and identity
- E1 (2012-2014) events carry `actor_attributes.email` and `name`, and no `actor.id`. Parsers drop
  both fields at read time. The salted actor hash uses the login in E1 (`actor_key_kind = login`),
  and distinct-actor metrics are versioned at the E1/E2 boundary. In 2012 repos have no id, so
  they are resolved by name with a stored confidence.

## ADR-012 (2026-10-20) Completeness periods drive metric windows (owner informed)
- Context: measured capture drops (spike-report section 4): -36% from 2025-05-23 18h, a near-empty
  gap from 2025-10-08 20h to 2025-10-14, and non-push types near zero from about 2026-04.
- Options: (a) treat all data alike (rejected: the signals would measure artifacts); (b) stop at
  2025-05-23; (c) ingest everything, publish a per-type completeness series, and compute activity
  signals only on periods that pass a completeness check, with the as-of date shown.
- Choice: **(c)**. Score windows default to ending at the last complete period. Periods after it
  are flagged and excluded from counts and trends unless a type-level check passes. Shares (for
  example concentration) may use a degraded period only if its per-type completeness is uniform,
  which a test checks. The live refresh keeps OSV, criticality and freshness current. This keeps the
  site updating at $0 and honest about what changed upstream.

## ADR-011 (2026-10-20) Hugging Face mirror rejected
- open-index/open-github: unofficial, last modified 2026-04-09, missing 33 of 2,146 releases on
  2015-06-15, no files for 2019/2024/2026 probes. GH Archive stays the only event source.

## ADR-010 (2026-10-20) Criticality and mapping: ecosyste.ms API plus registry cross-check
- Options: deps.dev API (robots `Disallow: /`, so excluded), deps.dev BigQuery (needs a project),
  ecosyste.ms dump (63.7 GB, over budget), ecosyste.ms API (open, 5,000 req/h anonymous, CC BY-SA 4.0,
  `critical` flag, dependents, downloads, `repository_url`), registry metadata.
- Choice: the ecosyste.ms API for the critical set and criticality inputs, cached and refreshed
  weekly within its limit, cross-checked with npm/PyPI `repository` fields for exact matches.
  The site attributes ecosyste.ms (CC BY-SA 4.0). Non-commercial use only.

## ADR-009 (2026-10-20) Backfill window: the whole archive, 2011-02-12 00h to the present
- Rule (brief section 10): the largest $0 window with an estimated 7 days or less unattended,
  minimum 12 months.
- Estimate (`scripts/spike/size_estimate.py`): 4.86 TB gz, about 4.0 days at 2 polite streams
  (median 6.96 MB/s each); parsing overlaps (about 2.8 days serial).
- Score windows are a separate choice (ADR-012): signals end by default at the last complete
  period, 2025-05-23 (bracketed 12h to 18h). The first draft chose C1 only; corrected after review.
- Executed newest-first and resumable in Phase 9. The run waits on the owner because free disk is
  below 50 GB (brief section 1).

## ADR-008 (2026-10-20) Subpackages created when a phase needs them
- Context: brief section 9c lists 15 subpackages. Options: create all now (empty), or on demand.
- Choice: on demand; CLAUDE.md lists the planned layout. Why: empty packages are noise and fail
  the tool-credibility spirit (nothing listed that does no work).

## ADR-007 (2026-10-20) Dependency audit via `uv export` + pip-audit
- Options: pip-audit on the runner env; `uv export` lockfile audit; OSV-Scanner.
- Choice: `uv export --all-groups` -> `pip-audit --strict -r`. Why: audits exactly the locked set,
  $0, no extra binary. OSV-Scanner may be added later since Keystone consumes OSV anyway.

## ADR-006 (2026-10-20) Scorecard while private
- Context: Scorecard publishing and code-scanning SARIF upload need a public repo (or paid GHAS).
- Choice: workflow runs now, keeps SARIF as an artifact; publish + upload switch on automatically
  when repo variable `KEYSTONE_PUBLIC=true` is set at go-public (review finding: schedule
  payloads have no `repository.private`). Why: $0, one switch at launch.

## ADR-005 (2026-10-20) Type checker: mypy (strict on `src/`)
- Options: mypy, pyright, ty. Choice: mypy. Why: most widely known in data-engineering teams,
  stable, pydantic plugin available, runs via uv with no Node dependency.

## ADR-004 (2026-10-20) Actions pinned to commit SHAs
- Choice: every `uses:` pinned to a full SHA with the tag in a comment; Dependabot bumps them.
  Why: Scorecard "Pinned-Dependencies" and supply-chain hygiene (Keystone measures this in others).

## ADR-003 (2026-10-20) Commit identity
- Context: brief says ask for the commit email. Roshni's repo-local identity is
  `azzammasood <ahmaduzzammasood@gmail.com>`, the same account Keystone belongs to.
- Choice: use that identity, repo-local only. Owner to correct if a different email is wanted
  (history is still local, so it can be rewritten at no cost before the first push).

## ADR-002 (2026-10-20) SSH alias `github-azzammasood`
- Found: `~/.ssh/config` has `github-azzammasood` (id_ed25519) and `github-azzammasood-coder`
  (a second key). `ssh -T` answers `azzammasood` and `azzammasood-coder` respectively; Roshni's
  remote uses `github-azzammasood`. Choice: `git@github-azzammasood:azzammasood/keystone-pipeline.git`.
  Not ambiguous: the alias, account and Roshni precedent agree.
- Note: the brief says gh is logged into azzamverse; `gh auth status` shows the active account is
  **azzammasood**. Irrelevant to Keystone (gh is not used against these repos) but recorded.

## ADR-001 (2026-10-20) Local root and Python
- Root `C:\Users\ahmad.d\Documents\projects\keystone\` (exists, sibling of `roshni\`), branch `main`.
- System Python is 3.11; uv provides 3.12 (`requires-python >=3.12,<3.13`). Scripts are
  cross-platform (pathlib), so the PowerShell-vs-Git-Bash question does not block anything.
