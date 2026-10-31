# Progress

Resume point for any new session. Updated at every gate and before long runs.

## Now
**2026-10-30: backend fully complete (ADR-065 to ADR-080).** The daily publish is green on
Actions (run 41027674711, commit 23e5b3a); CI and scorecard green on 2ef628b.
- **Live score:** cut 2026-10-01, 4,421 of 7,792 mapped repositories scored, with 100.0% of
  the window's hours covered. It reads the backfill's `data-live` plus the daily rollup's later
  days (ADR-080).
- **Brief:** written on Actions with the free OpenRouter model (2026-W44, every number checked).
- **engines[]:** lists BigQuery, databricks-sql, dbt-databricks and databricks-lakeflow
  (imported from `cloud-evidence`). The README names them with measured numbers only.
- **Backfill:** 78 of 78 chunks, 4,574,059,729 events, 1,873.2 GB downloaded, 37.88 job hours
  (`docs/benchmarks.md` section 5).
- **Backtest** (`docs/backtest.md`): event-stream a miss (18.4th percentile); xz a miss
  (4.0th); colors-faker not scored (30% of the weight available); leakage check passes.
- **Drift:** learned for E2 and E3 into `contracts/drift/gharchive.yml`. **Review:**
  `docs/reviews/phase-9.md`, clean for the gate.
- **Publish fixes made on the way:**
  - ADR-074/075: registry claims fetched in publish, with retries on a dropped body.
  - ADR-076: the fetch has a 35-minute budget, and the cache is kept after a failed step.
  - ADR-077: reviewed decisions name the claimed repository.
  - ADR-078: an empty quarantine counts as clean.
  - ADR-079: a refused npm name goes to quarantine.
  - ADR-080: the live window is read.
- **Phase 9 review fully resolved (2026-10-30, ADR-081, ADR-082):**
  - `top_actor_events` is replaced by `top_actor_share`, withheld below 3 human actors.
    All 78 published contributors assets were rewritten (78 ok, 0 failed, rows checked
    against each chunk's record).
  - Retry totals and overlap hours are counted apart in version 2 chunk records.
  - Polling deadlines, the Lakeflow catalog, the BigQuery truncation guard, the slot release,
    the secrets check and the line continuations are fixed. The Databricks rung is green
    after the change (run 41049631604, Lakeflow COMPLETED, 178 rows).
- **Radar names (2026-10-31, ADR-083):** a scored repository missing from `dim_repo` takes the
  unique name of its accepted package mapping. Before: 6,363 of 7,792 radar rows had no name;
  after (publish run 41179922737): 0 of 7,798 (radar and project pages).
- **Known cost:** each publish downloads about 5 GB from `data-live` (ADR-080).

**2026-10-27 (ADR-069, ADR-070):**
- **Backtest on Actions works** (run 40454178871, `docs/backtest.md`). Three runs failed on
  memory first; fixed by bounded DuckDB plus the reference filter pushed into SQL (ADR-070).
  - event-stream: scored, a miss (18.4th percentile; never flagged in 22 weight variants;
    leakage check passes).
  - colors-faker: partial data (88.8% of hours, needs 90%) until run 40451202607 redoes
    2021-05 and the fetch-error hours.
  - xz: pending.
- **Drift learned for E2** (16 entries; `keystone backfill drift --learn`, ADR-069).
- **Backfill (status 2026-10-27 17:30Z):**
  - xz + live (40371109404): 22 of 52 chunks done, no job failures, about 34 min per chunk;
    done around 2026-10-28 10:00Z.
  - Then one redo for all windows (40548762011, ADR-071: `chunk_days=16`,
    `split_from=2023-01-01`, `end=2026-10-27`, `redo_incomplete`). It covers colors-faker
    2021-05 plus 5 incomplete months, 5 xz chunks with failed downloads, and any live ones.
    Done around 2026-10-28 20:00Z.
  - 2021-08-25 17h to 08-27 22h (54 h) are absent at GH Archive: that chunk stays incomplete.
- **Then:**
  1. Rerun `backtest.yml`.
  2. `keystone backfill drift --learn` over all records: E3 and live; the 2025-10-01 chunk
     spans the break and is not learned.
  3. Fill `docs/benchmarks.md` section 5.
  4. Write `docs/reviews/phase-9.md`.
  5. Dispatch publish: brief, `engines[]` cloud tools, README.

**2026-10-26 (ADR-065 to ADR-068): backfill publishing again; cloud rungs ran.**
- Backfill: the 403 at `gh release create` was the tag target (run commit with older workflow
  files; GITHUB_TOKEN has no `workflows` permission), fixed (no `--target`) and the releases
  `data-colors-faker`, `data-xz`, `data-live` created once (ADR-065). Merge memory capped at 8 GB
  (ADR-066). Runs: 40371049281 (colors-faker, 13 whole months), then 40371109404 (xz + live,
  `chunk_days=16`, 52 chunks; queued behind it in the concurrency group). event-stream complete
  (13 chunks, 517,146,065 events). Projection (from 2021 months at 36-48 min each; later months
  are larger, so a range): colors-faker done around 2026-10-27 04:00Z, xz + live 22-42 h later,
  i.e. about 2026-10-28 02:00Z to 2026-10-28 22:00Z. Each record's `peak_rss_mb` checks ADR-066.
- Cloud (`cloud.yml`): Databricks Free Edition (stage to Unity Catalog, `dbt build --target
  databricks` 123 nodes / 94 tests pass, Lakeflow pipeline COMPLETED, lineage readable) and the
  BigQuery sandbox (bytes per column set, in-place rollup billed 29.4 MB, count equals our parse)
  ran on run 40374905775; numbers in `docs/benchmarks.md` sections 6-7. Weekly Databricks
  keepalive is scheduled. `engines[]` picks them up at the first publish after the backfill
  (`keystone metrics import-runs` from the `cloud-evidence` release); README mentions wait for it.
- Waits on the backfill: the first publish (and with it the brief on Actions with
  `OPENROUTER_API_KEY`, guarded `:free` only, $0, numbers checked), `backtest.yml`, learning the
  summed `hours.drift` into `contracts/drift/gharchive.yml` (ADR-051), and the final Phase 9
  totals + `docs/reviews/phase-9.md`. Unity Catalog lineage: 104 edges into 28 tables
  (rerun 40376934911, green with the audited lock).

**Backend complete (Phases 0 to 10), except the backtest rerun, which waits on the Phase 9
backfill** (2026-10-23, ADR-060 to ADR-064). When the backfill has landed:
`gh workflow run backtest.yml` (incident windows), then the first ingest runs on its schedule
(it yields while a backfill run is queued or running) and the daily publish uploads the site
exports to the pre-release `site-data`; the radar stays "partial data" until the live window's
hours are covered. Schedules and attestations are on (`KEYSTONE_SCHEDULES=true`,
`KEYSTONE_PUBLIC=true`, set 2026-10-23). The brief is skipped on Actions until the owner adds
the secret `OPENROUTER_API_KEY` (a free-tier key; optional).

Phase 8 (scoring and backtest) gate checks pass locally (2026-10-22): `keystone score
live|explain|backtest`, score_v1 (ADR-044), leakage and sensitivity checks; review
`docs/reviews/phase-8.md` (2 major, 6 minor, 3 nit; fixed or documented). **Backtest: pending
data** (`docs/backtest.md`): no incident window is in the local rollup yet. Rerun after the
Phase 9 backfill lands: `uv run keystone score backtest --rollup <rollup.duckdb or Parquet dir>
--warehouse data/warehouse/keystone.duckdb` (needs `repo_actor_month` for concentration and
churn; spec in ADR-044). On Actions: `gh workflow run backtest.yml` downloads the `data-<window>`
assets and decrypts `repo_actor_month` on the runner (ADR-045; owner approved the
encrypted asset 2026-10-22; secret `KEYSTONE_ACTOR_DATA_KEY`). **Phase 8 complete**: CI green on 3729988 (ci, parity, scorecard: runs
39769357851, 39769357770, 39769357745; checked with gh as azzammasood).

## Done
- 2026-10-31: the old blockers are cleared and moved here. Tokens arrived and the
  Databricks, BigQuery and OpenRouter rungs ran (ADR-067, ADR-068). The disk shortfall
  (28.2, 20 and 17.8 GB free) was designed around: the backfill ran on Actions with
  Release assets (ADR-039, ADR-065). Completeness after 2025-05-23 (ADR-012) is a data
  caveat carried by the completeness periods; the live score's window is 100% covered.
  Issues #2, #3 and #4 are closed with links to green runs.
- 2026-10-20 Phase 0 local: repo initialised (`main`), identity set repo-local (ADR-002/003),
  uv project with `keystone` CLI (`info`, `machine`), policy config, tests (6 pass), ruff + mypy
  strict clean, pre-commit, CI, Scorecard, Dependabot, CLAUDE.md, PLAN, DECISIONS, Roshni patterns.

- 2026-10-20 Phase 0 pushed to `origin/main`; review in `docs/reviews/phase-0.md` (1 major, 4 minor, all fixed).
- 2026-10-20 Phase 1 spike: 33 sample hours (1.31 GB downloaded, all deleted), 822-hour HEAD sweep,
  OSV, ecosyste.ms, deps.dev, HF mirror, tooling versions. Oracles frozen (tests/test_oracles.py).
- 2026-10-20 Phase 2 ingestion: `keystone ingest gharchive|gaps|osv|status`; polite async client
  (policy limits, robots, retries, MD5-verified conditional GET), DuckDB manifest, gap/late/near-empty
  detection with completeness period, OSV Range increments + zips. 27 tests (MockTransport).
  Gate: 2026-10-19 clean (24 new, 232.7 MB streamed and deleted, 46 s); re-run 24 unchanged (304s);
  injected gap (dropped row + 404) reported and healed. Review: 3 major, 8 minor, all fixed.
  CI + Scorecard green on GitHub (run 39384836335, checked with gh as azzammasood).
- 2026-10-20 Phase 3 parse: era-aware parser (E0-E3) to typed Parquet via pydantic, privacy at
  read time (salted HMAC keys, allow-list projection), quarantine without content, drift registry
  that fails the hour, 3 ODCS contracts (datacontract-cli) and GX suites, golden tests on the
  oracle hours, scrubbed fixtures for every era. Gate: 152 sample hours from every era parse
  (2,363,426 events, 0 quarantined, 0 drift); seeded drift (6 kinds) and seeded contract/GX
  violations are caught; CI green (runs 39394423463 and, after the review fixes, 39456605593:
  lint-test, contracts, security; checked with gh as azzammasood).
  Measured boundaries: E1 from 2012-03-10 09h (Pacific file names, E1/E2 8 h overlap), E3 from
  2025-10-08 17h with the push trim at 16:59:22Z, C2 from 2025-05-23 16h, C3 from 2025-10-14 21h.
- 2026-10-20 Phase 4 rollups: logic once in portable SQL (`rollup/sql`, SQLFluff ANSI), tables
  `repo_hour` and `repo_actor_day`; `free` = incremental DuckDB (recomputes touched windows from
  silver, ADR-025); `full` = silver replay to Redpanda (Avro, registry BACKWARD) and a Flink 2.2.1
  SQL job to Iceberg (Lakekeeper + RustFS) with late rows routed and folded, not dropped (ADR-026).
  Gate: parity tests pass on the stack (synthetic late/out-of-order scenario in stream and batch
  mode, the committed fixture hours, live schema compatibility); one-day replay of 2026-10-19
  (1,042,693 events) matches row for row in stream, batch and a shuffled-hours stream run
  (854,442 late contributions folded). CI: `parity.yml` (path-filtered, weekly). Review: 1 major
  (re-parse detection), fixed. CI green on 7f5a72c: ci, parity (2.6 min on the runner, stack
  built from scratch) and scorecard (runs 39465097661, 39465097650, 39465097750; checked with gh
  as azzammasood).
- 2026-10-21 Phase 5 lakehouse and warehouse: OSV silver (`parse osv`), dbt on DuckDB (`warehouse
  build`: 19 models and seeds, 3 unit tests, docs coverage 100%), `dim_repo` + name history,
  `dim_package`, `bridge_package_repo`, `fct_repo_day`, `fct_vuln`, metric inputs per era
  (maint_v1, maint_v2, release_v1); marts published to Iceberg by DuckDB and read with identical
  fingerprints in DuckDB, Spark 4.1.3, Flink 2.2.1 and Trino 483 (ADR-028 to 030). Review:
  `docs/reviews/phase-5.md` (2 major, 6 minor, 4 nit; all fixed or documented). Databricks rung
  skipped (no token). CI green on e4fadda: ci, parity (lakehouse step: 10 tables x 4 engines,
  40/40 match; whole run 4.8 min) and scorecard (runs 39538664988, 39538664991, 39538664965;
  checked with gh as azzammasood).
- 2026-10-21 Phase 6 metadata, lineage and observability: OpenLineage from the CLI, dbt
  (dbt-ol), Flink (openlineage-flink) and Spark (ran, no datasets for reads) into Marquez 0.51.1;
  DataHub v1.7.0.1 (dbt, Iceberg, Kafka sources; domains, owner group, glossary, tags,
  57 assertions, column lineage); static export `metadata/` (catalog, glossary, lineage,
  25 generated + 3 hand-written ODCS contracts); OTel collector, Prometheus, Grafana;
  `pipeline_metrics.json` with JSON Schema (ADR-031 to 035). Airflow lineage deferred to
  Phase 10, Databricks skipped (no token). Review: `docs/reviews/phase-6.md` (2 major, 10
  minor, 6 nit; all fixed). CI green on e4c627a: ci incl. the new metadata job, parity (Flink
  lineage collected, fixture gate path complete) and scorecard (runs 39557999916,
  39557999733, 39557999772; checked with gh as azzammasood).

- 2026-10-21 Phase 7 entity resolution (ADR-037, ADR-038): `keystone resolve fetch` (ecosyste.ms
  bulk lookups, npm registry cross-check, cached in silver, quota floor, `--replay` for CI),
  `resolve choose` (language-model rung: skipped, no OpenRouter key; interface refuses any
  unlisted candidate) and `resolve report` (match-rate report, review CSVs); dbt
  `map_package_repo`, `dim_repo_rename`, `map_repo_name_id` with status, rule and confidence,
  reviewed decisions as the seed `resolve_decisions`. The polite client now matches robots.txt
  per RFC 9309 (pypi.org disallows its JSON API: no PyPI registry cross-check).
  Review: `docs/reviews/phase-7.md` (3 major, 6 minor, 4 nit; all fixed; npm registry and
  ecosyste.ms now count as one source family). First parity run failed on Flink (a
  reserved column name, fixed). CI green on 00e32fd: ci, parity (13 published tables x 4
  engines, registry fixtures replayed) and scorecard (runs 39718514239, 39718514258,
  39718514491; checked with gh as azzammasood).

- 2026-10-22 Phase 8 scoring and backtest (ADR-044): `src/keystone/score/` with seven
  explained components (concentration, churn, trend, responsiveness proxy, release recency,
  collaborators added, OSV), fragility withheld below 50% weight coverage, criticality from
  ecosyste.ms dependents (live only), risk = criticality x fragility; backtest cut = first instant
  of the incident month, scored only with >= 90% of the window's hours (and of its last 3
  months); runtime leakage check (exit 1 on any difference), poisoned-data tests, 22 weight
  variants for sensitivity; misses reported as misses. Incidents carry repositories (GitHub API
  ids) and source links. Measured: the local rollup holds 0.1% of the live window's hours and
  none of any incident window, so the live score is withheld ("partial data") and every
  backtest is "pending data".

- 2026-10-23 Phase 10 narration, export, orchestration, fallback, hardening (ADR-060 to 064):
  `keystone export site` (radar, project pages, backtests, benchmarks, metrics, metadata, index;
  envelope 1.1 with `status`; row and metadata JSON Schemas, 11 schemas in CI);
  `keystone narrate brief` (free OpenRouter model, every number checked, cached by input hash,
  2-request cap; live run: written in 1 request, rerun cached with 0); `publish.yml` scores,
  exports, briefs and uploads to `site-data`, fallback issue edited in place; guards: ingest
  yields to the backfill, publish waits for an ingest cache (both dispatched: guard ok, job
  skipped, runs 39907534978 and 39907539094); Airflow 3.3.2 DAG calling the CLI (6 of 6 tasks,
  27.8 s, 13 OpenLineage events from the provider, Airflow in engines[]); `pytz` made a runtime
  dependency (found by the Airflow run). Gate: schemas validate; simulated full outage (5 of 5
  probes down) falls back to free and every site export validates, locally, in Airflow and in
  CI's metadata job. Skipped: Databricks keepalive, Terraform, Elementary (ADR-064).
  Review: `docs/reviews/phase-10.md` (3 major, 9 minor, 4 nit; majors fixed, minors fixed or
  documented: the backfill-side ingest check needs `backfill.yml`). Tests: 251 pass locally.
  CI green on 2a71e91: ci, parity, scorecard (runs 39908989250, 39908989282, 39908989333);
  release dry run 39907749464 (Syft SBOM); guarded ingest and publish 39909007290, 39909011774;
  checked with gh as azzammasood.

## Next
Updated 2026-10-31 (ADR-085, ADR-086). Done from the earlier list: the lineage hop to the
site, the atomic Iceberg publish, the OpenLineage log on `lineage-log`, the warehouse reading
the `full` rollup, and the match-rate report on the backfilled warehouse (measured in
publish 41186539823: names suggested 3,976 to 0; packages 80.0% of a wider 17,186 scope).
What remains:
1. **Deferred metric versions (ADR-086):** first-response time, OSV time to fix, bot-free
   opens and closes, E0/E1 folding. Each is a new metric version; three need a full
   re-backfill.
2. **Showcase limit:** `rollup flink --rebuild` for a re-parsed hour already streamed
   (ADR-026). Skipped: it needs the streaming stack, and `full` never runs on Actions.
3. **Running cost to watch:**
   - each publish downloads about 5 GB from `data-live` (ADR-080);
   - after 2026-10-27 the live window is fed by the daily ingest only. Coverage under 90%
     withholds the score.

## Blockers (hard stops)
None open (2026-10-31). Production runs on GitHub Actions at $0, and every credential the
phases needed is set: Databricks, BigQuery WIF, OpenRouter, the salt and the data key.
Standing constraints, not blockers:
- **Local disk:** 15.5 GB free of 261 GB (keystone machine, 2026-10-31), budget 10.8 GB. Bulk
  data stays on Actions and in Release assets. Freeing disk is the owner's call.
- **Local RAM:** 16.8 GB total, 8.3 GB available. The `full` showcase runs one Compose
  profile at a time.
- **`full` on Actions:** not reachable from a hosted runner. Issue #7 is the designed
  standing notice (ADR-062); scheduled jobs run `free` and say so in `produced_by`.

## Measured
| When | What | Value |
| --- | --- | --- |
| 2026-10-20 | Disk free / total (C:) | 28.2 GB / 261.0 GB |
| 2026-10-20 | Disk budget (70% of free) | 19.7 GB |
| 2026-10-20 | RAM total / available | 16.8 GB / 4.4 GB |
| 2026-10-20 | Logical CPUs | 8 |
| 2026-10-20 | Docker | 28.0.4 |
| 2026-10-20 | uv / Python | 0.11.25 / 3.12 (uv-managed) |
| 2026-10-20 | GH Archive download, single stream | median 6.96 MB/s (1.94 to 10.64) |
| 2026-10-20 | Parse: orjson / DuckDB | 315k events/s / 107 MB JSON/s (medians) |
| 2026-10-20 | Archive size estimate (gz) | about 4.86 TB total (backfill window, ADR-009); to 2025-05-23 about 4.25 TB |
| 2026-10-20 | GH Archive day 2026-10-19 (24 h, gz) | 232.7 MB; 46 s at 2 streams |
| 2026-10-20 | Re-run of a checked day (304s, 2 req/s) | 25 requests, 14 s, 0 B |
| 2026-10-20 | GAP start (HEAD scan) | 2025-10-08 17h (17h-03h: 109 to 250 KB per hour) |
| 2026-10-20 | OSV first increment / next | 1 MB head, 19,144 rows / one 256 KB chunk |
| 2026-10-20 | Parse throughput (1 core, E3 hour, drift on) | 25k to 27k events/s |
| 2026-10-20 | Phase 3 downloads (samples, probes; deleted after) | about 1.5 GB (estimate summed from run logs); 8 oracle hours kept (167 MB) |
| 2026-10-20 | Disk free after Phase 3 (C:) | 25 GB (df) |
| 2026-10-20 | Day 2026-10-19 silver | 24 hours, 1,042,693 events, 85 MB Parquet |
| 2026-10-20 | DuckDB rollup, that day (fresh state) | 2.7 s; 771,868 repo_hour + 374,592 repo_actor_day rows |
| 2026-10-20 | DuckDB rollup, 151 sample hours | 6.1 s, 2.30M events; re-run 0 changed |
| 2026-10-20 | Produce to Redpanda (Avro, lz4, 1 partition) | 52k-57k records/s; 189.8 MB Avro for the day |
| 2026-10-20 | Flink stream job, the day (bounded replay) | 49 s incl. startup (~21k events/s); fold 23 s |
| 2026-10-20 | Flink batch job, the day | 34 s; fold 19 s |
| 2026-10-20 | Phase 4 stack RAM (docker stats) | 1.4 GB idle, 3.0 GB after the replay (VM 8.2 GB) |
| 2026-10-20 | Disk free after Phase 4 (C:) | 25.6 GB; stack images 2.4 GB (docker images, uncompressed) |
| 2026-10-20 | OSV PyPI zip to silver | 26,393 records, 31,997 affected, 3,797 repo pointers, 0 quarantined |
| 2026-10-20 | dbt build + docs (151 sample hours + 2026-10-19 + PyPI OSV) | 73 nodes pass, 22 s; docs coverage 100% (19 nodes, 174 columns) |
| 2026-10-20 | Warehouse marts | 809,212 repos; 867,596 repo-days; 13,846 packages; 526 package-repo candidates (242 with a repo id) |
| 2026-10-20 | Iceberg publish (9 tables, DuckDB) | 12 s |
| 2026-10-20 | Engine reads, 10 tables (fingerprint after review) | DuckDB+Spark+Flink 30/30 match (5.2 min); DuckDB+Trino 20/20 (2.2 min) |
| 2026-10-20 | Reader RAM (docker stats) | Trino 0.91 GB idle, 1.45 GB peak (limit 1.5 GB); Flink TM 1.08 GB, JM 0.62 GB |
| 2026-10-20 | Disk free after Phase 5 (C:) | 20 GB; spark 1.1 GB + trino 1.38 GB images; data/warehouse 403 MB |
| 2026-10-21 | Final gate run (local data, one run id, 14 stages) | parse 8 kept hours 294,072 events 34.5 s (8.5k/s); rollup 832,958 rows 4.2 s; dbt build 19.9 s; produce 2,363,426 records 46.1 s (51k/s); Flink batch + fold 93.6 s (records not reported); publish 3,399,621 rows 7.8 s |
| 2026-10-31 | Match rate on the backfilled warehouse (publish 41186539823, ADR-085) | packages: 17,186 in scope (every ecosystem's critical list plus full OSV, not the 4,736 PyPI and npm of 2026-10-22, so the rates are not comparable), 13,743 decided (80.0%), auto_exact 273, 650 package candidates and 38 ties left; names: the 3,976 suggested are now 0 suggested (ids: ecosystems_current 9,146, ecosystems_previous_name 654, gharchive_unique_name 8, not_seen 223, ambiguous 1) |
| 2026-10-31 | Lineage graph with the score and site hops (ADR-085) | 121 nodes, 140 edges, 294 column edges |
| 2026-10-21 | Lineage graph (metadata/lineage.json) | 103 nodes, 118 edges, 294 column edges; dbt-ol 76 events, column lineage on 17 models, 61 unresolved column inputs dropped |
| 2026-10-21 | Gate path present | static JSON (column chain complete), Marquez (13 of 13 nodes, 6 of 6 fields), DataHub (5 of 5 upstream datasets, 5 of 5 column hops) |
| 2026-10-21 | Completeness | 19/19 models, 483/483 columns, 44/44 datasets, 0 owners missing |
| 2026-10-21 | Grafana vs pipeline_metrics.json | 50 of 50 equal (tolerance 0.001); 15 engines with versions |
| 2026-10-21 | DataHub ingestion | dbt 451 events, iceberg 261, kafka 6, 0 failures; 57 assertions; 53 datasets tagged and owned |
| 2026-10-21 | Phase 6 RAM (docker stats) | DataHub GMS 1.58 GB, OpenSearch 0.94 GB, Postgres 37 MB; Marquez 300 MB + db 58 MB; collector 31 MB, Prometheus 42 MB, Grafana 296 MB |
| 2026-10-21 | Disk free after Phase 6 (C:) | 8.4 GB (df); 17.8 GB after cleanup and Docker disk compaction |
| 2026-10-21 | Registry fetch (first run) | 4,736 ecosyste.ms lookups in 48 bulk requests + 2,334 npm GETs; 2,388 requests, 664 MB, 2,697 s at policy rates; 4,585 found (135 more after the NuGet key fix) |
| 2026-10-21 | Critical sets (ecosyste.ms) | PyPI 522, npm 2,295 |
| 2026-10-21 | Phase 7 match rate | 4,383 of 4,736 in scope (92.5%); auto_exact 276; accepted 4,107; suggested candidates 155 |
| 2026-10-21 | npm field vs ecosyste.ms | same 2,311; same id after rename 2; differ 1; no repository 20 |
| 2026-10-21 | Names without ids (E0/E1) | 194,416 events: 6,684 auto_exact, 10 accepted, 35,997 suggested, 151,725 no candidate in the sample |
| 2026-10-22 | Language-model rung (OpenRouter, free model) | 32 ties asked: 18 chosen (confidence 0.85 to 0.99), 14 cut off (token cap, fixed) then free daily cap 50 reached; cost $0; match rate 92.9% (4,401 of 4,736) |
| 2026-10-22 | Language-model rung, second run (new free-tier account) | 14 asked, 14 chosen; overall 29 of 32 ties decided (confidence 0.80 to 0.99); cost $0; match rate 93.2% (4,412) |
| 2026-10-22 | Review of the CSVs (seed decisions) | packages: 44 accepted + 4 rejected decisions, match rate 94.1% (4,456 of 4,736), 25 candidates left; names: 78 accepted (396 name-hours, 763 events), 3,976 left |

## Phase 10 prep (branch `claude/quirky-satoshi-cb4d8c`, written in parallel with Phase 7)
Score-independent half of Phase 10, merged 2026-10-22 (ADR-040 to ADR-043).
- Done: `keystone profile check [--run]` (probes Redpanda, its registry, Flink, Lakekeeper,
  RustFS; runs the rollup in `full` or falls back to `free`; `produced_by` in the run log and
  `pipeline_metrics.json`, ADR-040); `ingest.yml` (6-hourly) and `publish.yml` (daily skeleton),
  CLI only, failure issue, off until repo variable `KEYSTONE_SCHEDULES=true` (ADR-041);
  `release.yml` (Syft SBOM, SHA256SUMS, draft release; attestations when `KEYSTONE_PUBLIC=true`,
  ADR-042); zizmor in CI; `contracts/export_envelope.schema.json` and the schema check in CI
  (ADR-043). Measured: fixture hours with no stack, 5/5 probes down, free rollup 303 events,
  `produced_by=free, fallback=true`.
- (Done 2026-10-23, see Done and ADR-060 to 064; leftovers in Next.) Was remaining after Phase 8: site exports (radar, project pages) in the envelope with
  their row schemas, and their push from `publish.yml`; narration; the warehouse reading the
  `full` rollup when `produced_by=full`; atomic Iceberg publish (ADR-029); Airflow DAGs calling
  the CLI and their lineage; push of the compressed OpenLineage log with the live refresh;
  Actions-minutes reading for `cost.free_quota`; replace the cache-as-store with the Phase 9
  storage decision; measure workflow minutes on the first dispatch (ADR-041 has estimates).
- Owner, when ready: secret `KEYSTONE_ACTOR_SALT` (Phase 9; the workflows also use it to
  encrypt the data cache, since the repo is public), variable `KEYSTONE_SCHEDULES=true` to start
  the schedules, variable `KEYSTONE_PUBLIC=true` to switch on release attestations, a `v*` tag
  to cut a draft release.

## Phase 9 (branch `claude/peaceful-sanderson-c1e824`, PR #1): backfill, interim 2026-10-22
- Done: `keystone backfill plan|chunk|totals` and `backfill.yml` (ADR-050): 12-month incident
  windows and a 12-month live window (owner, 2026-10-22), 1,574 days in 52 monthly chunks run one
  at a time on Actions. Each chunk fetches politely, parses in parallel with per-hour partial
  rollups (merged exactly) and publishes key-free `repo_hour`, `contributors` and a JSON record
  to the release `data-<window>`. Tests: 199 pass locally; PR CI 5 of 5 green.
- Blocked on the owner (nothing has run yet, so there are no processed totals):
  1. Repo secret `KEYSTONE_ACTOR_SALT` is not set (gh as azzammasood, 2026-10-22). Git Bash:
     `gh secret set KEYSTONE_ACTOR_SALT -R azzammasood/keystone-pipeline --body "$(openssl rand -hex 32)"`
  2. Merging PR #1 into main (workflow_dispatch needs the workflow on main); the agent's
     merge was refused by the session's permission check.
- Next, once both are done: `gh workflow run backfill.yml -R azzammasood/keystone-pipeline -f max_chunks=1`
  to measure one chunk, then resize `chunk_days` if needed and dispatch the rest. Record the
  first chunk's rates in ADR-050 and `docs/benchmarks.md` (month rung), and the totals here from
  `keystone backfill totals`. `KEYSTONE_SCHEDULES` stays unset until the backfill is done.
- Bench rungs: 1 hour and 1 day on DuckDB, Spark and Flink are measured (benchmarks.md section 1).
  The month and year rungs will come from chunk records (DuckDB, end to end on the runner).
  Skipped: Databricks (no token), Snowflake (owner has not said yes), Spark and Flink beyond 1 day
  (laptop RAM; DuckDB is the production engine).
