# Progress

Resume point for any new session. Updated at every gate and before long runs.

## Now
Phase 7 (entity resolution) gate checks pass locally (2026-10-21): match-rate report
`docs/match-rate.md` measured on the local data: 4,736 packages in scope, 4,383 decided
(92.5%), 276 auto_exact (registry and OSV GIT range agree); renames from GH Archive (3,088)
and ecosyste.ms (1,207); names without ids: 6,694 of 194,416 events decided, 35,997
suggested (sample hours only). Review and CI (green on 00e32fd): see Done. Language-model
rung ran on 2026-10-22 (free OpenRouter model, $0): all 32 ties answered, 29 decided at
confidence >= 0.8 (3 below stay suggested); match rate 93.2% (4,412 of 4,736). Review CSVs
reviewed (owner asked Claude, 2026-10-22): 48 package and 78 name decisions in the seed,
match rate **94.1% (4,456)**; 25 candidates and 3,976 names stay suggested for lack of
evidence (docs/reviews/phase-7.md). **Phase 7 complete**: CI green on 2ff59b1 (ci, parity,
scorecard: runs 39742783444, 39742783454, 39742783568; checked with gh as azzammasood).
CI green on 71b8043 (ci, parity, scorecard: runs 39740257893, 39740257969, 39740257966;
checked with gh as azzammasood). Next phase: **Phase 8**.

## Done
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

## Next
1. **Phase 8** (next in the brief). Phase 7 leftovers: re-run
   the 3,976 names still suggested may resolve once Phase 9 loads the missing hours (re-run
   `warehouse build` and `resolve report` then);
   fold resolved E1/E0 events into activity metrics as a new metric version (Phase 8).
   Earlier leftovers: Phase 6 leftovers: Airflow lineage when the DAGs exist
   (Phase 10); push of the compressed OpenLineage log to a data branch with the live refresh
   (Phase 10); the site export adds one hop to the lineage graph (Phase 8); Databricks and
   Unity Catalog lineage when a token exists. Phase 5 leftovers: Databricks target when a token exists
   (ADR-030); atomic Iceberg publish (ADR-029); E1 2012 repositories without ids are resolved by
   name in Phase 7 (done, ADR-037). Known Phase 4 limit: a re-parsed hour already streamed needs a
   Flink batch rebuild (ADR-026).
2. Owner: set the production `KEYSTONE_ACTOR_SALT` as a secret before any non-sample parse (the
   local one in `.env` is for samples only; changing it changes every actor key). Owner
   2026-10-21: deferred to Phase 9; ask then and give the one-line command.
3. Backfill (Phase 9) still waits on the owner (disk below 50 GB; owner 2026-10-21: continue
   with Phases 7-8, decide storage before Phase 9). **Owner decisions 2026-10-21:** keep a
   30-day silver window plus aggregates for all downloaded history; download only the
   minimum history (about 24 months before each backtest incident, plus the live trailing
   window); the backfill runs on GitHub Actions; the owner makes the repo public
   themselves; curated data is stored as GitHub Release assets. Silver for the whole archive is an
   estimated ~490 GB (estimate: 85 MB for 2026-10-19 x ~5,700 days; earlier years are smaller,
   so it is an upper-side figure). Measure silver per year from the sample hours first, then
   design for the space there is: the owner cannot attach another drive (2026-10-21) and
   asks to use GitHub's storage where possible. Phase 9 options to measure and decide (ADR):
   keep silver only for a rolling window and the rollups for all of history (stream, roll up,
   delete); curated Parquet snapshots as GitHub Release assets (under 2 GiB each; check
   GitHub's terms on Releases as data storage first); never bulk data in the git history (repo
   size limits). Credentials: the owner provides them when a phase needs them; ask then.
   Do not start it. Before it:
   learn drift from a stratified sample, measure late pushes (ADR-022), parallelise parsing.
   *2026-10-21 (Phase 9 prep branch):* measured. `keystone bench run|storage`,
   `docs/benchmarks.md`, proposal ADR-039. Silver for
   the full archive is an estimated ~760 GB (635 GB to 1.03 TB), so the 490 GB figure above
   was low; no 24-month window fits the disk either. Owner decided 2026-10-21 (incident
   windows + live window only, 30-day silver, Actions, Release assets); ADR-039 sizes it:
   3.8 TB downloaded, ~51 GB published, 138 jobs of at most 5 h (estimates). Open: the
   30-day silver window as public rows without actor fields (privacy check with the owner).

## Blockers (hard stops)
- GH Archive completeness after 2025-05-23 (ADR-012): the owner should know activity signals end there.
- **Disk: 28.2 GB free on C: (below the brief's 50 GB threshold).** Phase 1 spike is fine; any
  backfill or full Compose stack needs more space or another drive.
- **RAM: 16.8 GB total, ~4.4 GB available at measurement.** Full-profile stacks must run one
  Compose profile at a time.
- Owner 2026-10-21: will provide a Databricks Free Edition token, a BigQuery sandbox project
  and an OpenRouter key (values go in `.env` only, never in chat or git). Until they appear:
- Optional rungs skipped until tokens exist: Databricks (Phase 5 rung: dbt-databricks, Unity
  Catalog, Lakeflow pipeline; logged 2026-10-20, ADR-030), BigQuery sandbox, OpenRouter.
- **Disk: 17.8 GB free on C: (2026-10-21, after cleanup and compaction).** Removed (owner
  asked): the DataHub images, containers and volumes (re-created from the pinned digests by
  `docker compose --profile datahub up` and `keystone lineage datahub`), three untagged
  Flink/Spark builds and the Phase 4 replay copy of 2026-10-19 (`data/replay`, also in
  `data/silver`). Docker's WSL disk was then trimmed and compacted (`Optimize-VHD`, elevated
  with the owner's UAC approval): 33.1 GB to 26.9 GB. Images of other projects (roshni,
  mqtt-rawevent-batch, bigqueryjob*, SAM build, Redpanda Console) were not touched. No other
  drive can be attached (owner): Phase 9 designs for this space (Next, item 3).
- **Owner: disk.** 20 GB free after Phase 5 (Spark and Trino images 2.5 GB). An untagged
  1.07 GB Flink image from an earlier build is reclaimable (`docker image prune`); freeing disk
  is the owner's call.

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
- Remaining for Phase 10 after Phase 8: site exports (radar, project pages) in the envelope with
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
