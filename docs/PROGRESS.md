# Progress

Resume point for any new session. Updated at every gate and before long runs.

## Now
Phase 4 (rollup engines) gate checks pass (parity on fixtures and on a one-day replay); ADR-024 to
027. Review and CI result: `docs/reviews/phase-4.md`. Next phase: **Phase 5** (see Next).

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
  (854,442 late contributions folded). CI: `parity.yml` (path-filtered, weekly).

## Next
1. **Phase 5** (next in the brief): Iceberg + REST catalog are running (ADR-027); dbt on dbt-duckdb
   over the rollups, `dim_repo`, `fct_repo_day`, metric models per era; Spark reads the same tables.
   Known Phase 4 limit: a re-parsed hour already streamed needs a Flink batch rebuild (ADR-026).
2. Owner: set the production `KEYSTONE_ACTOR_SALT` as a secret before any non-sample parse (the
   local one in `.env` is for samples only; changing it changes every actor key).
3. Backfill (Phase 9) still waits on the owner (disk below 50 GB). Do not start it. Before it:
   learn drift from a stratified sample, measure late pushes (ADR-022), parallelise parsing.

## Blockers (hard stops)
- GH Archive completeness after 2025-05-23 (ADR-012): the owner should know activity signals end there.
- **Disk: 28.2 GB free on C: (below the brief's 50 GB threshold).** Phase 1 spike is fine; any
  backfill or full Compose stack needs more space or another drive.
- **RAM: 16.8 GB total, ~4.4 GB available at measurement.** Full-profile stacks must run one
  Compose profile at a time.
- Optional rungs skipped until tokens exist: Databricks, BigQuery sandbox, OpenRouter.

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
