# Progress

Resume point for any new session. Updated at every gate and before long runs.

## Now
Phase 2 (ingestion) gate passed and reviewed (`docs/reviews/phase-2.md`); ADR-016 to 018.
Phases 0 to 2 pushed; `gh` allowed with an identity check (ADR-015).

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

## Next
1. Phase 3 parsing: plug parsers into `ingest_hours(on_file=...)` (Parquet + quarantine, era-aware,
   ADR-013 privacy at read time); refine completeness boundaries from parsed counts.
2. Backfill (Phase 9) still waits on the owner (disk below 50 GB). Do not start it.

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
