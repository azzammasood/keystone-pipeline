# Progress

Resume point for any new session. Updated at every gate and before long runs.

## Now
Phase 1 (spike) written: `docs/spike-report.md`, `era-map.md`, `data-audit.md`; ADR-009 to 014.
Phase 0 is pushed; its CI result is unconfirmed (no `gh` on Keystone repos; owner to check).

## Done
- 2026-10-20 Phase 0 local: repo initialised (`main`), identity set repo-local (ADR-002/003),
  uv project with `keystone` CLI (`info`, `machine`), policy config, tests (6 pass), ruff + mypy
  strict clean, pre-commit, CI, Scorecard, Dependabot, CLAUDE.md, PLAN, DECISIONS, Roshni patterns.

- 2026-10-20 Phase 0 pushed to `origin/main`; review in `docs/reviews/phase-0.md` (1 major, 4 minor, all fixed).
- 2026-10-20 Phase 1 spike: 33 sample hours (1.31 GB downloaded, all deleted), 822-hour HEAD sweep,
  OSV, ecosyste.ms, deps.dev, HF mirror, tooling versions. Oracles frozen (tests/test_oracles.py).

## Next
1. Phase 1 independent review -> `docs/reviews/phase-1.md`.
2. Phase 2 ingestion (hourly manifest, resumable, gap and late-file detection, OSV incremental).

## Blockers (hard stops)
- CI status on GitHub not observable here (the brief forbids `gh` on Keystone repos).
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
