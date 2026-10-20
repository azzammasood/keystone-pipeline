# Progress

Resume point for any new session. Updated at every gate and before long runs.

## Now
Phase 0 (repo, tooling, hardening): local work done; **waiting on owner** for the GitHub repo.

## Done
- 2026-10-20 Phase 0 local: repo initialised (`main`), identity set repo-local (ADR-002/003),
  uv project with `keystone` CLI (`info`, `machine`), policy config, tests (6 pass), ruff + mypy
  strict clean, pre-commit, CI, Scorecard, Dependabot, CLAUDE.md, PLAN, DECISIONS, Roshni patterns.

## Next
1. Owner creates `azzammasood/keystone-pipeline` (and `keystone-site`), empty, private.
2. Add remote `git@github-azzammasood:azzammasood/keystone-pipeline.git`, `git ls-remote`, push, CI green.
3. Phase 0 independent review -> `docs/reviews/phase-0.md`; then Phase 1 spike (`docs/PLAN.md`).

## Blockers (hard stops)
- Repo `keystone-pipeline` does not exist yet (`git ls-remote`: Repository not found).
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
