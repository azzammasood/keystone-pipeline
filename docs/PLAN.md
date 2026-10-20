# Plan: Phase 0 and Phase 1

## Phase 0: repo, tooling and hardening
1. Identity: inspect SSH config, Roshni remote, `ssh -T` (done; ADR-002/003).
2. Read Roshni patterns; one-page summary in `docs/roshni-patterns.md`.
3. Scaffold: `git init -b main`, `.gitignore` before first commit, `.gitattributes`, MIT, `.env.example`,
   uv project (Typer CLI `keystone`, pydantic, ruff, mypy, pytest), `config/policy.yml`.
4. Quality: pre-commit (ruff, mypy, gitleaks, large-file, data/ block), CI (lint, format, types,
   tests, gitleaks, pip-audit), Scorecard workflow, Dependabot (uv + actions).
5. Docs: `CLAUDE.md`, `DECISIONS.md`, `PROGRESS.md`.
6. Commit; prove nothing large or secret is tracked (`git ls-files`, `git status --ignored`).
7. Remote (blocked on owner creating `keystone-pipeline`): add SSH remote, `git ls-remote`, push, CI green.
8. Independent review -> `docs/reviews/phase-0.md`.
- Gate: CI green on GitHub, `git remote -v` correct, nothing large/secret tracked, patterns summary.

## Phase 1: feasibility spike (read-only, small downloads, $0)
Outputs: `docs/spike-report.md`, `docs/era-map.md`, `docs/data-audit.md`, frozen oracles in tests.
1. Hosts: `robots.txt`, response headers (server/CDN, ETag, Last-Modified), HEAD timing, polite limits.
2. GH Archive sample hours across eras: 2011 (first hour search), 2012-2014 (old timeline format),
   2015, 2019, 2023, 2025-10-06 vs 2025-10-08 (Events API change), latest day. Per hour: bytes,
   events, event-type mix, parse throughput. Payload field inventory per type per era -> era map.
3. Size model: bytes and events per day across years (HEAD Content-Length on a sparse sample, not
   full downloads) -> total size estimate, labelled as estimate.
4. GH Archive data terms; OSV over HTTPS (`all.zip`, per-ecosystem, `modified_id.csv`), licenses.
5. Package-to-repo mapping: deps.dev (API/BigQuery dataset), npm/PyPI registry `repository`
   fields, ecosyste.ms: terms, rate limits, coverage on a sample of packages.
6. Hugging Face `open-index/open-github`: provenance, license, count comparison vs GH Archive on
   sample hours.
7. BigQuery sandbox (only if owner gives a project): bytes per column set via dry runs, export path.
8. Databricks Free Edition (only if a token exists): upload, Iceberg/Delta, quota behaviour.
9. Tooling: Flink latest stable + memory, Redpanda vs Kafka, Iceberg REST catalog (Polaris vs
   Lakekeeper), S3-compatible store, OpenLineage support matrix, DataHub resource needs vs this
   machine (16 GB RAM, 8 CPUs), GitHub Actions runner limits, free stores (R2, Releases, HF).
10. Scale decision rule (automatic): largest $0 window with <= 7 days unattended resumable wall
    time, minimum 12 months; ADR. Freeze measured numbers as regression oracles.
- Constraint: free disk is ~28 GB (budget 19.7 GB). The spike needs well under 1 GB; any backfill
  must stream-and-delete, and the owner is asked about freeing space (brief: below 50 GB).
