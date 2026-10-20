# Roshni patterns reused in Keystone (one page)

Source: local `..\roshni\` (pipeline + `web/`, which is published to the public `roshni-site` repo).
Patterns are reused; no Roshni-specific code or content is copied.

| Roshni pattern | Where in Roshni | Keystone use |
| --- | --- | --- |
| One Typer CLI; orchestrators only call commands | `src/roshni/cli.py`, `airflow/` | `keystone` CLI; Actions + Airflow call it, no logic in DAGs |
| `paths.py` with env-overridable root, pathlib only | `src/roshni/paths.py` | `src/keystone/paths.py` (`KEYSTONE_ROOT`) |
| Polite client: fixed UA, robots.txt, throttle, retries, host denylist, injectable transport/clock for tests | `fetch/http.py` | Phase 2 fetcher; limits come from `config/policy.yml` (GH Archive 2 concurrent, ~2 req/s) |
| Raw snapshots with sha256 + fetched_at, `raw_snapshots.csv` manifest, change detection (new/unchanged/known) | `fetch/snapshot.py`, `store.py` | Hourly-file manifest (url, hour, status, etag, last-modified, bytes, sha256, fetched_at) |
| Parsers return pydantic models -> Parquet + quarantine with reasons | `parsers/run.py` | Era-aware parsers, quarantine, schema-drift detector |
| Great Expectations on parsed tables, dbt tests on models, no duplication | `quality/suites.py`, `dbt/` | Same split, plus ODCS contracts via datacontract-cli |
| dbt SQL portable to Databricks (Phase 3b ran unchanged) | `dbt/` | dbt-duckdb + dbt-databricks targets |
| Pydantic export models generate JSON Schema (`schemas/v1`), versioned export | `export/models.py`, `schemas/v1` | Versioned export incl. `pipeline_metrics.json` |
| Measured numbers in `docs/data-audit.md` are regression tests | `docs/data-audit.md` | Phase 1 oracles frozen as tests |
| CI: uv sync --frozen, ruff check/format, pytest, dbt parse (no data in CI) | `.github/workflows/ci.yml` | Same plus mypy, gitleaks, pip-audit, Scorecard |
| Scheduled publish; failure opens/updates a `pipeline-failure` issue, closed after a good run | `publish.yml`, `.github/scripts/failure-issue.sh` | Same for ingest and publish (Phase 10) |
| pre-commit: large-file cap 500 KB, private-key detection, block data folder | `.pre-commit-config.yaml` | Same plus gitleaks and mypy |
| Short CLAUDE.md, long facts in docs, DECISIONS newest-first | `CLAUDE.md`, `docs/` | Same; DECISIONS as ADRs |
| LLM narrates prose only; numbers injected and validated; spend capped | `narrate/` | Weekly risk brief via OpenRouter |
| Portfolio layer: How it's built, dbt docs at `<site>/dbt-docs/`, maker page, freshness badge | `web/` | Site phase, later |

Lessons carried over (brief section 4): claims audit before launch; scale claims only with exact
counts, bytes and date ranges; every scope addition logged in DECISIONS.md.
