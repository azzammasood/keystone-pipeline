# Keystone pipeline: working notes for Claude

Open-source supply-chain risk radar on GH Archive + OSV + a package-to-repo mapping. Backend only
in this repo; the site (`keystone-site`) comes later. The full brief lives outside git; its rules
are summarised here. Long facts go in `docs/` (`spike-report.md`, `era-map.md`, `data-audit.md` from Phase 1;
`benchmarks.md` from Phase 9). Decisions: `docs/DECISIONS.md` (ADRs). Status and resume point: `docs/PROGRESS.md`.

## Hard rules
- **Privacy:** repository and package level only. Never publish, rank or label people. No raw logins
  in published tables; silver tables use a salted hash of `actor.id` (salt `KEYSTONE_ACTOR_SALT`, env
  only). Never extract emails. Filter bots (`[bot]` suffix + maintained list). Aggregate shares only.
- **Wording:** "signals", never "will be abandoned". No shaming, no predictions. Corrections path honoured.
- **Polite client:** UA and limits from `config/policy.yml`. GH Archive: at most 2 concurrent, ~2 req/s.
  Check `robots.txt` for every host. Conditional requests where possible.
- **Never invent data or numbers.** Unparseable records go to quarantine with a reason.
- **Eras:** payloads differ before/after 2025-10-07 (and pre-2015). Metrics use only fields present in
  the whole period they cover (`docs/era-map.md`); version a metric at a break, never silently mix.
- **Point-in-time:** backtest features use only data available before the incident date.
- Store all timestamps in UTC.
- **Money:** $0 only. BigQuery: no billing account, always `maximum_bytes_billed`. Any possible charge
  is a hard stop. Track every free quota and stop before exceeding it. Non-commercial.
- **Tool credibility:** a tool appears in README/site only if it ran, with evidence (`engines[]` in
  `pipeline_metrics.json`). Cut tools that do no real work.
- **Measured numbers in `docs/` are regression oracles.** If a source changes, surface it; don't edit tests.
- Bulk data lives in `data/` (git-ignored), never in git. Stream and delete; stay inside the disk budget
  (`keystone machine`).

## Hard stops (ask the owner)
Owner-only actions (repos, accounts, tokens, Docker/admin installs, freeing disk); any spend; GitHub
identity ambiguity or anything under **azzamverse**; making a repo public or publishing the site;
destructive actions; privacy/legal red flags; the frontend design choice; an unmeetable requirement.
If a stop affects one rung only, skip it, log it in PROGRESS.md and continue.

## Repo and git safety
- Remote: `git@github-azzammasood:azzammasood/keystone-pipeline.git` (SSH alias in `~/.ssh/config`,
  same as Roshni). `gh` is allowed (owner, 2026-10-20, ADR-015) **only after `gh api user --jq .login`
  prints `azzammasood`**; the keyring also holds azzamverse, so check before each session's first use
  and stop if it differs. Never `gh repo create`, never change repo visibility or settings without asking.
- Commit identity set per repo (azzammasood, ahmaduzzammasood@gmail.com). Never touch global git config.
- Never force-push without asking. Private until the owner says go public. Never commit secrets (`.env`).
- Small conventional commits (`feat:`, `fix:`, `build:`, `ci:`, `docs:`, `test:`, `chore:`).

## Autonomy
Decide by: $0 > site updates forever > portfolio value (in-demand tools doing real work) > reliability
> least effort. Log non-trivial choices as ADRs. Before each gate: independent review in a fresh
subagent, write `docs/reviews/phase-N.md`, fix, rerun (max two attempts, then stop and report).
Gates auto-advance when clean. Update PROGRESS.md at every gate and before long runs.

## Commands (PowerShell or Git Bash)
```
uv sync ; uv run pre-commit install       # install (Python 3.12) + git hooks
uv run keystone --help
uv run keystone machine [--json]          # disk, RAM, CPUs, Docker, disk budget
uv run keystone ingest gharchive --start 2026-10-19 [--end ..] [--keep] [--head-only]
uv run keystone ingest gaps --start .. [--end ..] [--json]   # gaps, late, near-empty + period
uv run keystone ingest osv [--ecosystem PyPI] [--no-zips]   ; uv run keystone ingest status
uv run keystone parse gharchive --start .. [--end ..]     # parse kept raw hours (needs KEYSTONE_ACTOR_SALT)
uv run keystone parse files <hour.json.gz>... --root DIR  # local files (fixtures, CI)
uv run keystone parse learn-drift --start ..              # add kept hours to the drift registry; review diff
uv run keystone quality contracts|gx [--root DIR]        # ODCS contracts (datacontract-cli) / GX suites
uv sync --all-groups                      # includes the `quality` group (datacontract-cli, GX)
uv run pytest                             # network tests skip unless KEYSTONE_NETWORK_TESTS=1
uv run ruff check . ; uv run ruff format .
uv run mypy
uv run pre-commit run --all-files
```

## Layout
```
src/keystone/            cli.py (Typer), paths.py, policy.py, machine.py
  fetch/ sources/        polite client + DuckDB manifest; GH Archive + OSV ingestion (Phase 2)
  parsers/ quality/      era-aware parser, privacy, drift, Parquet sink; contracts + GX (Phase 3)
  planned subpackages:   eras rollup resolve metrics score backtest bench
                         profile lineage telemetry narrate export (created when a phase needs them)
config/                  policy.yml (UA, limits, disk budget); later sources.yml, incidents.yml
contracts/               ODCS contracts (*.odcs.yaml) and drift/gharchive.yml (learned registry)
dbt/ metadata/ compose/  (filled in from Phase 4 on)
tests/                   tests/fixtures/ (small, committed), tests/parity/ (Flink vs DuckDB)
docs/                    PLAN, PROGRESS, DECISIONS, roshni-patterns, reviews/
data/                    generated, git-ignored
```

## Conventions
- Python 3.12, uv, ruff (line 100), mypy strict on `src/`, pytest. pathlib only.
- Parsers return pydantic models -> Parquet + quarantine. Every source-derived table keeps
  `source_file`, `source_sha256`, `fetched_at`.
- Rollup logic written once in portable SQL; parity tests between engines fail CI on any diff.
- dbt SQL stays portable (DuckDB and Databricks). Every model and column documented (>= 95% gate).
- GitHub Actions = production; Airflow = local showcase calling the CLI only (no logic in DAGs).
- Compose profiles (`core`, `streaming`, `lineage`, `catalog`, `obs`, `bench`): start only what a
  phase needs; measure RAM first (machine has ~16 GB total).
- Owner is on a limited plan: no large dumps into the conversation; one phase per session.
