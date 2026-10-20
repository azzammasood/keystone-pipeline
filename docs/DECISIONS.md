# Decisions (ADRs)

Newest first. Each entry: context, options, choice, why, date. Scope changes are recorded only here.

## ADR-015 (2026-10-20) gh CLI allowed (owner)
- Owner amended the brief: `gh` may be used on Keystone repos (CI status, runs, issues, PRs).
- Guard: run `gh api user --jq .login` first; proceed only if it prints `azzammasood` (the keyring
  also holds azzamverse). Still never `gh repo create`, visibility or settings changes without asking.

## ADR-017 (2026-10-20) OSV increments: head of modified_id.csv plus ecosystem zips as baseline
- Context: `modified_id.csv` is 151 MB, sorted newest first, and is rewritten often (its ETag
  changed within an hour on 2026-10-20). `all.zip` is 2.78 GB.
- Choice: read the head in 256 KB Range chunks until the stored cursor (nanosecond timestamp
  plus the keys at that timestamp, so ties are handled), using `If-Range` on the first chunk's
  ETag and restarting (at most 3 times) if the file changes mid-read. On the first run, read at
  most 1 MB; the per-ecosystem `all.zip` files (conditional GET, MD5-verified, kept) are the
  baseline. Increments are written to Parquet, named by their modified range; unparseable lines
  are quarantined.
- Measured: the first run read 1 MB (19,144 rows); later runs read one chunk.

## ADR-016 (2026-10-20) GAP period starts at 2025-10-08 17h (measured)
- A Phase 2 HEAD-only scan found that 2025-10-08 17h, 18h and 19h are already near-empty
  (166 to 250 KB), while 16h was 97 MB (era-map). `config/sources.yml` now starts GAP at 17h
  instead of the bracketed "about 20h". Spike oracles are unchanged; spike-report section 4
  carries a dated note.

## ADR-015 (2026-10-20) Ingestion manifest in DuckDB; stream and delete by default
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
