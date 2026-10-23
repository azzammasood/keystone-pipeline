# Decisions (ADRs)

Newest first. Each entry: context, options, choice, why, date. Scope changes are recorded only here.

## ADR-064 (2026-10-23) Phase 10 close: switches on, rungs skipped, what waits on the backfill
- **Switches (owner 2026-10-22/23: "don't stop for decisions"):** repository variables
  `KEYSTONE_SCHEDULES=true` (ingest every 6 h, publish daily) and `KEYSTONE_PUBLIC=true`
  (release attestations, ADR-042), set with `gh variable set` after `gh api user --jq .login`
  printed `azzammasood`, once the guarded workflows were dispatched and green. No other
  repository setting was changed.
- **Self-hardening, as it stands:** Scorecard (weekly and on push, ADR-006), zizmor on every
  workflow in CI, actions pinned by SHA and dependencies by `uv.lock` (`uv sync --frozen`
  everywhere), Dependabot, gitleaks, pip-audit, Syft SBOM + checksums + attestations on `v*`
  releases (ADR-042).
- **Skipped (logged, not built):** Databricks keepalive (no Databricks account); Tier 2
  Terraform for repository settings and R2 (no R2 account; repository settings are the owner's)
  and Elementary (dbt tests and the contracts already gate; not trivial). The warehouse reading
  the `full` rollup when `produced_by = full`, the atomic Iceberg publish (ADR-029) and pushing
  the compressed OpenLineage log to a data branch: `full` never runs in production (no stack on
  runners, ADR-040), so these are showcase-only and stay listed in PROGRESS.
- **Waits on the backfill:** the backtest rerun (`backtest.yml`), the first real ingest
  (it yields while a backfill run is queued or running, ADR-062), and so the first published
  `site-data`. Until then the radar says "partial data" and the backtests "pending data".

## ADR-063 (2026-10-23) Airflow: a local DAG that only calls the CLI, lineage from the provider
- Options: a full Airflow stack (scheduler, API server, Postgres) as a compose profile; or a
  one-shot `airflow dags test` in the official image with a SQLite metadata DB. Choice: the
  one-shot (`compose` profile `airflow`, `apache/airflow:3.3.2-python3.12` pinned by digest).
  Why: production orchestration is GitHub Actions (ADR-041); Airflow shows the same CLI under
  a second orchestrator, and RAM is tight (Docker VM 8.2 GB). It installs the project into its
  own venv in the container (the host `.venv` is Windows).
- `airflow/dags/keystone_pipeline.py`: parse the fixture hours, the rollup through
  `profile check --run` (no stack in the container: falls back to free), score, metrics, site
  export, brief. No logic in the DAG; no secrets (a showcase salt, no OpenRouter key: the brief
  is skipped and says so).
- Lineage: the Airflow OpenLineage provider (2.20.1) writes task START/COMPLETE events with the
  file transport into the same events directory as the CLI's own events, so one graph holds
  both. A stage run under Airflow records `detail.orchestrator` and `services.airflow` (from
  `AIRFLOW_CTX_*`, `AIRFLOW_VERSION`), which puts Airflow in `engines[]` only when it ran.
- Measured 2026-10-23: `docker compose --profile airflow run --rm airflow` 66 s wall (venv
  install included); DAG run 27.8 s, 6 of 6 tasks succeeded; 13 provider events (6 START,
  7 COMPLETE incl. the DAG run) + 4 CLI events; engines[] = airflow 3.3.2, duckdb 1.5.6,
  keystone 0.1.0, keystone-parser 0.1.0; index of 9 validated files. Image 2.36 GB on disk.
- Found by this run: `pytz` was missing from the runtime dependencies (DuckDB needs it to
  fetch TIMESTAMPTZ; the dev group had hidden it). Added.

## ADR-062 (2026-10-23) Publish wiring: exports to `site-data`, fallback issue, guards
- `publish.yml` runs, CLI only: rollup through the profile check, OSV, warehouse, contracts,
  `score live`, `score backtest`, metadata export, `metrics emit`, the schema check, `export
  site`, `narrate brief` (secret `OPENROUTER_API_KEY`; none is set, so it is skipped and says
  so), then uploads every export to the pre-release `site-data` (`gh release upload
  --clobber`, assets flat: `metadata/x.json` as `metadata__x.json`). The freshness lag, SLO and
  producer go to the run summary. `contents: write` on that job only.
- Fallback issue: after `metrics emit`, `.github/scripts/fallback-issue.sh` opens
  "[keystone] full profile unavailable: scheduled jobs ran in free" on the first fallback and
  edits its body on later ones (no daily notifications). Runners never have the stack, so the
  issue stays open while production is `free`, which is the designed state (ADR-040).
- Guards: GH Archive allows 2 concurrent requests and a backfill chunk uses both, so ingest
  skips (a notice, success) while a `backfill.yml` run is queued or in progress; the next run
  re-checks its hours with conditional requests. Publish skips until an ingest cache exists
  (the warehouse cannot build without silver; failing daily would open an issue for nothing).
- `cost.free_quota`: the Actions minutes line says the repository is public and standard
  runners are not metered (no quota to read), instead of a private-repository null.
- After review (`docs/reviews/phase-10.md`): the ingest guard counts every backfill run not
  completed; it stays check-then-act. `backfill.yml` now waits (up to 60 minutes) before each
  chunk fetches while an ingest run is not completed (2026-10-23); both guards are
  check-then-act, so a start in the same minute can still overlap briefly. The publish guard reads `main`'s caches only. The salt is set on
  the cache steps only. `index.json` rows carry the flat asset name, it is uploaded last, and
  assets not in it are deleted. `metrics emit` runs before export and brief, so those two
  stages appear in the next run's `pipeline_metrics.json`.
- CI's metadata job runs the gate path on every push: fixture hours, `profile check --run`
  with no stack (falls back), score, metrics, `export site` (every file validated), brief
  skipped, and checks `produced_by` and the index.

## ADR-061 (2026-10-23) Weekly brief: free model, numbers checked, cached by input hash
- `keystone narrate brief` builds facts from the exported JSON (radar status and top 5 rows,
  backtest outcomes, freshness target met or missed, the producing profile), asks a free
  OpenRouter model for 3 to 6 sentences, and publishes `brief.json` (dataset `brief`, v1).
- Checks before publishing: every number in the text must occur in the facts; banned wording
  (predictive: "will be abandoned/compromised", "predict", "forecast", "going to"; shaming:
  "fault", "negligen"; no URLs, no `@`); at most 1,500 characters. A refused text gets one
  retry with the reason; a second failure publishes `text: null`, outcome `rejected`.
- Cache: answers stored under SHA-256 of facts + model + prompt version; the same input never
  asks twice, and a cached text is checked again before it is republished. Rejections are not
  cached.
- After review: numbers in words, leading-dot decimals and digits glued to letters are refused;
  modal/adverb predictive forms, "likely", "abandon*", shaming adjectives, pronouns and
  "maintainer/author/owner" are banned. A model or price-check failure publishes outcome
  `error` (or `skipped`) and never fails the publish. Client retries are off, so the 2-request
  cap is exact. Residual risk: a plain person name is not detectable by pattern; the facts hold
  repository names only and the prompt forbids naming people.
- $0: only a `:free` model id, listed prices checked to be zero, every response's reported cost
  zero (the Phase 7 client), at most 2 requests per run (no retries). No key: outcome `skipped`, said in the
  file. Options rejected: placeholder templates (the brief asked for validated numbers, and
  the checks catch an invented one); a paid model (the $0 rule).
- Measured 2026-10-23 (local key, `nvidia/nemotron-3-ultra-550b-a55b:free`): 1 request per
  brief, written and passed the checks (2 prompt versions tried); the rerun was `cached` with
  0 requests; cost $0.

## ADR-060 (2026-10-23) Site exports: envelope 1.1, row schemas, metadata schemas, index
- `keystone export site` writes `radar.json`, `project_pages.json`, `backtests.json`,
  `benchmarks.json` (each one envelope), copies `pipeline_metrics.json` and the metadata export,
  and writes `index.json` (every file, its SHA-256, size and the schema that validated it).
- Every dataset's rows are checked against `contracts/<dataset>_row.schema.json` (radar,
  project_page, backtest, benchmark, brief, index); catalog, glossary and lineage against
  `contracts/metadata_*.schema.json`; pipeline_metrics against its own schema. Rows are
  `additionalProperties: false`, so a person field is refused. The schema check in CI runs
  every example (`valid_*` must pass, `invalid_*` must fail).
- Envelope 1.1 (minor, readers of 1.x keep working): optional `status` {state: ok | partial
  data | pending data | skipped, detail}. An empty radar says why, instead of looking like "no
  risk".
- Rows hold only what measured files hold: scores from `keystone score live`, names and
  mapped packages from the warehouse, backtests from `keystone score backtest`, benchmarks
  parsed from `docs/benchmarks.md` (the measured ladder, 9 runs). Component `raw` values stay
  internal; pages show each signal's value, weight and reason.
- Measured 2026-10-23 on local data after a forced outage (5 of 5 probes down): 8 files
  validated plus the brief; radar 0 rows, status "partial data: the rollup holds 0.1% of the
  window's hours; not scored"; backtests "pending data" for event-stream, colors-faker, xz.

## ADR-051 (2026-10-23) Backfill reports rare-type drift instead of failing the hour (amends ADR-022)
- Context: the first backfill chunk (event-stream, 2017-12) lost one of 746 hours: 2017-12-01
  19h holds 95 `FollowEvent` records, a type the E2 registry never saw because the stratified
  drift sample ADR-022 planned before the backfill was not taken. Every such hour would mark its
  chunk incomplete, and the owner asked to finish fast and not stop for decisions.
- Choice: `backfill chunk` parses with `drift_mode = "report"`. Unknown event types, fields and
  JSON types are written to the hour's drift report and summarised in the chunk record
  (`hours.drift`: finding -> hours seen), and the hour is kept. Era-shape drift (a record shaped
  like another era, or a push contradicting the trim instant) still fails the hour. Live ingest
  and `parse` keep the default `fail` mode.
- Why it is safe: the parser is an allow-list projection (ADR-019) validated by strict pydantic
  models, so a field or type it does not read cannot reach silver or the rollups; an unknown
  event type is rolled up under its own `event_type` key and used by no metric unless named.
- Follow-up: after the backfill, review the summed `hours.drift` of all chunks and learn the
  reviewed shapes into `contracts/drift/gharchive.yml`.

## ADR-050 (2026-10-22) Backfill: 12-month windows, monthly chunks on Actions, aggregates only (supersedes parts of ADR-039)
- **Owner decisions (2026-10-22, relayed by the Phase 4 kickoff session):** minimum data. Each
  incident window is the 12 months before the incident month plus that month (event-stream
  2017-11 to 2018-11, colors-faker 2021-01 to 2022-01, xz 2023-03 to 2024-03); the live window
  is the month 12 months back to today (2025-10-01 onward). No 30-day silver window: a
  correction re-downloads the hour from GH Archive. Publish only the rollup aggregates. The
  backfill runs on GitHub Actions (public repo, standard runners free), chunked, resumable,
  inside the 6 h job limit and the runner disk, chunks one at a time (polite limits). Curated
  output is Release assets, each under 2 GiB; never `repo_actor_day` or an actor key.
- **Superseded in ADR-039:** the 24-month lookback, the 30-day silver window (and its open
  privacy question: moot), the day-by-day job model and its sizing.
- **Windows:** `config/incidents.yml` (`lookback_months: 12`, `trailing_months: 12`), read by
  the same `design_windows` as `keystone bench storage`. 1,574 days on 2026-10-22 (395 + 396 +
  397 + 386), 52 monthly chunks. The windows are disjoint; the planner refuses overlaps.
- **Workflow** `backfill.yml` (manual dispatch only; concurrency group `keystone-backfill`):
  a `plan` job lists the assets of the `data-*` releases and runs `keystone backfill plan`,
  which emits the pending chunks as a matrix; the `chunk` job runs them with `max-parallel: 1`
  and `fail-fast: false`, timeout 350 min. Inputs: windows, `chunk_days` (split months when a
  month does not fit a job), `max_chunks`, `redo_incomplete`.
- **A chunk** (`keystone backfill chunk`, `src/keystone/backfill/chunk.py`): the polite client
  fetches the chunk's hours plus `overlap_hours` (2) past its end with a fresh manifest; each
  verified hour goes to a process pool (one per CPU) that parses it with the production salt,
  runs the two `rollup/sql` queries over that hour alone into partial Parquet (with the file
  hour), and deletes the raw file and the silver at once. A semaphore holds at most 2 hours per
  worker in flight (disk). Measures are sums and maxima, so merging partials by key is exact.
  The merge keeps windows inside `[start, end)`; events outside are counted, not guessed:
  `before_start_in_previous_overlap` (kept by the previous chunk), `after_end_next_chunk`
  (kept by the next), and `*_lost` (filed beyond the overlap; reported per chunk).
- **Published per chunk** in the release `data-<window>` (prerelease, never "latest"):
  `repo_hour-<start>-<end>.parquet` (zstd, sorted), `contributors-<start>-<end>.parquet` (per
  repository, day and era: `human_actors`, `human_events`, `top_actor_events`; no key) and
  `chunk-<start>-<end>.json`: exact counts (hours by status, missing hours, failed hours and
  their errors, bytes downloaded, events parsed and quarantined, edge counts, rows and bytes
  out, eras), wall times, peak RSS, cost $0 and the repository-level fetch manifest (URL,
  status, bytes, MD5, SHA-256, ETag, Last-Modified). The JSON is uploaded last and marks the
  chunk done; a chunk with failed or unpublished hours is published as
  `chunk-<name>.incomplete.json` and retried only with `redo_incomplete`. An asset over 1.5 GiB
  fails the chunk (rerun with `chunk_days`). `repo_actor_day` partials stay in the runner's
  temp directory, deleted at the end of the step; the runner is discarded.
- **Totals:** `keystone backfill totals --records DIR` sums the chunk records (download them
  with `gh release download data-<window> -p 'chunk-*.json'`); these feed PROGRESS.md and
  `docs/benchmarks.md`.
- **Known limits:** the live window's last chunk is named by its end day, so a later dispatch
  re-runs that month; after the backfill the scheduled ingest (ADR-041) takes over the live
  edge. Metrics that need actor identity across chunks (new versus departed contributors over
  more than a month) cannot be computed from the published files; the metrics phase uses the
  per-day counts or derives such signals inside a chunk.
- **Why:** $0; nothing bulk on this disk; minimal download (about 40% of ADR-039's days);
  parallel parsing uses the runner's 4 CPUs while the network stays at the policy's 2
  connections; per-chunk resumability needs no state beyond the release itself.

## ADR-045 (2026-10-22) Encrypted `repo_actor_month` assets; decrypt only on the backtest runner
- **Context:** concentration and churn need per-actor monthly counts over 12 months (ADR-044);
  the backfill jobs hold only a chunk of days, and actor keys are never published. Owner
  (2026-10-22, relayed by the kickoff session): store `repo_actor_month` as an AES-256-encrypted
  release asset under its own secret `KEYSTONE_ACTOR_DATA_KEY`; do not stop for decisions, take
  the recommended option and log it.
- **Choice:** one shared form for both sides, `.github/scripts/actor-data.sh`:
  `openssl enc -aes-256-cbc -pbkdf2 -iter 600000 -salt` (the data-cache.sh form) with the
  passphrase `KEYSTONE_ACTOR_DATA_KEY`. The backfill (Phase 9) uploads
  `repo_actor_month-<chunk>.parquet.enc` to `data-<window>` with `actor-data.sh encrypt`. The
  manual workflow `backtest.yml` downloads each incident window's `repo_hour-*.parquet` and the
  `.enc` assets, decrypts them on the runner into `data/backtest/repo_actor_month/`, runs
  `keystone score backtest --rollup data/backtest`, and uploads only `backtest.json` and
  `backtest.md` (counts and shares, no actor keys). A window without assets is pending data;
  without the encrypted table, concentration and churn are unavailable for that window.
- **Options rejected:** a Python cipher dependency (openssl is on every runner and matches the
  existing cache form); reusing `KEYSTONE_ACTOR_SALT` as the passphrase (one leak would expose
  both the keys' salt and the table; the owner chose a separate secret); uploading the
  decrypted table as an artifact (artifacts of a public repository are downloadable).
- **Limits:** the workflow has no warehouse, so OSV advisories and the name history (faker.js
  has no API id) are unavailable there; a local run with the warehouse fills both. The round
  trip is tested on Linux CI (`test_actor_data_roundtrip`).

## ADR-044 (2026-10-22) score_v1: rule-based fragility x criticality, point-in-time backtests
- **Context:** brief section 8 (candidate signals, risk = criticality x fragility, no learned
  score in v1, backtests on `config/incidents.yml`, misses published, never claim prediction).
  Era rules (era-map.md), completeness periods (ADR-012), privacy (no actor keys published) and
  the owner's 12-month windows (2026-10-22) constrain what each signal can be.
- **Choice (`src/keystone/score/`, `keystone score live|explain|backtest`):**
  - *Cut and window.* A cut is the first instant of a UTC month; the window is the 12 months
    before it, half-open. Per-actor input is monthly, so a month counts only when it ends by the
    cut. Backtest cut = first instant of the incident month: nothing from that month is read,
    whatever the disclosure day (incident months only are recorded, no invented days).
  - *Components* (sub-score 0..1, 1 = most fragile; weight): maintenance-actor concentration,
    top-1 share (top 3 shown) of maintenance actions (0.30); contributor churn, share of the actors
    with maintenance actions in months 1-6 that have none in months 7-12, new ones shown (0.15); activity trend, last 3
    months' monthly human events against months 1-9 (0.15); responsiveness proxy, issue + PR
    closes per open, bots included (0.10); release recency, days since the last GitHub release / 365 (0.10);
    collaborators added in the last 6 months, unavailable in E0 (0.05); OSV advisories published
    before the cut and not withdrawn by it, malicious-package records left out, high/critical 1 and others 0.5, saturating at 5 (0.15).
    Fragility = 100 x weighted mean of the available components; withheld below 50% of the
    weight. Every component carries its raw inputs and a one-line reason (or why unavailable).
  - *Eras:* maintenance actions are `maint_v1` (push, merged PR, release, issue close) in E1-E2
    windows, `maint_v2` (PR close in place of merged PR) in E3 windows, and `maint_v0` (push,
    release, issue close) in windows that mix eras or touch E0. The version is in every
    explanation. The activity trend is unavailable when the window crosses a completeness
    period (the live window does: GAP, C3 to C5), so raw counts are never compared across them.
  - *Not in v1 (no point-in-time input):* issue and PR first-response time (rollups keep no
    per-issue timing; the closes-per-open proxy stands in, named as a proxy); OSV time to fix
    (no fix timestamps; `has_fixed` is today's state, so it is not used at all).
  - *Criticality* = log10(1 + dependent repositories) / 6, capped at 1, from ecosyste.ms via
    the accepted/auto_exact package mapping. It is today's snapshot, so it is used in the live
    score only: **backtests report fragility, not risk.** Risk = criticality x fragility (0-100).
  - *Backtest:* a point is scored only when the rollup holds >= 90% of the window's hours, else
    and of its last 3 months, else "pending data" (none) or "partial data". The incident repository is *flagged* when its
    fragility is at or above the 90th percentile of active repositories (>= 100 human events)
    at the same cut (the targets always join that population); otherwise the outcome is a **miss**, published as such. Repository ids
    come from the GitHub API (recorded in the config) or, for a removed repository, from the
    name history before the cut.
  - *Leakage:* every query bounds event time by the cut; the run itself rescores the incident
    repositories on inputs truncated at the cut and fails (exit 1) on any difference (a check
    of the time filters). Known snapshot inputs, not point-in-time: the package mapping, OSV
    severity labels and alias sets (aliases of one flaw can count twice). The cut is the
    disclosure month, which can follow the start of a compromise; the report says so. Tests
    poison the data with rows at and after the cut (activity, actors, advisories,
    withdrawals) and show the scores unchanged, and that the check catches a leaky signal.
  - *Sensitivity:* 22 weight variants (each component x0.5, x1.5, dropped), reported as the
    incident repository's percentile range and the number of variants that flag it.
  - *Inputs:* `repo_hour` (published) and `repo_actor_month` (internal, salted keys: per-actor
    maintenance counts per month; built from `repo_actor_day` when only that exists). Phase 9
    decides how the backfill provides `repo_actor_month` without publishing actor keys; without
    it, concentration and churn are unavailable for that window (reported, not guessed).
- **Options rejected:** a learned score (brief: none in v1); mixing E2 and E3 maintenance
  definitions in one window (era rule); using today's dependents or fix status in backtests
  (leakage); daily "largest actor" aggregates for concentration (do not compose across days).
- **Why:** transparent rules a reader can check, honest about missing inputs, and no event
  or advisory from after the cut (snapshot inputs are named, not hidden). Weights are judgement, not fit; the sensitivity
  check shows how much each outcome depends on them.

## ADR-043 (2026-10-21) Versioned export envelope; every JSON Schema checked in CI
- Every published dataset is one JSON document in an envelope
  (`contracts/export_envelope.schema.json`, JSON Schema 2020-12): `envelope_version` (MAJOR.MINOR,
  readers accept any MINOR of a MAJOR they know), `dataset` + `dataset_version` (v1, v2: a row
  shape or metric definition changes at an era break get a new version, never mixed),
  `generated_at`, `produced_by` (profile, fallback, run id, Keystone version), the event-time
  `window` and its completeness periods, `sources`, `privacy` (`repository` or `package` level;
  people only as `aggregate-only`), the `corrections` URL, `row_count` and `rows`.
- `export/envelope.py` takes `produced_by` from the run log only (the latest successful
  `profile` stage); with none recorded it refuses to wrap (an export never guesses its profile).
- `python -m keystone.export.schemas` checks every `contracts/*.schema.json` against the 2020-12
  meta-schema and runs its examples (`tests/fixtures/schemas/<schema>/valid_*|invalid_*.json`).
  The pipeline_metrics example is a real emit from the committed fixture hours. CI runs it in the
  metadata job, after a real emit.
- Not yet: the row schemas of the site datasets (radar, project page) come with their exports
  after Phase 8.

## ADR-042 (2026-10-21) Release hardening: Syft SBOM, checksums, attestations when public
- `release.yml` (a `v*` tag; manual dispatch is a dry run): `uv build`, a Syft SBOM in SPDX JSON
  (anchore/sbom-action, which scans `uv.lock`), `SHA256SUMS`, a **draft** GitHub release for the
  owner to review. Every action is pinned by SHA (ADR-004); `tests/test_workflows.py` fails on
  an unpinned `uses:`, a default token above read, or an ungated schedule.
- Options for signing: GitHub artifact attestations (Sigstore-backed provenance and SBOM
  attestations); cosign keyless `sign-blob`. Choice: attestations
  (`actions/attest-build-provenance`, `actions/attest-sbom`), switched on by the go-public
  variable `KEYSTONE_PUBLIC` (ADR-006). Why: on GitHub's free plan, attestations in a private
  repository are not available (GitHub Enterprise Cloud only), and any public-good Sigstore
  signature (cosign included) writes the repository name and workflow identity to the public
  Rekor log, which would reveal a private repository. One switch at go-public, as for Scorecard.
  The repository is public since 2026-10-21; the attestation steps run once the owner sets
  `KEYSTONE_PUBLIC=true` (repository settings are the owner's; not changed from here).

## ADR-041 (2026-10-21) Scheduled ingest and daily publish: CLI only, off by default
- `ingest.yml` every 6 hours (minute 23): the last 6 completed hours (`ingest gharchive`, parse on,
  then `ingest gaps`). `publish.yml` daily (03:41 UTC): `profile check --run`, OSV ingest and
  parse, `warehouse build`, `quality contracts`, `metadata export`, `metrics emit`, the schema
  check, then pipeline_metrics.json as a 7-day artifact. Steps call only the CLI. A failure opens
  an issue titled `[keystone] <workflow> workflow failing`, or comments on it while it is open
  (`.github/scripts/failure-issue.sh`, `issues: write` on that job only).
- Minutes: the repository became public on 2026-10-21 (verified with gh by the Phase 4 session),
  so standard-runner minutes are free. The schedules still run only when the repository
  variable `KEYSTONE_SCHEDULES` is `true`; it does not exist, so they are off until the owner
  sets it. Manual dispatch always runs. Estimate, not measured (measure on the first dispatch
  and record it here): ingest about 2 to 3 minutes per run x 120 runs a month, publish about 5
  minutes x 30.
- Logs are public: steps print counts and file names only; no secret, actor key or login is
  ever printed (the parser never emits logins; actor keys stay inside Parquet).
- Ingest needs the production `KEYSTONE_ACTOR_SALT` secret (owner, deferred to Phase 9); the
  workflow fails fast with that message while it is missing.
- Interim store: the `data/` directory travels between runs in the Actions cache (one entry per
  run, newest restored; GitHub evicts beyond 10 GB per repository), **encrypted**
  (`.github/scripts/data-cache.sh`: tar + gzip, OpenSSL AES-256-CBC, PBKDF2 600,000 rounds,
  passphrase `KEYSTONE_ACTOR_SALT`). Why: in a public repository, workflows of pull requests
  from forks can restore caches of the base branch, and `data/` holds salted actor keys (silver,
  `repo_actor_day`); forks never get the secret, so they only see ciphertext. Both workflows
  need the salt secret. Silver grows about 85 MB a day (measured 2026-10-19), so this holds for
  weeks, not for the backfill. The Phase 9 storage decision (rolling silver window, Release
  assets) replaces it; nothing is published from it.
- Concurrency group `keystone-data`: ingest and publish never run at once on the same cache.

## ADR-040 (2026-10-21) Profile check: probe `full`, fall back to `free`, record `produced_by`
- `keystone profile check` probes the `full` services from the host, in parallel with a 2 s
  timeout: Redpanda Kafka (TCP 19092), its schema registry (`/subjects`), Flink REST
  (`/overview`), Lakekeeper (`/health`) and RustFS (any HTTP answer below 500: an anonymous S3
  request gets 403 from a live server). `--run` runs the rollup: in `full` (replay to Redpanda,
  Flink batch job and fold into a fresh namespace) when every probe passes; else, or when the
  full job fails, in `free` (the DuckDB rollup, the production path). A free failure fails the
  run (nothing to fall back to).
- The decision is one `profile` stage in the run log; its detail carries `produced_by`,
  `fallback`, `reason` and each probe. `pipeline_metrics.json` gains `produced_by` (null when no
  job ran through the check) and its `profile` follows the recorded decision when there is one
  (else, as before, the services that ran). The export envelope (ADR-043) copies it, so every
  published dataset says which profile produced it.
- Measured 2026-10-21 on the fixture hours, no stack running: 5 of 5 probes down, free rollup
  ran (225 repo_hour + 183 repo_actor_day rows, 303 events), pipeline_metrics.json valid with
  `produced_by.profile = free`, `fallback = true`. The test (`tests/test_profile.py`) points the
  probes at closed ports and checks the fallback's rollup equals a reference free run row for
  row.
- Limit: dbt reads the free rollup (ADR-028). A `full` run writes Iceberg tables in its own
  namespace and is checked by `rollup parity`; making the warehouse read the full output when
  `produced_by = full` is a Phase 10 item. On GitHub runners the stack never runs, so production
  records `free`.

## ADR-039 (2026-10-21) Phase 9 storage and compute: incident windows on Actions, history as Release assets
- **Partly superseded by ADR-050 (2026-10-22):** 12-month windows, no 30-day silver window,
  monthly chunks with parallel parsing. The sizing below is the 24-month design's.
Numbers: `docs/benchmarks.md` (sections 2 to 4; every projection
is an estimate, sized with `keystone bench storage`).
- **Owner decisions (2026-10-21, relayed by the Phase 4 kickoff session; not re-asked):**
  (1) a 30-day rolling silver window plus rollups/aggregates for all downloaded history;
  (2) download only about 24 months before each backtest incident (event-stream 2018-11,
  colors/faker 2022-01, xz 2024-03) plus the live score's trailing window; (3) compute on
  GitHub Actions, the repo made public by the owner (public since 2026-10-21); (4) curated data
  as GitHub Release assets, each under 2 GiB, after checking GitHub's terms; never publish
  `repo_actor_day` or any actor key.
- **Context (measured):** disk budget 17.9 GB; the full archive's silver would be ~760 GB and
  each 24-month window's 45 to 240 GB (estimates), so nothing is kept whole. Silver is 88.8 to
  267.0 MB per E3 day, rollups 12.8 to 13.0 MB (two thirds `repo_actor_day`). Parse runs at
  5,600 to 6,600 events/s per process; download at 5.06 MB/s with 2 connections.
- **Windows** (`config/incidents.yml`, new): from the first day of the incident month minus
  24 months to the end of the incident month; live: from the month 24 months back to today
  (the live score uses the backtest features' lookback). Overlaps are downloaded once. Sized:
  event-stream 2016-11 to 2018-11, colors-faker 2020-01 to 2022-01, xz 2022-03 to 2024-03,
  live 2024-10 onward: 3,034 days, 3.8 TB of gz to download, 9.1 billion events (estimates).
- **Backfill on Actions (one workflow, resumable chunks):**
  - A job is a run of consecutive days sized to 5 h (the 6 h job limit less an hour of
    margin): per hour fetch, verify, parse, delete raw; per day roll up, publish, delete. 138
    jobs, 664 job-hours, about 28 days end to end (estimate, this machine's rates; parallel
    parsing on the runner's 4 CPUs would shorten it and is not assumed).
  - Polite: jobs run one at a time (a `concurrency` group), 2 connections and the
    `config/policy.yml` limits, so GH Archive never sees more than this machine did.
  - Resumable: a job starts from the published state (which months' assets exist) plus the
    fetch manifest kept as a Release asset (repository-level only: URLs, sizes, checksums), so a
    cancelled or failed job is re-run from its first missing day. Idempotent per day.
  - Disk: peak ~1 GB per job of the runner's 14 GB (one raw hour, the day's silver and
    `repo_actor_day`, the job's output). RAM well under 16 GB (DuckDB peak 0.6 GB a day).
- **What is kept where:**
  - *Release assets (public):* monthly zstd Parquet of `repo_hour` and of the key-free
    contributor table (per repository and day: human actors, their events, the largest
    actor's events), one release per window and year. 34.6 + 16.7 GB in total, 50 assets per
    window, the largest month ~1.1 GB (estimates; upper-side for E2 years, whose rollup ratio
    comes from one holiday sample, and rollups grow far slower than volume). A month that would
    pass 1.5 GiB is split in two files, keeping a margin under the 2 GiB limit.
  - *Only on the runner, during a job:* `repo_actor_day`, silver with actor fields, raw files.
    Metrics that need actor identity across days (new versus departed contributors) are
    computed inside a job over the days it holds, or dropped; to settle in the metrics phase.
  - *30-day silver window:* 7.1 GB as parsed, 6.3 GB without the actor fields (estimate).
    Proposed: Release assets of silver without the actor fields, one per day, the oldest
    deleted daily. The Actions cache is not used for it: pull-request runs (forks too) can
    restore base-branch caches, and use above the 10 GB per repository is billed.
  - *This machine:* nothing in bulk; a local copy of the months a phase needs.
- **GitHub terms (checked 2026-10-21):** "About releases": each file "must be under 2 GiB",
  "Up to 1000 release assets", and "There is no limit on the total size of a release, nor
  bandwidth usage". Acceptable Use Policies section 9: bandwidth well above similar users may
  be throttled or suspended; repositories placing "undue strain" may be deleted after notice.
  Additional Product Terms (Actions): not for activity "unrelated to the production,
  testing, deployment, or publication of the software project", nor "as a content delivery
  network", nor a disproportionate burden. Runner reference: "free and unlimited on public
  repositories" (standard runners: 4 CPUs, 16 GB RAM, 14 GB SSD); limits: 6 h per job, 35
  days per workflow run, 256 matrix jobs, 20 concurrent jobs on Free. The backfill builds and
  publishes this project's own data, which reads as within "publication of the software
  project"; it is one sequential job at a time, not a CDN. Risk accepted with mitigations:
  publish compact monthly files, never serve the site from release downloads at volume.
- **Why:** $0; nothing bulk on this disk; every job fits the runner; privacy rules keep actor
  keys on ephemeral runners only; the windows are what the backtests and the live score read.
- **Open:** the 30-day window as public event-level rows without actor fields is a privacy
  judgement (no person fields, GH Archive itself is public); confirm with the owner before
  the first upload, else keep the window as `repo_hour` only. Runner throughput is to be
  measured on the first job and this ADR's job plan resized.
## ADR-038 (2026-10-21) robots.txt patterns per RFC 9309 in the polite client
- Found in Phase 7: pypi.org's robots.txt has `Disallow: /pypi/*/json`, and
  `urllib.robotparser` (used since Phase 2) matches rule paths as plain prefixes, so it would
  have allowed the PyPI JSON API. `fetch/robots.py` now implements RFC 9309 matching: the group
  of our product token, else `*`; `*` and `$` in paths; the longest match wins and `Allow` wins
  a tie; anything that is not a rule line (the npm registry answers `/robots.txt` with a
  package document) is ignored. Tested with pypi.org's rules. No earlier host was affected
  (GH Archive and the OSV bucket have no robots.txt; ecosyste.ms allows all).
- The policy also gains a free-quota guard: a host may name its remaining-quota header and a
  floor (`quota_header`, `quota_floor`, `quota_limit_header`); the client raises
  `QuotaExhausted` before a request that would go below the floor. The share used is reported
  in `pipeline_metrics.json` (`cost.free_quota`).

## ADR-037 (2026-10-21) Entity resolution: packages to repositories, renames, names without ids
- Sources (ADR-010 refined by measurement): ecosyste.ms `POST /packages/bulk_lookup` (100
  purls per request, 0.5 req/s, stop at 500 requests left of the anonymous 5,000/hour) gives
  the repository URL it derived, the repository's GitHub id (`repo_metadata.uuid`) and its
  previous names; the critical sets come from `package_names?critical=true` (PyPI 522, npm
  2,295 on 2026-10-21). The npm registry `/<name>/latest` (1 req/s, conditional on the ETag)
  gives the `repository` field the maintainers published. PyPI's JSON API is disallowed by
  robots.txt (ADR-038), so PyPI has no registry cross-check; deps.dev stays excluded (ADR-010).
  Claims are refreshed after 7 days; ecosyste.ms absences are asked again each run.
- Silver (`silver/registry/`): allow-listed fields with lineage; maintainers, authors, emails,
  READMEs and funding links are never read. Package keys normalise names as the registries
  compare them: PEP 503 for PyPI, lower case for NuGet (ecosyste.ms answers NuGet in lower
  case); the warehouse recomputes the key, so a changed rule never splits a package.
- Universe: OSV packages with at least one non-malware advisory, plus the critical sets.
  Packages known only from malware reports stay in `int_package_universe` with package_scope
  `osv_malware_only` and are out of the match rate (their registry entries are removed).
- Package to repository (`map_package_repo`, one row per package and candidate). A candidate
  is a GitHub id when known (claims under an old and a new name merge: a rename), else
  host/owner/name. Independent source families: the registry (the npm `repository` field and
  ecosyste.ms count once together, since ecosyste.ms derives its URL from the same registry
  metadata; their agreement is a consistency check, not a second witness) and an OSV GIT
  range; OSV FIX references only support. Candidates a reviewer rejected stop counting as
  rivals. Rules, first match wins: `review` (seed `resolve_decisions`, overrides
  everything); `sources_agree` (both families, no rival with one) is the only `auto_exact`;
  `contested` (one family against a rival named only by a FIX reference in an advisory
  about this package alone) leaves both `suggested`, since a packager's repository and the
  upstream one are often confused there; `single_source` (one family, no rival, GitHub id
  known) is `accepted`; `single_source_unverified` is `suggested`; `most_sources` (strictly
  more families than every rival) is `accepted`; a tie goes to the language model (`llm`,
  accepted at confidence >= 0.8, and only when the stored answer was asked about exactly the
  current tied candidates, so a stale answer never decides) or stays `suggested` (`tie`);
  fewer families than a rival is `rejected` (`outvoted`); FIX references alone are
  `suggested`. Whether a FIX reference is about one package is counted after name
  normalisation. Confidence is fixed per rule (high, medium, low). dbt tests: at most one
  decided repository per package, `auto_exact` only with both families and no rival, a
  model's choice only from the listed candidates, every reviewed decision still matching a
  listed candidate (a key that changes would orphan it); a dbt unit test pins every rule.
- Repository ids of claimed names (`int_repo_identity`): ecosyste.ms current name of one id;
  else its previous name (or the redirect it followed) of one id; else the one id GH Archive
  saw with the name; else the latest of several (a name taken again); else none.
- Renames (`dim_repo_rename`): consecutive spells of one id in GH Archive are `auto_exact`
  (the id is the identity), bounded by the old name's last and the new name's first
  sighting; ecosyste.ms previous names that GH Archive did not see are `accepted` (only from
  the newest fetch of the id, and only for ids with exactly one current name).
- Names without ids (`map_repo_name_id`, ADR-028), per name and UTC hour, E1 copies from
  2015 dropped (ADR-020): one id's spell spans the hour, `auto_exact`; one id ever carried the
  name and is within 90 days, `accepted`; one id that already existed at the hour (GitHub
  allocates ids in creation order, so an id no larger than one seen in an event before the
  hour existed then) stays `suggested` (`unique_name_id_existed`): existing then says nothing
  about the name it had then (the review measured a median gap of 2.5 years and ids seen under
  other names in between); a newer id or several ids, `suggested`. Folding these
  events into the activity metrics is a metric change for Phase 8 (versioned, not silent).
- Review without blocking: `keystone resolve report` writes the match-rate report and two
  CSVs of suggested rows (`data/resolve/`); a reviewer copies a row with `decision` filled to
  `dbt/seeds/resolve_decisions.csv`. This is also the corrections path for maintainers.
- Language-model rung: `keystone resolve choose` asks only about ties, lists the candidates
  (repository keys with names and the sources naming them), and `chooser.validate` refuses any
  answer outside the list or with a confidence outside [0, 1] (stored as `refused`, never
  used); an unreadable answer is refused too. OpenRouter (`OPENROUTER_API_KEY` in `.env`, run
  with `uv run --env-file .env`) on `nvidia/nemotron-3-ultra-550b-a55b:free` (2026-10-22; the
  first choice, nemotron-3-super, returned provider 404s). $0 by construction: only `:free`
  model ids, listed prices checked to be zero before the first request, every response's
  reported cost checked to be zero, 402 a hard stop. The account has never bought credits, so
  the free tier allows 50 free-model requests per day: a 429 for the daily cap stops the run,
  keeps the answers so far, and the next run asks only ties without a valid choice for the
  same candidates. Paced at 0.25 req/s (policy.yml). Reasoning models think before the JSON,
  so the cap is 4,000 tokens (the first run used 400 and 14 of 32 answers were cut off).
- CI has no network for registries: `resolve fetch --replay tests/fixtures/registry` serves
  recorded, synthetic responses through the same projection (refused on the default data
  root). Order in a pipeline run: warehouse build, resolve fetch, warehouse build, report.
- Measured on the local data (2026-10-21, after the review fixes): 4,736 packages in scope
  (PyPI 522 + npm 2,295 critical, 1,919 more with an advisory); first fetch 2,388 requests,
  664 MB (ecosyste.ms bulk responses are large), 2,697 s; 4,383 decided (92.5%): 276
  auto_exact, 4,107 accepted (before the language-model rung; with it, 2026-10-22: 4,412
  decided, 93.2%, 29 of 32 ties chosen by the model at confidence >= 0.8; after the
  owner-delegated review of the CSVs, 48 package decisions and 78 name decisions in the seed:
  4,456 decided, 94.1%); 155 suggested candidates in the review CSV (25 after the review); the npm field and
  ecosyste.ms name the same repository for 2,311 packages, the same id after a rename for 2,
  differ for 1. Renames: 3,088 from GH Archive, 1,207 more from ecosyste.ms. Names without ids:
  of 194,416 events (101,786 name-hours) 6,694 are decided, 35,997 suggested, 151,725 have no
  candidate in the loaded sample hours (docs/match-rate.md). Review: docs/reviews/phase-7.md.

## ADR-036 (2026-10-21) Lakekeeper terms of use accepted (owner)
- The owner accepted Lakekeeper's terms of use for this non-commercial project on 2026-10-21.
  `catalog-init` and the parity CI keep sending `accept-terms-of-use: true`. Closes the open
  question of ADR-027.

## ADR-035 (2026-10-21) Telemetry: stage run log, OpenTelemetry, pipeline_metrics.json
- Every data command is a measured `Stage` (telemetry/runlog.py): wall-clock seconds, records
  and bytes it counted (null when it cannot count), status, one line in
  `<root>/telemetry/runs.jsonl`. A pipeline run is the set of stages sharing `KEYSTONE_RUN_ID`
  (an orchestrator sets it; else one id per process). Rate = records out / the stage's seconds.
  Phase 4's `rollup/runs.jsonl` is replaced by this log.
- `keystone metrics emit` builds `pipeline_metrics.json` (schema:
  `contracts/pipeline_metrics.schema.json`, JSON Schema 2020-12, validated on every emit) from
  the log, the data (silver and rollup counts, newest event, quarantine rows) and the metadata
  export. A value nothing measured is null, never 0 (`bytes_downloaded` before any ingest,
  `checks_passed` before any check; the Flink stage's record counts, which the SQL client does
  not report). A run's status and `last_run.stages` use the last attempt of each stage.
  Definitions: `events_processed` sums parse work (a re-parsed hour counts again);
  `events_stored` counts silver rows; an engine is `active` when it ran in the last run;
  `cost.usd` is 0 because no service used has a billing account; the one free quota measured
  so far is the disk budget (GitHub Actions minutes are listed with null until the production
  workflow can read them). Timestamps are UTC (DuckDB hands TIMESTAMPTZ back in the local
  zone; converted). Nothing is estimated.
- OpenTelemetry (`obs` profile): each stage records gauges (`keystone_stage_*`, labels stage
  and service) via OTLP/HTTP to the collector 0.163.1, which exposes them to Prometheus 3.15.0;
  Grafana 13.2.4 provisions the `keystone-pipeline` dashboard. The resource has a fixed
  `service.instance.id` (the SDK would start a new series per process, and `max` over stale
  series showed old values; found in the first gate check), and resource attributes are not
  copied into labels, so a version bump keeps the same series (review); series key on stage
  and service. `metrics emit` also exports the
  pipeline totals as gauges.
- Gate check (`keystone metrics check-grafana`): reads the dashboard from Grafana's API, runs
  each panel's query through Grafana's `/api/ds/query`, and compares it with the JSON path in
  the panel description (stage panels per stage of `last_run`); tolerance 0.001 (the JSON
  rounds rates and durations to 3 decimals). The stage gauges are written by each stage as it
  ends, independently of the JSON, which is built later from the run log. Order: emit, then
  check (a check is itself a stage, so a check before a new emit compares older JSON).
- Measured 2026-10-21 (final gate run on the local data, after the review): 50 of 50 values
  equal. 42 are stage values (14 stages x records out, duration, rate), written by each stage
  independently of the JSON; the other 8 are pipeline totals exported from the JSON itself, so
  they check only the transport (OTLP, collector, Prometheus, Grafana). 15 engines with
  versions read from the running service or its pinned tag. Stage numbers are in
  PROGRESS.md. RAM: collector 31 MB, Prometheus 42 MB, Grafana 296 to 323 MB (limit 512 MB).

## ADR-034 (2026-10-21) Catalog metadata, generated contracts, static export, completeness
- One source: `config/metadata.yml` (owner group, domains, classifications, freshness SLAs,
  glossary, column docs, datasets outside dbt) and dbt `meta` per folder and model (owner,
  domain, layer, classification, SLA). The owner is the maintainers group, never a person.
- Contracts: hand-written ODCS contracts stay in `contracts/` (silver GH Archive). The others
  are generated into `metadata/contracts/` (`keystone metadata contracts`): OSV silver (Arrow
  schemas), the free rollup tables (DuckDB) and every dbt model and seed (types from
  `catalog.json`, descriptions from dbt YAML, `required` from not_null tests).
  datacontract-cli 1.2.7 tests them, DuckDB-file contracts with its `duckdb` server type. CI
  regenerates them (`--check`) and fails on any difference, so a schema or description change
  needs a reviewed contract change. Exception: `stg_rollup__repo_hour` is a view over the
  attached rollup database, which datacontract-cli cannot attach; dbt build checks it.
- Static export (`keystone metadata export`): `metadata/catalog.json` (44 datasets with owner,
  description, domain, layer, SLA, contract, classification, columns and glossary terms),
  `glossary.json` (the six terms and where they are attached), `lineage.json` (ADR-031) and
  `contracts/` (all contracts). Completeness gate: models and columns documented >= 95% each,
  every dataset with every catalog field, no owner missing; measured 19/19 models, 483/483
  columns, 44/44 datasets. Published tables are classified public (tested).
- "Published export field" (gate wording) is, until the site export exists (Phase 8), a
  column of a table in the published Iceberg namespaces (`marts`, `metrics`); the site export
  will add one hop to the same graph.

## ADR-033 (2026-10-21) What Spark and Flink report to OpenLineage (measured)
- openlineage-flink 1.54.0 (job-status listener, FLIP-314, jar pinned by SHA-256) works with
  Flink 2.2.1: for the batch job it reported the Kafka topic as input and the four Iceberg
  `_ontime`/`_late` tables as outputs, and the fold job's inputs and outputs, all table level,
  with schemas, no column lineage. It emitted START events only (no COMPLETE) in our batch
  runs, and named Iceberg tables `ice.<namespace>.<table>` with an empty namespace;
  `naming.canonical` maps them, and the graph takes edges from START events too (run counts
  use COMPLETE only).
- openlineage-spark 1.54.0 (listener in spark-defaults.conf) ran in every Spark read but, for
  the read-only fingerprint queries, emitted only application START/COMPLETE without datasets.
  Spark is therefore listed as a lakehouse reader that ran, not as a lineage source.
- Both write to the mounted `data/lineage/incoming/` (file transport); `keystone lineage
  collect` canonicalises the events into the log. The parity workflow checks that Flink wrote
  lineage on every run.

## ADR-032 (2026-10-21) DataHub: lightest viable setup, sources plus Keystone's own aspects
- Setup (`datahub` profile): DataHub v1.7.0.1 GMS (consumers embedded, no frontend), the
  system-update job, OpenSearch 2.19.3 (512 MB heap) and Postgres 17.6 as the store; the
  message bus is the Redpanda broker already in the stack. DataHub's Kafka-free `pgqueue`
  transport was tried first and failed: it needs the `pg_partman` extension, absent from the
  stock Postgres image. Measured: GMS 1.22 to 1.58 GB (limit 1.75 GB), OpenSearch 0.93 GB,
  Postgres 40 MB; images about 3 GB compressed, so free disk fell from 15 GB to 9.4 GB. It runs
  with the Flink containers stopped.
- Sources (acryl-datahub 1.7, `metadata` group): `dbt` (451 events: models, sources, column
  lineage from the compiled SQL, 57 assertions from the tests and their last results),
  `iceberg` (Lakekeeper REST, 261 events) and `kafka` (the topic and its registry schema).
- Keystone's own aspects (lineage/datahub.py): 5 domains, the owner group, the glossary (node
  plus 6 terms), and per dataset ownership, domain, tags (layer, classification, freshness
  SLA), description, a link to its contract and column glossary terms; every non-dbt job of
  the lineage graph (CLI and Flink) as a DataJob with inputs, outputs and column lineage
  (including the rollup's, ADR-031). DataJobs of the flow that the current graph no longer has
  are soft-deleted (the first emit left a combined `warehouse.publish` job; review).
- Check: every dataset of the gate path is upstream of the published table in DataHub's
  lineage search (dbt nodes count by either sibling, dbt or duckdb; indexing is asynchronous,
  so the check retries for up to 2 minutes), and every hop of the gate's column chain has a
  fine-grained lineage edge stored in DataHub (read from the UpstreamLineage and
  DataJobInputOutput aspects; lineage search on a schemaField returns datasets only). Databricks and Unity Catalog lineage: skipped, no
  token (ADR-030).

## ADR-031 (2026-10-21) OpenLineage from the CLI and dbt; one naming scheme; Marquez
- The CLI emits OpenLineage 2-0-2 events with openlineage-python 1.54 (START at a stage's
  start, COMPLETE or FAIL at its end, a parent facet pointing at the pipeline run) to
  `<root>/lineage/events/YYYY-MM-DD.jsonl`, and posts them to Marquez when `OPENLINEAGE_URL` is
  set (`keystone lineage push` replays the log; Marquez de-duplicates). Column lineage is
  emitted where the CLI knows it: the topic (projection of silver events), the Iceberg publish
  (identity copies) and the rollups, derived from their SQL by sqlglot 30.12
  (`lineage/sqllineage.py`: 13 columns of repo_hour and 15 of repo_actor_day trace to silver
  columns; `events` counts rows and has no source column). The parse hop (hourly file to
  silver) is table level: the file has no schema to name fields against. The publish is one job per table, so no table seems to
  feed another (the first version listed all tables in one event; caught in the gate run).
- Naming (lineage/naming.py): stable logical namespaces instead of machine paths
  (`file://keystone`, `duckdb://keystone`, `kafka://redpanda:9092`,
  `iceberg://lakekeeper/keystone`, the GH Archive and OSV URLs). Other tools' identifiers are
  mapped onto it and the rewrite is counted in a `keystone_canonical` run facet.
- dbt: `dbt-ol send-events` (openlineage-dbt 1.54) over the build's artifacts (the build's
  run_results.json is restored after `docs generate`). openlineage-sql has no Windows wheel,
  so on Windows it runs in a pinned uv container (the `lineage` group of the same lock); on
  Linux (CI) natively. Measured: 76 events, column lineage on 17 models; 61 column inputs name
  CTEs or `read_parquet` and are dropped and counted, not guessed.
- Graph and gate path (`lineage.json`): datasets, jobs, fields; the gate path from an hourly
  file to a published table (table level) and a published field whose column chain follows
  that path dataset by dataset; `--require-path` requires the chain to reach the table the
  hourly file is parsed into (`complete`; the first version passed with a chain ending in
  staging, review). Measured: `metrics.metric_maint_v1_repo_day.activity_date` back through
  the metric, `fct_repo_day`, `stg_rollup__repo_hour`, `rollup.main.repo_hour` to silver
  `created_at`. The full-engine branch (topic, Flink job, Iceberg rollup) is beside it.
- Marquez 0.51.1 (`lineage` profile, Postgres 17.6, search off, no web UI): the gate path's 13
  nodes and the full-engine path are present in its lineage API (job names compared by
  `simpleName`, since Marquez prefixes child jobs with the parent's name), and the 6 fields of
  the column chain in its column-lineage API.
- Who could do what (table / column): CLI yes / where exact; dbt-ol yes / yes (parsed SQL);
  DataHub dbt source yes / yes; sqlglot on the rollup SQL - / yes; Flink yes / no;
  Spark ran, no datasets for reads; Airflow not run (deferred to Phase 10, when DAGs exist:
  the provider adds nothing until a DAG calls the CLI); Databricks not run (no token).
- Retention: past days are gzipped (appended as another gzip member when the day's archive
  exists, so a late event never replaces it) and capped (`keystone lineage compact`, default 50 MB); the
  oldest days beyond the cap are summarised into `lineage/summary.jsonl` and removed. The push
  of the compressed log to a data branch belongs to the production workflow and is deferred
  to the live refresh (Phase 10).

## ADR-030 (2026-10-20) Phase 5 rungs: Databricks skipped, Trino done (Tier 2)
- Databricks (dbt-databricks, Unity Catalog, a Lakeflow Declarative Pipeline for marts) needs a
  workspace token, which does not exist yet: skipped and logged in PROGRESS.md. The dbt SQL is
  written for both (ANSI casts, CASE, window functions, `DATE_TRUNC('hour', ...)`, `MAX_BY`,
  all in DuckDB and Databricks SQL); sources, the profile and the seed column type (switched on
  `target.type`) are the only per-target parts. Not yet run on Databricks. "`dbt build`
  passes on every available target" therefore means the `duckdb` target today.
- Trino ran within the RAM budget (container limit 1.5 GB, 0.91 GB idle, 1.45 GB peak during the
  fingerprint queries), so the Tier 2 rung is in: `trino` profile, Iceberg REST connector,
  native S3. The default per-node query memory (357 MB) failed on the distinct counts of the
  800k-row tables; `compose/trino/config.properties` gives queries 768 MB within the same limit.

## ADR-029 (2026-10-20) Lakehouse reads: DuckDB writes the marts, four engines read them
- Publish: `keystone warehouse publish` copies every dbt model tagged `publish` (schemas `marts`
  and `metrics`) into Iceberg through DuckDB's Iceberg extension (REST catalog, CTAS); the
  namespace is the schema, with a prefix for test runs (`ci_`). Drop and re-create, so a reader
  can miss a table for the seconds of a publish (12 s for the nine tables); an atomic swap
  (branch or `replace` commit) is a Phase 10 item. Options: dbt-duckdb materialising into the
  attached catalog (not tried: dbt's table materialisation builds a temporary relation and
  renames it, and the explicit copy keeps the Iceberg write in one place), Spark or Flink as
  the writer (rejected: a JVM in the free path).
- Readers: DuckDB (in-process), Spark 4.1.3 (newest line with an Iceberg 1.12.0 runtime; 4.2 has
  none yet), Flink 2.2.1 (the Phase 4 image, SQL client in batch mode) and Trino 483. Spark and
  Trino run only for the check (profiles `spark`, `trino`); images pinned by digest, jars by
  SHA-256. Every port binds to 127.0.0.1.
- Check (`keystone warehouse verify`): one fingerprint query per table, built from the Iceberg
  schema: row count; per column the non-null count, plus the sum of an integer or boolean
  column, or the distinct count of any other column and the summed character length of a
  string. All exact integers, the same SQL in every engine (Trino spells CHAR_LENGTH as
  LENGTH), compared against the first engine. Limit: a timestamp or date read with a constant
  shift (a time-zone slip) keeps its distinct count, so it would not be caught; there is no
  engine-neutral epoch function. A requested namespace with no tables fails the check.
  `period` was renamed `completeness_period` because it is reserved in Flink SQL.
- Measured 2026-10-20: the nine marts (sample hours of every era, the day 2026-10-19 and the
  PyPI OSV zip) and the Flink-written `replay_20261019_batch.repo_hour` match in all four
  engines (after the review fixes: one run DuckDB+Spark+Flink, 30 of 30 reads, 5.2 min; one run
  DuckDB+Trino, 20 of 20, 2.2 min; the distinct counts make it slower than the first version's
  52 s). The parity workflow runs all four together on fixtures with a fresh Flink batch table.

## ADR-028 (2026-10-20) Warehouse: dbt on DuckDB over silver and the free rollup
- dbt-core 1.12.6 with dbt-duckdb 1.11.0 (`warehouse` dependency group); anonymous usage
  stats off. The CLI (`keystone warehouse build`) sets every path from `--root`, runs
  `dbt build` and `dbt docs generate`, then a docs-coverage gate (>= 95%) that compares the
  real columns in `catalog.json` with the YAML descriptions (100%: 19 models and seeds, 174
  columns). Orchestrators call the CLI, never dbt.
- Inputs: silver Parquet (GH Archive events, OSV) read in place and the free engine's DuckDB
  file attached read-only. `repo_actor_day` (salted actor keys) is not declared as a source,
  so no model can read it; a singular test (itself tested with a planted column) fails on any person-like column (actor, login,
  email, author, owner) in `marts` or `metrics`, and the publish refuses such columns again.
  No owner column is derived from repository names.
- OSV silver (`keystone parse osv`): vulns, affected packages and repository pointers, an
  allow-list like ADR-019 (no `details`, `summary`, `credits`, IOCs). Pointers are GIT ranges
  (belong to one package) and FIX references (belong to the record); advisory, web and package
  links are not repositories. OSV repeats a record in the zip of every ecosystem it affects;
  staging keeps one copy per id (latest `modified_at`, then `source_file`) and only that copy's
  affected rows and pointers (tested with a duplicate zip). PyPI: 26,393 records (11,852 malicious-package reports), 31,997
  affected entries, 3,797 pointers, 0 quarantined.
- Models: `dim_repo` and `dim_repo_name_history` (spells of a name per repository id, ordered
  by first sighting, compared case-insensitively since GitHub names are; a name taken again is
  a new spell; the E1/E2 overlap rule of ADR-020 applies here as in the rollups; ids missing in
  E1 2012 and some E0 are resolved by name in Phase 7, ADR-037), `dim_package`
  (ecosystem + name as OSV spells it), `bridge_package_repo` (confidence is a rule: high = GIT
  range, medium = FIX reference in a one-package record, low = FIX reference in a multi-package
  record; `repo_id` only when exactly one id ever carried the name, else `ambiguous_name`),
  `fct_vuln` (record x package), `fct_repo_day` (UTC day x repo x era x completeness period:
  era and period are keys, so nothing mixes) and metric inputs per version: `maint_v1` (E1,
  E2: merged flag), `maint_v2` (E3: closes only), `release_v1` (E1 to E3). Accepted-values tests
  keep other eras out. dbt unit tests pin the spell logic, the overlap rule, the UTC day cut and
  the per-type pivots. The periods and eras seeds are copied from `config/sources.yml`; a
  pytest test fails if they differ.
- Measured on the local data (151 sample hours of every era plus 2026-10-19, PyPI OSV), after
  the review fixes: 73 nodes pass (3 of them unit tests), build plus docs 22 s; 809,212
  repositories (812,300 name spells; their events equal the 2,094,765 of `fct_repo_day`),
  867,596 repo-days, 13,846 packages, 526 package-repo candidates (242 with a unique repository
  id), 27,894 vuln-package rows. The
  event total of 2026-10-19 in `fct_repo_day` (1,042,689) is the day's 1,042,693 events minus 4
  without a repository id.

## ADR-027 (2026-10-20) Lakehouse components confirmed for Phase 4 (updates ADR-014)
- Catalog: **Lakekeeper v0.14.1** (Iceberg REST) on Postgres 17.6; object store: **RustFS 1.0.3**
  (Apache-2.0, single binary, S3 API). Both run in the `catalog` compose profile; a one-shot
  `catalog-init` creates the bucket (SigV4 via curl), bootstraps Lakekeeper and registers the
  `keystone` warehouse (S3 profile, path style, STS off). SeaweedFS was the alternative; RustFS
  has stable 1.0 releases and needs no master/volume split. MinIO stays out (ADR-014).
- Flink writes through the REST catalog with S3FileIO; DuckDB reads the same tables through the
  REST catalog (`ATTACH ... TYPE iceberg`) with its own S3 secret. Credentials are local-only
  defaults, overridable in `.env`; every port binds to 127.0.0.1.
- Open owner question: Lakekeeper's bootstrap call requires `"accept-terms-of-use": true`.
  `catalog-init` sent it for the local stack and the parity CI does too. Accepting terms is an
  owner decision, so it is listed under Blockers in PROGRESS.md until the owner confirms.
- Measured (docker stats, Docker VM 8.2 GB): both profiles use about 1.4 GB idle and about
  3.0 GB after the one-day replay (taskmanager 1.37 GB, jobmanager 0.68 GB, RustFS 0.45 GB).

## ADR-026 (2026-10-20) `full` rollup: Redpanda + Avro + Flink 2.2.1 to Iceberg, late rows kept
- Versions: Flink 2.2.1 (newest supported by both flink-connector-kafka 5.0.0-2.2 and
  iceberg-flink-runtime-2.2 1.12.0, ADR-014), Redpanda v26.2.5 (Kafka API and schema registry).
  Connector jars are pinned by SHA-256 in `compose/flink/Dockerfile`; images by digest. License:
  Redpanda is BSL 1.1 (source-available, not OSI open source), allowed for this non-commercial,
  self-hosted use; Flink, Iceberg, Lakekeeper and RustFS are Apache-2.0. Redpanda Console was
  dropped: nothing used it (tool-credibility rule).
- Topic value: Avro (`contracts/avro/gharchive_event.avsc`, the columns the rollups read plus
  lineage), Confluent wire format, subject `<topic>-value` with BACKWARD compatibility; the
  producer checks compatibility before registering, `keystone stream check-schema` checks a
  candidate. Tested live: an optional field passes, a required field or a narrowed type fails.
- Replay: GH Archive is hourly files, not a firehose. Hours are produced in arrival order (event
  time of their content by default) to a one-partition topic, so the topic order is the arrival
  order. Watermark `created_at - 1 h`, emitted per event (`scan.watermark.emit.strategy`).
- Late data: Flink SQL window TVFs have no allowed lateness and drop late rows silently. This is
  a deliberate deviation from the brief's "allowed lateness": instead of keeping windows open,
  the job applies the window operator's own lateness test (`window_end - 1 ms <= watermark`) with
  `CURRENT_WATERMARK` in the Calc after the source and routes those rows to `<rollup>_late`
  (append) while the rest go through TUMBLE to `<rollup>_ontime`. Why it is exact: the Calc's
  watermark is never behind the window operator's (same chain at parallelism 1; after a hash
  exchange the window operator takes the minimum over its inputs), so a row flagged on time is
  never dropped, and the two filters split every row exactly once. The parity test checks this
  conservation (`sum(events)` over both tables equals the filtered input). Both inserts run in one statement set (one job, one
  source). A batch fold writes `<rollup>` = SUM/MAX over both. So nothing is dropped and the
  result does not depend on when the watermark moved. The parity test checks the late path ran
  and that `_ontime` alone differs (the test can see the difference).
- Exactly-once is *configured*, not yet failure-tested: checkpoints every 10 s (default
  exactly-once mode), Kafka offsets live in the checkpoint and the Iceberg sink commits per
  checkpoint (final checkpoint at the end of a bounded replay). A kill-the-taskmanager recovery
  test is a Phase 9 benchmark item.
- Modes: `stream` (bounded replay: rebuilds its tables and waits; live with `--unbounded`: keeps
  its tables, resumes from its consumer group, returns after submit, and needs `rollup fold`
  periodically, so corrections appear at fold time, not in the streaming output), `batch`
  (backfill; the same SQL, nothing is late), `fold` (batch). Limit: the stream is append-only, so a re-parsed hour that was already
  produced needs a batch rebuild of its windows in this engine (DuckDB handles it in place);
  Phase 9/10 wire that rebuild into the backfill and the profile fallback.

## ADR-025 (2026-10-20) `free` rollup: DuckDB recomputes touched windows from silver
- State: one DuckDB file (`data/rollup/rollup.duckdb`) with the rollup tables and
  `rollup_files` (silver hour, event-time range and a fingerprint: source SHA-256 plus size and
  mtime of the hour's silver files, which are replaced atomically). The source SHA alone would
  miss a re-parse of the same raw file (parser fix, bot list, salt); the test re-parses with an
  unchanged source SHA. A run finds new, rewritten and removed hours, takes every window their old and new ranges touch, reads every
  silver hour whose range overlaps those windows (from `hour_types`, so a late hour corrects
  earlier windows), and deletes and re-inserts those windows in one transaction.
- Options: add deltas per hour (rejected: not idempotent on re-parse), MERGE per key (rejected:
  a re-parse would leave stale keys), recompute touched windows (chosen).
- Why: idempotent (a re-run is a no-op), independent of arrival order (tested: incremental in
  arrival and reversed order equals a full run), and bounded: a run reads only overlapping hours.

## ADR-024 (2026-10-20) Rollup logic once, in portable SQL
- Each rollup is one ANSI contribution query in `src/keystone/rollup/sql/` (one row per event:
  event time, keys, 0/1 measure columns) with a header naming the window (`1 hour` or `1 day`),
  the keys and the MAX measures (all others SUM). `input.sql` holds the shared filter. DuckDB
  wraps a query with `date_trunc` and GROUP BY; Flink with TUMBLE. Neither engine has its own
  copy of a measure. SQLFluff (ANSI dialect) lints the files; a test rejects engine functions.
- Tables: `repo_hour` (repo, era, event type: events, human events, opened, closed, reopened,
  merged and `merge_known`, published, added, newest fetch) and `repo_actor_day` (repo, salted
  actor key, era: pushes, PRs opened/closed/merged, issues opened/closed, releases, members
  added, comments; bots excluded; internal only, never published). Era is a key everywhere, so
  eras never mix; `merged` counts only where `merge_known` does (E1, E2).
- Rules: windows are UTC; ADR-020's overlap rule (E1 rows from 2015-01-01 00:00Z dropped) is in
  `input.sql`; timestamps are cut to milliseconds (the Avro contract) in both engines.
- Watermark delay 1 h. Measured on the 151 sampled silver hours (hour_types): every E0, E2 and
  E3 file holds only its own UTC hour; E1 files (Pacific names) span at most 60.1 minutes and 68
  of 72 cross a UTC hour boundary by seconds. Replayed in event-time order, nothing is late.
  Late arrival (a healed gap, an out-of-order replay) is handled by ADR-025/026, not by a longer
  delay.

## ADR-023 (2026-10-20) Contracts with datacontract-cli, expectations with Great Expectations
- Contracts: ODCS v3.2.0 YAML in `contracts/` (events, hour_types, quarantine), tested by
  datacontract-cli 1.2.7 (DuckDB over the Parquet files). Column-level rules (required, patterns,
  valid values) plus SQL rules (one row per source line, key kind per era, ids per era, repo name
  form, the payload-trim rule). datacontract-cli does not check column types on Parquet (it warns);
  `tests/test_contracts.py` checks names, types, required and descriptions against the Arrow
  schemas instead. Any other warning, failure or error fails the run.
- Expectations: Great Expectations 1.24 (ephemeral context, one Parquet file per batch), for the
  per-era rules with row conditions (E1 keys by login, offsets only in E1, trimmed pushes by event
  time). GX usage analytics are switched off in code before import.
- Both live in the `quality` dependency group. CI: `contracts` job (lint every contract, parse the
  scrubbed fixtures, run contracts and GX) and the pytest suite (seeded violations must fail).
- Why both: contracts are the published interface (ODCS, portable to dbt and Databricks later);
  GX holds cross-column and per-era rules. Each does real work and each caught a seeded violation.

## ADR-022 (2026-10-20) Schema drift: learned registry per era, fail the hour loudly
- The registry `contracts/drift/gharchive.yml` lists key paths up to 3 levels deep (array
  elements as `[]`) and JSON types per era and event type; paths outside the payload are shared
  per era. `null` is accepted anywhere. It holds paths and type names only, never values.
- An unknown event type, field or type, or a record whose shape does not match its era (or a
  push whose shape contradicts the payload-trim instant), fails the whole hour: nothing is
  written, a value-free report goes to `data/quality/drift/`, and the manifest marks the hour
  `error` so the next run retries it. `keystone parse learn-drift` adds a reviewed sample to the
  registry; its git diff is the review.
- Measured: a 6-hour baseline flagged 49 of the 123 hours kept at that point (rare types,
  null-only fields). A
  backfill will hit drift in hours unlike the samples; that is the intended stop, not a bug.
  Phase 9 learns from a stratified sample (one day per quarter) before the run.
- Known edge (review): a late event inside an E3 file created before the trim instant but
  without `payload.size` would fail its hour on every retry. None was seen in the sampled E3
  hours; Phase 9 measures late pushes before the backfill and either handles them or keeps them
  as drift with an ADR. `learn-drift` refuses hours with quarantined records.

## ADR-021 (2026-10-20) Completeness boundaries refined from parsed counts
- C2 starts 2025-05-23 16h (was 18h): capture falls inside 16h from about 16:35Z (per-minute
  counts); C1 ends at the last complete hour, 15h. GAP ends at 2025-10-14 20h and C3 starts at
  21h (was 2025-10-15 00h): 21h is 30.5 MB, 20h 0.25 MB.
- C4 and C5 starts are unchanged: the oracle hours show non-push types falling gradually from
  2026-03 to 2026-08, not a step, so a cheap exact boundary does not exist. They stay bracketed.
- `tests/test_ingest.py::test_hour_names_and_periods` pins config boundaries, not a docs oracle;
  it was updated to the measured values. Spike oracles and data-audit numbers are unchanged.

## ADR-020 (2026-10-20) Schema eras measured; E1/E2 overlap rule
- E1 starts at file 2012-03-10 09h (E0 ends at 08h, which holds one event; 09h to 21h are empty;
  the first E1 data is 22h). E1 file names are Pacific-local hours (DST included), so the E1 files
  of 2014-12-31 16h-23h cover 2015-01-01 00:00 to 08:00 UTC, which E2 files also cover.
- Rule for rollups (the first phase that aggregates events): event-time windows take E2 rows
  from 2015-01-01 00:00Z and drop E1 rows with `created_at >= 2015-01-01 00:00Z`. Silver keeps
  both, faithful to the source.
- E3 starts at file 2025-10-08 17h. GitHub's payload trim is an instant, 2025-10-08 16:59:22Z
  (`payload_trim_at`): push shape is checked against each event's created_at, not the file era.
- Early E1 `created_at` uses `YYYY/MM/DD HH:MM:SS -0800` until 2012-06-01 09h, and `public` may be
  the integer 0 in 2012-03; both are parsed faithfully, never guessed.

## ADR-019 (2026-10-20) Parser: allow-list projection to typed Parquet, privacy at read time
- One pydantic model per table (strict, extra fields forbidden); the Arrow schema is derived from
  the model, so Parquet types cannot drift from it. One Parquet file per source hour and table
  (`data/silver/gharchive/{events,hour_types}/date=YYYY-MM-DD/`), written atomically, so a re-run
  overwrites (idempotent). Every row keeps `source_file`, `source_sha256`, `fetched_at` and
  `source_line`.
- Privacy: the event row copies only repository-level and event facts. People appear only as
  `HMAC-SHA256(KEYSTONE_ACTOR_SALT, kind:value)`, first 32 hex characters, by account id (E0, E2,
  E3) or lower-cased login (E1, `actor_key_kind = login`); PR and issue authors use the same kind.
  No salt, no run (no default). Bots are flagged (`[bot]` suffix, `config/bots.yml`).
- Quarantine rows hold the reason, line number, line hash and length, never record content.
- Ingest hook: `GhArchiveSink` is `on_file`; `is_done` makes ingest fetch and parse hours that
  were verified before parsing existed (Phase 2), instead of skipping them as unchanged.
- E0 `repo.name = "/"` (no repository) is stored as null; other malformed names fail the contract.

## ADR-018 (2026-10-20) OSV increments: head of modified_id.csv plus ecosystem zips as baseline
- Context: `modified_id.csv` is 151 MB, sorted newest first, and is rewritten often (its ETag
  changed within an hour on 2026-10-20). `all.zip` is 2.78 GB.
- Choice: read the head in 256 KB Range chunks until the stored cursor (nanosecond timestamp
  plus the keys at that timestamp, so ties are handled), using `If-Range` on the first chunk's
  ETag and restarting (at most 3 times) if the file changes mid-read. On the first run, read at
  most 1 MB; the per-ecosystem `all.zip` files (conditional GET, MD5-verified, kept) are the
  baseline. Increments are written to Parquet, named by their modified range; unparseable lines
  are quarantined.
- Measured: the first run read 1 MB (19,144 rows); later runs read one chunk.

## ADR-017 (2026-10-20) GAP period starts at 2025-10-08 17h (measured)
- A Phase 2 HEAD-only scan found that 2025-10-08 17h, 18h and 19h are already near-empty
  (166 to 250 KB), while 16h was 97 MB (era-map). `config/sources.yml` now starts GAP at 17h
  instead of the bracketed "about 20h". Spike oracles are unchanged; spike-report section 4
  carries a dated note.

## ADR-016 (2026-10-20) Ingestion manifest in DuckDB; stream and delete by default
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

## ADR-015 (2026-10-20) gh CLI allowed (owner)
- Owner amended the brief: `gh` may be used on Keystone repos (CI status, runs, issues, PRs).
- Guard: run `gh api user --jq .login` first; proceed only if it prints `azzammasood` (the keyring
  also holds azzamverse). Still never `gh repo create`, visibility or settings changes without asking.

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
- Refined in Phase 7 (ADR-037): bulk lookups, npm cross-check only (PyPI's JSON API is
  disallowed by its robots.txt, ADR-038).

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
