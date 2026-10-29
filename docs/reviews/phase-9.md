# Phase 9 review: history backfill, benchmarks, cloud rungs

Independent review by a fresh Claude Code subagent (read-only, no network, no long jobs), dated
2026-10-29. Scope: `src/keystone/backfill/` (plan, chunk, cli, drift), `backfill.yml`; the
reference-set changes in `score/signals.py` and `score/backtest.py`, `backtest.yml`;
`src/keystone/cloud/` (databricks, bigquery, cli, lakeflow SQL), `cloud.yml`,
`.github/scripts/cloud-evidence.sh`, the cloud import step in `publish.yml`,
`telemetry/runlog.py:import_runs`; `map_package_repo.sql` and
`assert_review_decisions_still_apply.sql` (ADR-073); `docs/benchmarks.md` sections 5-7 and
ADR-050, 051, 065-073 (including the uncommitted edits to both docs). Checked with
`pytest tests/test_backfill.py tests/test_score.py tests/test_cloud.py tests/test_workflows.py`
(55 passed, 1 skipped: no openssl) and a planner probe (`plan.chunks` for different `end` and
`chunk_days` inputs). The chunk records are not on this machine, so the section 5 numbers
were checked for internal consistency only. Result: 1 major, 8 minor, 6 nit.

| # | Severity | Location | Finding | Fix |
| --- | --- | --- | --- | --- |
| 1 | major | `backfill/plan.py:68-103`, `backfill.yml:8-9,279-287`, `backtest.yml:44`, `backfill/cli.py:125-135`, `docs/benchmarks.md:229` | `pending` matches only exact chunk names. It never checks which days are already covered, so any dispatch with inputs other than those of the original runs plans chunks that overlap published ones. Verified with the planner: with `end` empty (today), `end=2026-10-29` plans `live 20261001-20261015` and `20261015-20261029` next to the published `20261001-20261014` and `20261014-20261027`. With the defaults (`chunk_days=0`, no `split_from`), all 26 xz and live half months are planned again as whole months. Both copies are kept: `backtest.yml` downloads `repo_hour-*.parquet` and `repo_actor_month-*.enc` by glob, so xz events and maintenance actions would be counted twice without any warning. `backfill totals` would also count the days twice. ADR-071 avoids this only by remembering the exact inputs. The workflow header ("re-dispatching continues where a cancelled or failed run stopped") and section 4 ("the live window then grows by one day a day (one short job)") describe behaviour the code does not have. | Parse `<start>-<end>` from the published asset names. Have `plan` drop or refuse any chunk that overlaps a published chunk of its window, unless it exactly replaces an incomplete one. The live tail needs an explicit "replace tail" step that deletes the superseded assets. Make `score.signals.open_inputs` (or the backtest download step) fail on overlapping chunk ranges. Add a planner test: plan with `end=D`, then with `end=D+2` and with `chunk_days=0`; nothing pending may overlap the published chunks. Fix the two doc sentences. |
| 2 | minor | `backfill/chunk.py:364-389`, `:431`, `:442` | Retry passes (ADR-072) return their summaries to nowhere. `hours.status` (counts per status), `download.bytes` and `download.requests` come from the first pass only. A chunk whose retries succeeded therefore has `status: {"error": n}`, empty `fetch_errors` and `complete: true` in one record, and the totals ("downloaded GB" in benchmarks section 5) leave out the retried downloads. | Accumulate the retry summaries into `summary`, or derive `status` from the final manifest. Note in benchmarks whether retry bytes are included. |
| 3 | minor | `backfill/chunk.py:146-150`; ADR-050 | The published `contributors` asset has `top_actor_events` (the largest single person's daily event count per repository) and `human_actors`. Where `human_actors` is 1 or 2, the row shows one identifiable person's activity per repository and day. That is a per-person value, not an "aggregate share" (CLAUDE.md privacy rule), even though GH Archive is public. | Publish a top-actor share (`top_actor_events / human_events`), or suppress `top_actor_events` when `human_actors` is below a small k (for example 3). Record it in an ADR. |
| 4 | minor | `backfill/chunk.py:207-210`, `:434` | `parse_errors` copies raw exception text (up to 400 characters) into the public chunk JSON. A whole-hour failure that is not quarantined (for example a pydantic `ValidationError`, which prints `input_value=...`, or a DuckDB error) can include record content: logins, or commit author emails in E2 push payloads. None of the final records seems to have any, but nothing prevents it. | Store `type(exc).__name__` plus a fixed reason category in the record. Keep the full text only in the job's private work directory (or drop it). |
| 5 | minor | `map_package_repo.sql:43-52`, `assert_review_decisions_still_apply.sql:5-17`, ADR-073 "Why" | (a) The join `d.candidate = 'github.com/' \|\| i.repo_name_lower` does not lower-case the decision, so a reviewer who types `github.com/Owner/Name` matches nothing. All 48 current decisions are lower case. (b) When a name is later reused by another repository, `int_repo_identity` maps the name to the new id, and the decision silently moves to a repository nobody reviewed. ADR-073 says "a later rename or id change keeps working", which overclaims. (c) A decision by name and one by id for the same candidate become two rows with the same key. `unique_combination` then fails the daily publish with a message that does not point at the seed. | Use `LOWER(d.candidate)` in both places. Record the id the name resolved to when the decision was made (or test that a name-keyed decision's id has not changed). Add a seed test: unique `(subject, subject_key, candidate_key)` after normalization. Reword the ADR. |
| 6 | minor | `docs/benchmarks.md:242`, `:252` | "Peak RSS stayed at or under 8.5 GB (self) in every chunk after the 8 GB merge cap". The month rung table above it lists colors-faker 2021-02 "(ADR-066 cap ...)" at 9,115 MB, and the xz half month at 8,511 MB, which is over 8.5 GB. | State the measured maximum after the cap, with its chunk, and mark which rows predate the cap. |
| 7 | minor | `score/signals.py:289-290`, `score/backtest.py:174-180`; benchmarks section 5 (2021-10-22 to 10-29 gap, 171 h) | The colors-faker cut (2022-01) passes the 90% recent-coverage gate (2,208 hours less 171 is 92.3%). `recent_events` then lacks about 7.7% of its hours, while `prior_events` lacks much less, so the activity trend leans towards "declining" because of a source gap. This is the residual of phase-8 finding 6. | Normalise `recent_events` and `prior_events` by the covered hours of each sub-window (both are already counted by `hour_coverage`). Or report the coverage of each sub-window beside the trend in the backtest report. |
| 8 | minor | `cloud/databricks.py:201-217`, `:92-94`; `cloud/bigquery.py:143-144` | On timeout, `lakeflow` returns a non-COMPLETED state but leaves the pipeline update running on serverless compute. That draws on the Free Edition daily quota after the job has given up. `Workspace.sql` and BigQuery's `getQueryResults` loop have no deadline (bounded only by the job timeout). | Call `POST /api/2.0/pipelines/{id}/stop` when the loop times out. Give both poll loops a deadline, and cancel the statement or job when it passes. |
| 9 | minor | `backfill/chunk.py:376-379`, `:406`, `:415`; `backfill/drift.py:214-227` | Fetch errors in the 2 overlap hours (which belong to the next chunk) mark this chunk incomplete. The overlap hours are also parsed by two chunks, so drift findings in them are counted twice in `backfill drift`. Both are conservative, but the record does not say so. | Count only in-chunk hours in `fetch_errors` and `drift`, and list overlap-hour errors separately. |
| 10 | nit | `cloud/bigquery.py:6` | The docstring says the month's usage comes from INFORMATION_SCHEMA. The code (and ADR-068) use `jobs.list`. | Update the docstring. |
| 11 | nit | `telemetry/metrics.py:50-52` | The engine roles for `databricks-sql`, `dbt-databricks` and `databricks-lakeflow` do not say that they ran on the CI fixtures (547 rows). `engines[]` is evidence a reader may take as production use. | Add "(CI fixtures)" to these roles, as benchmarks section 7 already says. |
| 12 | nit | `cloud/lakeflow/marts_pipeline.sql:17` | `workspace.marts.fct_repo_day` hard-codes the catalog, while `DATABRICKS_CATALOG` is configurable. | Use the pipeline's catalog (unqualified `marts.fct_repo_day`) or a pipeline configuration value. |
| 13 | nit | `cloud/bigquery.py:189-210` | `max_rows=5_000_000` silently truncates the export. Nothing checks `exported_rows` against `totalRows`. | Fail (or record `truncated: true`) when they differ. |
| 14 | nit | `backfill/chunk.py:352-362`; `backfill.yml:371-376` | When `pool.submit` raises after `slots.acquire()`, the slot is never released, and enough such failures stall the fetch. `KEYSTONE_ACTOR_DATA_KEY` is only checked by `actor-data.sh`, after a 30-50 minute chunk has run. | Release the slot in an `except` around `submit`. Check both secrets in the "Production salt is set" step. |
| 15 | nit | `backfill.yml:386`, `publish.yml:92` | A lost line continuation left long runs of spaces inside one command line. It works, but it is hard to read. | Restore the `\` continuation. |

Checked and fine (reviewer):
- **Filtered reference set equals the unfiltered one.** The narrow aggregate
  (`human_events > 0`, `SUM >= min`) selects exactly the repositories whose
  `SUM(human_events)` in the wide query reaches the threshold: human events are non-negative,
  and nulls and null `repo_id` are excluded both ways. `keep` adds the targets.
  - Repositories that appear only in actor data are dropped in both modes.
  - The actor and vulnerability queries use the same scope.
  - `test_filtered_compute_equals_the_reference_of_the_full_compute` checks set and values for
    three thresholds.
  - The leakage check still runs on the targets with the unfiltered path.
- **Point in time.** Every event filter is half-open `[cut - 12 months, cut)`. Actor months count
  only when they end by the cut.
- **Chunk merge.** Partials are keyed by hour file stem, so an hour that is retried and parsed
  again overwrites its partial and is not summed twice. The merge keeps `[start, end)` only, and
  edge events are counted, never assigned.
- **Superseded records.** `record_files` counts a redone chunk once.
- **Actor keys.** Salted keys leave the runner only AES-encrypted. The plaintext and the partials
  stay in `$RUNNER_TEMP/work` and are deleted. `backtest.yml` uploads only the report and its
  JSON. Workflow inputs reach shell steps through `env`, never inline.
- **BigQuery spend.** Each query has a dry run first. The 5 GB per-query cap, the 20 GB run
  budget and the 100 GB monthly stop come next, and the job stops when the month's usage is
  unknown. Each real job sets `maximumBytesBilled`. The project has no billing account. The
  token comes from WIF (30 minutes) and is sent only in a header. Error text holds only the API
  error object.
- **Databricks.** The token is sent only in a header. Errors carry the path and the response
  text. Free Edition has no billing path.
- **Cloud evidence.** `engines()` counts only stages with status `ok`. A failed Lakeflow stage is
  recorded as failed. `import_runs` deduplicates by `(run_id, stage, started_at)`. The evidence
  upload runs only on success.
- **ADR-073 key fan-out.** `int_repo_identity` is unique on `repo_name_lower`, so the decision
  join cannot multiply rows, and `unique_combination` on `map_package_repo` would catch it if it
  did.
- **Section 5 arithmetic.** Every column of the year-rung table adds up to its total:
  - 78 chunks, 1,579 days (live 391 days to the pinned end 2026-10-27);
  - 4,574,059,729 events, 1,873.2 GB downloaded, 2,342,977,429 / 846,599,327 / 684,733,178
    rows;
  - 7.42 + 2.39 = 9.8 GB published, 37.88 job hours, 228 missing hours (225 = 54 + 171);
  - the ratios: 781.1 / 286.3 = 2.7x, and 1,257.6 / 1,678.1 = 75% of the events.
- **Section 6 bytes.** They are consistent: every 8-byte column adds rows x 8 (218,938 and
  1,042,693 rows).
- **Wording.** The docs and ADRs use "signals", and misses are reported as misses. No
  predictions.

Verdict: **not clean**. Fix finding 1 before the next backfill dispatch or backtest run:
without that fix, one dispatch with default inputs would silently double the xz and live
history. Findings 2-9 are small. Fix them, or document them in PROGRESS.md and leave them for
later, at the owner's choice.

## Resolution (2026-10-29, main session)

1. Fixed (ADR-074):
   - `plan.pending` refuses a pending chunk whose days overlap a published chunk under another
     name, and `backfill plan` exits 1 with the reason (tested).
   - The `backfill.yml` defaults are now the published layout (`chunk_days=16`,
     `split_from=2023-01-01`, `end=2026-10-27`), so a bare re-dispatch plans 0 chunks.
   - The stale "grows one day a day" sentence is corrected.
2. Open (minor): the retry passes' counts are not folded into the record's status and bytes
   counts. The `fetch_errors` and `complete` fields come from the final manifest state and are
   right.
3. **Owner decision (privacy):** `top_actor_events` in the published `contributors` assets.
   Changing it means rewriting 78 published chunks, so it is not done unilaterally. Listed in
   PROGRESS.
4. Fixed: `parse_errors` keeps the exception type only, never its text. The published records
   so far have none.
5. Fixed: the name match is lower-cased. The reused-name and duplicate-decision cases stay as
   documented limits of the seed (ADR-073).
6. Fixed: the peak RSS sentence now gives the maximum per window from the records (event-stream
   9,474 MB, before the cap).
7. Documented: the 2021-10 source gap (171 h) is in `docs/benchmarks.md`. The colors-faker point
   is not scored anyway (30% of the weight available).
8. Partly fixed: a timed-out Lakeflow update is stopped. The statement and BigQuery polling
   loops still have no overall deadline (the job timeouts bound them).
9. Open (minor): overlap-hour accounting of errors and drift.
10. Fixed: docstring. 11. Fixed: roles say "CI fixtures".
12-15. Open nits, recorded.

Verdict after fixes: clean for the gate. The only major finding is fixed; the open items are
minor or nits, and one privacy item awaits the owner.
