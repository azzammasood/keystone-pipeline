# Phase 8 review: scoring and backtest

Independent review by a fresh Claude Code subagent (read-only, no network, no long jobs), dated
2026-10-22. Scope: commits `203f7ba..HEAD`: `src/keystone/score/` (model, signals, backtest, cli),
`tests/test_score.py`, `config/incidents.yml`, `docs/backtest.md`, ADR-044, checked against
CLAUDE.md, `docs/era-map.md` and the rollup SQL (`src/keystone/rollup/sql/`). Checked with
`pytest tests/test_score.py` (12 passed), `keystone score backtest --help`, column types of the
local rollup and warehouse (read-only) and a DuckDB probe. Result: 2 major, 6 minor, 3 nit.

| # | Severity | Location | Finding | Fix |
| --- | --- | --- | --- | --- |
| 1 | major | `signals.py:286`, `:294-296`; `rollup/sql/repo_actor_day.sql:14` | Contributor churn counts `events`, which is every human event type, including `WatchEvent` and `ForkEvent`. "Contributors of months 1-6" are mostly people who starred or forked once, so churn is near 1 for any popular repository. The reason text ("contributors ... not seen") then describes something that was not measured. The 3-actor minimum is met trivially. | Define a contribution as push + PR opened/closed + issue opened/closed + release + comments. These columns are already in `repo_actor_day`; carry `prs_opened`, `issues_opened` and `comments` through `ACTOR_MONTH_FROM_DAY`. Use that sum for `e1`/`e2`. Comments drop sharply in E3 (era-map), so leave them out or version the metric. Add a test where stargazers alone do not create churn. |
| 2 | major | `signals.py:171-182`, `:318-330`; ADR-044 "Why"; `backtest.py:125-158` | Backtests use today's snapshots beyond the dependents count: (a) `map_package_repo` is today's mapping, so a package published after the cut still makes `vulnerabilities` available (often with 0 advisories, which lowers fragility); (b) `severity_label` is OSV's current label; (c) the advisory set and its aliases are today's. The runtime leakage check renames only `repo_hour`, `repo_actor_month` and `vulns`, using the same predicates as `compute`, so it cannot see any of this. ADR-044 still says "no number that depends on data from after the cut", which overclaims. | Restrict `mapped` in backtests to packages whose first OSV/registry evidence predates the cut, or make `vulnerabilities` unavailable in backtests the way criticality is. Name the remaining snapshot inputs (mapping, severity) in ADR-044 and in the report header. Reword the leakage check as "event-time filters verified" in the report, not a general no-leakage proof. |
| 3 | minor | `signals.py:248-251`; `rollup/sql/repo_hour.sql:61-62` | `opened`/`closed` in `repo_hour` include bots (only `human_events` is bot-filtered). Dependabot PR opens push the responsiveness proxy towards fragile, and stale-bot closes push it towards healthy. | Compute opens and closes from the bot-free `repo_actor_month` (`prs_opened + issues_opened`, `prs_closed + issues_closed`), or add human-only measures to `repo_hour` as a versioned rollup change. |
| 4 | minor | `signals.py:322-323`; `dbt/models/marts/fct_vuln.sql` | `COUNT(DISTINCT vuln_id)` counts aliased records separately. In PyPI, a PYSEC and a GHSA record for the same flaw count as 2. `MAL-` malicious-package records count as advisories too. | Count distinct alias groups (or keep one record per alias set), and leave `is_malicious` out of the count or report it separately. Add a test with two aliased records. |
| 5 | minor | `backtest.py:66-74`, `:195` | The flag percentile compares the target with the active (>= 100 human events) population, but the target is not in that population when it is below 100 events. The target also usually has `vulnerabilities` available (it is mapped), while most reference repositories are unmapped and lack that 0.15 weight. The ranks therefore compare different component sets. | Rank within repositories that have the same available component set (or at least the same mapped/unmapped class), and include the target in the population. Report the reference coverage beside the percentile. |
| 6 | minor | `backtest.py:176`, `signals.py:245-246`; `model.py:118` | The 90% coverage threshold applies to the whole window. Up to 36 missing days in months 10-12 would read as a falling activity trend and a slower close rate. | Require coverage in each sub-window (months 1-9 and 10-12), or normalise the counts by covered hours. Add a test with a gap only in the last 3 months. |
| 7 | minor | `config/incidents.yml:12-42`, `backtest.py:3-5`, `docs/backtest.md` | The incident month is the disclosure month, not the start of the compromise. The event-stream window (to 2018-11-01) contains the September 2018 handover and the October malicious release. The xz window (to 2024-03-01) contains the backdoored 5.6.0 release (late February 2024). A "flagged" result can therefore reflect the incident itself, and it reads as early warning. | Record a `compromise_month` per incident (with a source) and treat the point before it as the headline, or say in the report, per incident, that the cut follows the compromise. Run `--months-before` points that predate it. |
| 8 | minor | `signals.py:217-222`, `backtest.py:207`, `cli.py:38-39` | `_scope([])` calls `executemany` with an empty list. DuckDB raises `executemany requires a non-empty list` (verified). The crash happens when every incident repository is unresolved (for example a removed repository without name history), and in `score live` when nothing is mapped. | Return a filter that matches nothing (`AND false`) when the list is empty. Skip the leakage check with status "no repository resolved". Add a test. |
| 9 | minor | `tests/test_score.py:82-104`, `:173-184` | Test gaps: (a) poison touches only repo 1, so a leak in the reference population is not exercised; (b) the `maint_v2`/`maint_v0` SQL is never run on data (only `maint_version` labels are tested), and nothing checks that `prs_merged` is excluded in E3; (c) `resolve_name` (removed repository, name reused before or after the cut) has no test; (d) the `repo_actor_month` input path and the `actor_data=False` path through `compute` are untested; (e) no test covers bot-only opens/closes or star-only actors (findings 1 and 3). | Add a poison row for a healthy repository and assert identical reference fragilities; add E3 and mixed-era windows with PR rows; add a `dim_repo_name_history` fixture; add cases for each input path. |
| 10 | nit | `model.py:77`, `:95`, `:116`, `:133` | Reason strings hard-code thresholds ("fewer than 10 ...") instead of reading `rules`, so a changed rule would be explained wrongly. | Format them from `rules.min_*`. |
| 11 | nit | `model.py:162-173` | `members_added` is always "available" (sub 0 when there is no MemberEvent, including E0 windows where the payload lacks the member) and adds 5% coverage to every repository. | Mark it unavailable for windows touching E0, and leave it out of the coverage sum, or document it. |
| 12 | nit | `cli.py:123-128`, `docs/backtest.md:3,8` | `--incident` with a subset or an unknown id silently overwrites the full `docs/backtest.md` (empty when no id matches). The generated report mixes wrapped and very long lines. | Error on unknown ids; write the default report only for full runs. Wrap the generated paragraph text consistently. |

Checked and fine (reviewer):
- Every event query is half-open `[cut - 12 months, cut)`, and `repo_actor_month` keeps only
  months ending by the cut.
- Columns are TIMESTAMPTZ in the real rollup and warehouse, and the connection is UTC.
- Advisories withdrawn after the cut are restored.
- `maint_version` follows the era seed, so E2 and E3 windows use `maint_v0`, and the live window
  correctly loses the activity trend (completeness periods).
- Criticality and risk are never computed in backtests.
- No actor key reaches the JSON or the report: only aggregate counts and shares.
- The wording keeps to "signals", and misses are reported as misses.
- The "pending data" report is honest about having no backfill.
- Sensitivity has 22 variants, as stated.

## Fixes (2026-10-22)
1. Fixed: churn counts actors with maintenance actions (same version as concentration) in each
   half, so stars, forks and comments never make a contributor; test data adjusted.
2. Fixed (documented): ADR-044 and the report name the snapshot inputs (package mapping, OSV
   severity labels and alias sets) and no longer claim that no number depends on later data;
   the runtime check is described as a check of the time filters.
3. Documented: the responsiveness reason says "bots included" (`repo_hour` opens and closes are
   not split by bot; a rollup change is a new metric version, left for later).
4. Fixed: malicious-package records are left out (`NOT is_malicious`, tested); alias double
   counting is documented (fct_vuln keeps no alias list).
5. Fixed: targets always join the percentile population; mixed component sets are documented.
6. Fixed: a point is scored only when both the window and its last 3 months hold >= 90% of hours.
7. Documented in the report header: the cut can fall after the compromise began.
8. Fixed: an empty id list scopes to nothing (tested in `compute` and `criticality_inputs`).
9. Added tests: `repo_actor_month` Parquet input path, an E2/E3 window using `maint_v0`, empty
   scope, malicious record left out. `resolve_name` remains covered only by the CLI path.
10. Fixed: reason strings read the thresholds from the rules.
11. Fixed: members_added is unavailable for windows touching E0.
12. Fixed: `--incident` subsets no longer overwrite `docs/backtest.md`.
