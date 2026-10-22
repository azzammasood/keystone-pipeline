# Phase 7 review: entity resolution

Independent review by a fresh Claude Code subagent (read-only, no network, no long jobs), dated
2026-10-21. Scope: the uncommitted Phase 7 working tree: the polite client (RFC 9309 robots,
JSON POST, quota floor), `sources/registry.py`, `resolve/`, the `resolve` CLI, the dbt
resolution models, seed, tests and unit tests, the registry fixtures, `config/policy.yml`,
`config/metadata.yml`, `telemetry/metrics.py`, the CI workflows and the draft of ADR-037/038.
Claims were checked with read-only queries on the local warehouse (UTC) and `pytest
tests/test_resolve.py`, ruff and mypy. Result: 3 major, 6 minor, 4 nit; all fixed.

| # | Severity | Finding | Resolution |
| --- | --- | --- | --- |
| 1 | major | npm registry and ecosyste.ms were counted as two independent sources, but ecosyste.ms derives its npm `repository_url` from the same registry field. 2,313 of 2,589 `auto_exact` rows rested on that pair alone; a wrong `repository` field would become `auto_exact`. | Fixed: the two form one registry family; `auto_exact` needs the registry and an OSV GIT range (276 rows); npm-only agreement is `single_source` (`accepted`). Unit test updated. |
| 2 | major | `unique_name_id_existed` accepted 11,549 name-hours (33,146 events): an id existing at the hour says nothing about its name then (median gap 2.5 years; 332 hours where the id had another name first, 6 contradictions). | Fixed: the rule is `suggested` (review CSV); unit test updated. |
| 3 | major | A stored language-model choice was applied to every tied candidate without checking it was asked about the current tie; after a refresh or a key change every tied candidate became `rejected`. | Fixed: `llm` applies only when the choice is a current tied candidate and the stored candidate list equals the current tie; else `tie`. Unit test with a stale choice. |
| 4 | minor | A reviewer's `rejected` still counted in the rival tallies, so the remaining candidate could be `outvoted` and the package left with nothing. | Fixed: tallies run over candidates still in play; unit test. |
| 5 | minor | Reviewed decisions are keyed on `repo_key`, which changes when an id becomes known; a decision could be orphaned silently. | Fixed: dbt test `assert_review_decisions_still_apply`; the review CSV carries `repo_id`. |
| 6 | minor | `dim_repo_rename` paired every previous with every current name across fetches: a self-rename and a conflicting rename for one id with two current names. | Fixed: staging keeps the newest fetch per id; ids with more than one current name are skipped; `from_name <> to_name` plus a dbt test. |
| 7 | minor | A bulk-lookup record that could not be tied to a package was quarantined while its package was written as a measured absence (`found = false`). | Fixed: a batch with such a record writes no absences (asked again next run); test. |
| 8 | minor | The report's DuckDB connection used the host time zone for hour cuts and the 2015 era bound. | Fixed: `SET TimeZone = 'UTC'`; the Markdown no longer hard-codes "skipped". |
| 9 | minor | `EXISTS` in a SELECT list (`dim_repo_rename`) is a Databricks portability risk. | Fixed: LEFT JOIN. |
| 10 | nit | The LLM-choice test used a substring match (`github.com#12` inside `github.com#123`). | Fixed: delimited match. |
| 11 | nit | The id ceiling counted ids first seen later in the same hour. | Fixed: only hours before the event hour. |
| 12 | nit | "Sole" FIX references counted OSV spellings, not normalised packages. | Fixed: counted after normalisation in `int_package_repo_claims` (Phase 5's bridge unchanged). |
| 13 | nit | The review CSV has more columns than the seed; `MIN(id_rule)` picked a rule alphabetically when names merged. | Fixed: docs say to copy the first six columns; the current name's rule is used. |

Checked and fine (reviewer): no package has two decided rows; `(package_key, repo_key)` and
`int_repo_identity` are unique; the ecosyste.ms id on claims never contradicts the identity
rule; the allow-list keeps maintainers and emails out of silver, the LLM context and the CSVs;
off-list or out-of-range model answers are refused; robots matching follows RFC 9309; the
quota floor is checked before every request; `--replay` refuses `data/`; the E1 overlap filter
matches ADR-020; the PEP 503 macro equals Python.

After the fixes (local data, 2026-10-21): 123 dbt nodes pass, docs coverage 100%; 4,383 of
4,736 packages decided (92.5%), 276 `auto_exact`; 155 package candidates and 4,054 names in
the review CSVs (docs/match-rate.md).

Found after the review, in the first parity run on GitHub (run 39651514424): Flink could not
read `map_package_repo` because its column `scope` is a reserved word in Flink SQL and the
cross-engine fingerprint query does not quote identifiers; the script stopped there, so every
later table failed too (DuckDB, Spark and Trino matched). Fixed: the column is
`package_scope`, and a test fails on any published column named like a Flink reserved word.
