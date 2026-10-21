# Phase 5 review: lakehouse and warehouse

Independent reviewer: a fresh subagent, read-only, no network or Docker, 2026-10-20. Scope:
`0af9368..a3748d9` (OSV silver, dbt project, warehouse CLI, Iceberg publish and engine reads,
Spark and Trino services, parity workflow, tests, ADR-028 to 030). Reviewer checks: ruff and
mypy clean; `tests/test_warehouse.py` 16 passed; every mart count in ADR-028 and PROGRESS
matched the local warehouse; every CLI flag used in `parity.yml` exists; no person-level
column in `marts` or `metrics`. Two major findings.

## Findings and resolution
| # | Sev | Finding | Resolution |
| --- | --- | --- | --- |
| 1 | major | The name history read silver without the E1/E2 overlap rule, so 58,769 E1 copies (2015-01-01 00:00 to 08:05Z) were counted next to their E2 events | Fixed: the ADR-020 filter is in `int_repo_name_hours`, pinned by a dbt unit test; `dim_repo` events now equal `fct_repo_day` (2,094,765); 809,212 repositories (was 810,117) |
| 2 | major | OSV repeats a record in every ecosystem zip it affects; with more than one zip parsed, `vuln_id` would be duplicated and the build would fail | Fixed: staging keeps one copy per id (latest `modified_at`, then `source_file`) and only its affected rows and pointers; the pytest build parses the fixture zip twice |
| 3 | minor | The fingerprint ignored string, timestamp and date values (non-null counts only) | Fixed: distinct count of every non-integer column and summed character length of strings; the remaining limit (a constant time shift) is stated in ADR-029 and the wording in PROGRESS is "identical fingerprints" |
| 4 | minor | `verify --namespace` passed silently on an empty namespace | Fixed: an empty requested namespace fails the check |
| 5 | minor | Seed `column_types: varchar` would fail on Databricks | Fixed: `string` when `target.type == 'databricks'` |
| 6 | minor | Spells split on case-only changes; `last_seen_at` can pass `valid_to` | Fixed: names compared case-insensitively (spelling of the latest sighting kept); the within-hour overlap is documented in the model and its YAML; a unit test covers case change and A to B to A |
| 7 | minor | `parity.yml` path filters missed fixtures, `config/sources.yml`, `pyproject.toml` and `uv.lock` | Fixed: added |
| 8 | minor | Test gaps: spells, UTC day cut, per-type pivots, overlap, multi-ecosystem OSV | Fixed: three dbt unit tests (spells, overlap, UTC cut and pivots) and the duplicate-zip build |
| 9 | nit | `stg_ref__schema_eras` was unused | Fixed: removed (the seed stays, checked against `sources.yml`) |
| 10 | nit | GitLab subgroup URLs are cut to two path parts | Noted in the parser; only github.com rows get a repository id |
| 11 | nit | `parse osv --zip` uses the file's mtime as `fetched_at` | Kept: documented in the help text, as for `parse files` (fixtures, CI) |
| 12 | nit | The four-engine match was measured as two local runs; no CI run evidenced | The parity workflow runs all four engines together; its run is recorded in PROGRESS.md |

After the fixes: 73 dbt nodes pass on the local data, docs coverage 100%; the ten tables match
in DuckDB+Spark+Flink (30/30) and DuckDB+Trino (20/20) with the stronger fingerprint.
