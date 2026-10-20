# Phase 4 review: rollup engines

Independent reviewer: a fresh subagent, read-only, no network or Docker, 2026-10-20. Scope:
`7673bd1..37b49f6` (rollup SQL and engines, Avro contract, compose stack, CLI, tests, CI, docs).
Reviewer checks: ruff, mypy and SQLFluff clean; pytest 75 passed / 5 skipped (stack, network);
CLAUDE.md 101 lines; every number in PROGRESS and the ADRs matches the replay log. One major.

## Findings and resolution
| # | Sev | Finding | Resolution |
| --- | --- | --- | --- |
| 1 | major | DuckDB change detection used the raw file's SHA, so a re-parse of the same file (parser, bot-list or salt change) kept stale windows; the test faked a new SHA | Fixed: `rollup_files.fingerprint` = source SHA + size and mtime of the hour's silver files; the test re-parses with the same source SHA (ADR-025) |
| 2 | minor | Late-routing argument stated loosely ("same chained step"); no conservation test | Fixed: the watermark inequality is in ADR-026; stack test asserts `_ontime` + `_late` = filtered input |
| 3 | minor | "Allowed lateness" replaced by late table + fold without saying so | Fixed: stated as a deviation in ADR-026; live mode needs a periodic `rollup fold` |
| 4 | minor | Unbounded stream dropped its tables and read from the earliest offset on every start | Fixed: live mode keeps tables (create if missing) and resumes from group offsets; returns after submit |
| 5 | minor | "Exactly-once" backed by configuration only | Fixed wording: "configured"; a recovery test is a Phase 9 item (ADR-026) |
| 6 | minor | Lakekeeper bootstrap accepts terms of use automatically | Open: owner decision, listed under Blockers in PROGRESS.md (ADR-027) |
| 7 | minor | Redpanda BSL 1.1 licence not recorded | Fixed: ADR-026 |
| 8 | minor | Redpanda Console ran but did no work | Fixed: removed from compose |
| 9 | minor | Images pinned by tag only | Fixed: all images pinned by digest |
| 10 | minor | PROGRESS cited this file before it existed; no CI run evidenced | Fixed: this file; CI result recorded in PROGRESS.md |
| 11 | minor | Parity tests left Iceberg namespaces behind | Fixed: purged through the REST catalog in a fixture |
| 12 | nit | Paths and secret interpolated into SQL unescaped; namespace unvalidated | Fixed: literals escaped or bound; namespace must match `[a-z][a-z0-9_]{0,62}` |
| 13 | nit | `events_path` built by string replace | Fixed: built from partition and file name |
| 14 | nit | CI readiness loop fell through on timeout | Fixed: a final health assertion |

## Recheck
After the fixes: ruff, mypy (strict), SQLFluff and pre-commit clean; 75 tests pass, 5 skipped;
on the restarted (digest-pinned) stack the 4 parity tests pass, including conservation and cleanup.
