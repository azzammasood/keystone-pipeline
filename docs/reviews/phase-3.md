# Phase 3 review: parse, contracts and quality

Independent reviewer: a fresh subagent, read-only, no network, 2026-10-20. Scope: `e593a10..HEAD`
(parsers, quality, ingest `is_done`, CLI, config, contracts, drift registry, tests, fixtures, CI,
docs). Reviewer checks: pytest 65 passed / 1 skipped, ruff and mypy clean; 13,145 fixture strings
scanned, all placeholders, enums or timestamps (no real email, login or text); the drift registry
holds key paths and type names only; all action SHAs pinned. No major findings.

## Findings and resolution
| # | Sev | Finding | Resolution |
| --- | --- | --- | --- |
| 1 | minor | Missing golden hours skip silently, so a cache saved after a failed fetch disables golden tests | Fixed: `KEYSTONE_GOLDEN_REQUIRED` in CI lists the 5 hours; the test fails if any is absent |
| 2 | minor | `is_done` checks the events file, written first; a crash before hour_types left an hour "done" | Fixed: events are written last |
| 3 | minor | A late pre-trim event without `payload.size` in an E3 file would fail its hour forever | Recorded in ADR-022 (none seen in sampled E3 hours); Phase 9 measures before the backfill |
| 4 | minor | `parse gharchive`/`parse files` stopped the batch on any non-drift error | Fixed: other exceptions are counted as failed and the batch continues (`failed=` in output) |
| 5 | minor | `learn-drift` learned shapes from hours with quarantined records | Fixed: such hours are refused |
| 6 | minor | Trim instant duplicated in the events contract and config | Fixed: `test_contract_trim_instant_matches_config` |
| 7 | minor | Docs numbers: registry lines 1,420 (file has 1,468); "49 of 123" unexplained; "152 hours" vs 151 in the table | Fixed from the run outputs: 1,468 at end of phase; 123 = hours kept at that point; 152 parsed hours, one without events |
| 8 | nit | Byte searches on zstd Parquet tested little; drift test never seeded the token it checked | Fixed: decoded quarantine values are checked; the drift mutation now seeds `someone@example.invalid` |
| 9 | nit | `quality contracts` passed when a table had no rows | Fixed: `--require-all`, used in CI; CI parses a committed quarantine fixture |
| 10 | nit | With `--keep`, a pending hour is fetched again despite a verified local copy | Accepted: rare (catch-up only); `parse gharchive` parses kept copies without a fetch |
| 11 | nit | E1 `date=` partition and `hour_types.hour` follow the Pacific file label | Documented in era-map.md: event-time windows use `created_at` |
| 12 | nit | Drift walk is most of parse time | Recorded in data-audit.md (25k-27k events/s); Phase 9 parallelises or samples |

## Recheck
After the fixes: ruff, mypy (strict) and pre-commit clean; 66 tests pass, 1 network test skipped
(golden hours required and present); `parse files` on the fixtures plus `quality contracts
--require-all` passes all 3 contracts (108, 36, 30 checks). CI result is recorded in PROGRESS.md.
