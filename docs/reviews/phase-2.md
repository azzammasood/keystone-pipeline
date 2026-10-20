# Phase 2 review: ingestion

Independent reviewer: a fresh subagent with no network access and no write access, 2026-10-20.
Scope: `config/sources.yml`, `src/keystone/fetch/`, `src/keystone/sources/`, the `ingest` CLI and
`tests/test_ingest.py`. The reviewer reproduced findings 1 to 3 with an offline probe built on
MockTransport.

## Findings and resolution
| # | Sev | Finding | Resolution |
| --- | --- | --- | --- |
| 1 | major | `DiskBudgetExceeded` subclassed `FetchError`, so the run recorded it per hour as an `error` and kept requesting; the CLI hard stop never fired | Fixed: it is now a `RuntimeError`, so the run stops and the CLI exits 2. Test `test_disk_budget_is_a_hard_stop` |
| 2 | major | A read error mid-body (`ReadError`, `ReadTimeout`) escaped as a raw httpx exception and killed the whole run | Fixed: `download`/`get_range` convert `httpx.HTTPError` to `FetchError`, which is recorded as an hour `error` and retried next run. Test `test_stream_error_is_an_hour_error_not_a_crash` |
| 3 | major | `.part` replaced `dest` before verification, so a failed re-download deleted the verified copy | Fixed: the `.part` file is verified first and only then replaces `dest`. Test `test_failed_redownload_keeps_the_verified_copy` |
| 4 | minor | An OSV zip error wiped the stored etag and sha256, which forced a full re-download | Fixed: `_carry` keeps the previous verified facts |
| 5 | minor | The first full fetch after a HEAD-only check reported `unchanged` | Fixed: a verified body is compared only with a verified SHA-256, and the first verified copy is `new`/`empty`. The test was updated |
| 6 | minor | The OSV state write was not atomic, and a crash could duplicate Parquet rows | Fixed: the state goes to a tmp file and then `replace`; Parquet files are named by their modified range, so a re-run rewrites the same file |
| 7 | minor | Quarantine lines lacked `source_sha256` and `fetched_at` | Fixed |
| 8 | minor | Hashes were taken after content decoding | Fixed: `Accept-Encoding: identity` and `aiter_raw` |
| 9 | minor | 429/503 slowed only the request that got it, and `Retry-After` as an HTTP date was ignored | Fixed: the host gate is pushed out by the delay, and HTTP dates are parsed |
| 10 | minor | An ecosystem name went straight into a Windows path (`Debian:11` is an NTFS stream) | Fixed: `_safe_dir` |
| 11 | minor | An `on_file` exception aborted the run | Fixed: it is recorded as an hour `error` with the hashes cleared, so the next run fetches the hour again. Test added |
| n1 | nit | OSV `http_status` was hard-coded to 206 | Fixed: the real status of the first chunk |
| n2 | nit | A naive `end` raised `TypeError` | Fixed: `hour_floor(end)` |
| n3 | nit | `ingest osv` never exited non-zero | Fixed: exit 1 on a zip error, exit 2 on the disk budget |
| n4 | nit | Bad lines just above the cursor are quarantined again on every run | Accepted: harmless; none seen live |
| n5 | nit | A robots.txt 3xx is treated as allow-all | Accepted: neither host redirects robots.txt; revisit if a new host does |
| n6 | nit | The DuckDB manifest is locked during a run, so `gaps`/`status` cannot run at the same time | Accepted: documented in ADR-015 |

## Recheck
After the fixes: ruff, mypy (strict) and pre-commit are clean, 27 tests pass (1 network test is
skipped), and the real gate was re-run (2026-10-19: 24 `unchanged`, 0 findings; OSV: 4 new rows
from one 256 KB chunk, PyPI zip `unchanged`). This was attempt 1 of 2.
