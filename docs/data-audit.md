# Data audit: measured facts and oracles

Every number here is a regression oracle. If a source republishes or changes, surface it; do not
edit the test. Machine-readable oracles: `tests/fixtures/oracles/gharchive_hours.json` (33 hours:
bytes, MD5 ETag, sha256, events, events per type, created_at range). Network check:
`KEYSTONE_NETWORK_TESTS=1 uv run pytest tests/test_oracles.py` (ETags unchanged: passed 2026-10-20).

## GH Archive sample hours (12h UTC unless noted)
| Hour | Events | gz MB | Notes |
| --- | --- | --- | --- |
| 2011-02-12-0 | 1,552 | 0.28 | first hour |
| 2012-06-15 | 5,547 | 1.6 | E1, Pacific offsets |
| 2013-06-15 | 6,954 | 2.2 | E1 |
| 2014-12-31 | 12,057 | 6.7 | last E1 day |
| 2015-01-01 | 8,273 | 2.7 | first E2 day |
| 2017-06-15 | 53,828 | 21.1 | |
| 2019-06-15 | 46,408 | 18.0 | |
| 2021-06-15 | 150,065 | 80.5 | |
| 2023-06-15 | 198,897 | 90.7 | |
| 2024-06-15 | 233,373 | 69.7 | 542 MB JSON |
| 2025-05-15 | 260,758 | | C1 |
| 2025-05-22 | 261,910 | 124 | last C1 Thursday; 943 MB JSON |
| 2025-05-29 | 166,442 | | C2 (-36% against a week earlier) |
| 2025-10-06 / 10-08 | 166,432 / 166,407 | 97.3 / 91.1 | full payloads still |
| 2025-10-15 | 156,052 | 31.8 | first E3 sample |
| 2026-08-23 | 169,788 | 20.7 | C4: 77 PullRequestEvents |
| 2026-10-19 | 46,849 | 8.8 | C5 |

The full list with per-type counts is in the oracle fixture.

## Boundaries (bracketed by HEAD sizes, MB gz)

Evidence: `data/spike/boundary_heads.csv` from `scripts/spike/boundary_heads.py`, which also covers the 2011-02-11 404s and the mirror probes.

- 2025-05-23: 06h 108.1, 12h 125.8, **18h 73.8**; 2025-05-24 06h 57.9.
- 2025-10-08: 16h 97.4, **20h 0.2**; 0.1 to 0.4 MB every sampled hour up to 2025-10-14 20h;
  2025-10-15 00h 34.9 (trimmed payloads).

## OSV (2026-10-20)
`all.zip` 2,782,891,197 B; `modified_id.csv` 151,133,298 B; `npm/all.zip` 218,601,224 B;
`PyPI/all.zip` 36,469,854 B; 46 ecosystems.

## Mirror comparison (open-index/open-github)
| Day | Type | GH Archive | Mirror | Missing in mirror |
| --- | --- | --- | --- | --- |
| 2011-02-12 | CommitCommentEvent | 410 | 410 | 0 |
| 2015-06-15 | ReleaseEvent | 2,146 | 2,113 | 33 |

## Privacy note
E1 events (2012 to 2014) contain `actor_attributes.email` and `name`. The spike scripts counted only
key paths and stored no values. Parsers must drop these fields (see era-map.md).
