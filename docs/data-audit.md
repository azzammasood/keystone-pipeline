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

## Phase 3 parse (2026-10-20)

Sample set (all downloads deleted afterwards except the 8 oracle hours used by the golden tests):
full days 2011-02-12 (E0), 2012-03-10 22h to 2012-03-11 21h, 2012-06-15, 2014-12-31 (E1),
2015-01-01 (E2), 2026-10-19 (E3), plus hours 2012-03-10 08h, 2013-06-15 12h, 2019-06-15 12h,
2025-05-23 16h, 2025-10-06 12h, 2025-10-08 16h (E2) and 2025-10-10 12h-13h (E3, near-empty).

| Era | Files with events | Events | Event types | Quarantined | Drift |
| --- | --- | --- | --- | --- | --- |
| E0 | 25 | 32,198 | 15 | 0 | 0 |
| E1 | 72 | 477,389 | 18 | 0 | 0 |
| E2 | 28 | 808,680 | 15 | 0 | 0 |
| E3 | 26 | 1,045,159 | 16 | 0 | 0 |
| All | 151 | 2,363,426 | | 0 | 0 |

- Golden: the 8 kept oracle hours parse to exactly the oracle event counts, per-type counts and
  created_at range (UTC), with 0 quarantine and 0 drift (`tests/test_golden.py`).
- Drift baseline experiment: a registry learned from 6 oracle hours (one per era step) flagged 49
  of the 123 hours kept at that point (the 6 baseline hours among them) (rare event types such as ForkApplyEvent and PullRequestReviewEvent, fields
  seen only as null in the baseline). After learning all samples (1,468 lines in
  `contracts/drift/gharchive.yml` at the end of Phase 3), 2025-05-23 16h, an unlearned E2 hour, parsed with no drift.
- Throughput (one core, E3 hour 2026-10-19 12h, 46,849 events): 25k to 27k events/s with drift
  checks; row building is about 40% of the time and the drift walk most of the rest. Phase 9
  must parallelise across hours or optimise (raw orjson parse is 315k/s, spike-report section 3).
- Quality over all sample output: 3 ODCS contracts pass (108 + 36 checks; quarantine had no rows),
  Great Expectations passed on 304 Parquet files (events and hour_types for 152 parsed
  hours; one of them, an early-E1 hour, has no events, so the table above counts 151).
- Boundaries measured (sources.yml, ADR-020/021): E1 from file 2012-03-10 09h (first data 22h);
  created_at format switch at 2012-06-01 09h; E3 from file 2025-10-08 17h with the push trim at
  16:59:22Z; C2 from 2025-05-23 16h (capture falls from about 16:35Z: 4.1k to 2.5k events per
  minute); GAP to 2025-10-14 20h, C3 from 21h (HEAD: 20h 0.25 MB, 21h 30.5 MB).

## Privacy note
E1 events (2012 to 2014) contain `actor_attributes.email` and `name` (also `blog`, `company`,
`location`). The spike scripts counted only key paths and stored no values. The Phase 3 parser is an
allow-list projection and copies none of them; the committed fixtures are scrubbed (every string
and id replaced by a counter-based placeholder, emails by `redacted@example.invalid`).
