# Benchmarks (Phase 9 preparation)

Measured 2026-10-21 on this machine (Windows 11, 8 logical CPUs, 16.8 GB RAM with about 3.2 GB
available, Docker Desktop VM 8.2 GB on the WSL2 kernel 5.15 with cgroup v1; disk 25.5 GB free,
budget 17.9 GB). Commands: `keystone bench run` (scale ladder) and `keystone bench storage`.
Raw results: `data/bench/results.jsonl` and `data/bench/storage.json` (git-ignored).
These numbers are regression oracles: if a rerun differs, surface it; do not edit them.

**Labels.** *Measured* = bytes, rows and seconds observed on this machine. *Estimate* =
projected from measurements by the method stated next to it. Every projection is an estimate.
The backfill was **not** run.

## 1. Scale ladder: one rollup workload, three engines

Workload: the two Phase 4 rollups (`repo_hour`, `repo_actor_day`) from `src/keystone/rollup/sql`
(written once), on identical input: the scope's silver hours projected to the Avro event
contract and written once as one Snappy Parquet file (not timed). Each engine writes its
rollups as Parquet; the harness counts rows, bytes and the sum of every count measure.

| engine | how it runs | peak memory measured as |
|---|---|---|
| DuckDB 1.5.6 | child process (`keystone.bench.duck_worker`), all 8 threads | child's peak working set |
| Spark 4.1.3 | fresh `spark` container, `local[2]`, driver 768 MB, limit 1200 MB | container cgroup peak |
| Flink 2.2.1 | batch mode, freshly created jobmanager + taskmanager (4 slots), limits 1200 + 1800 MB; file source and sink, TUMBLE wrapper of the full engine | sum of both containers' cgroup peaks |

Wall time is from the host and includes start-up (container and JVM for Spark; the SQL client
for Flink, on a cluster already up). cgroup v1 `max_usage_in_bytes` includes the page cache, so
container peaks are upper-side. Cost: **$0** for every run (local machine, no billed service).
Output bytes differ by codec: DuckDB and Spark write zstd, Flink its default (Snappy, INT96
timestamps).

| rung | input rows | input MB | engine | wall s (run 1 / run 2) | engine s (run 1 / run 2) | peak MB (run 1 / run 2) | rows out | MB out |
|---|---:|---:|---|---:|---:|---:|---:|---:|
| 1 hour (2026-10-19 12h) | 46,849 | 1.7 | DuckDB | 2.9 / 1.6 | 0.5 / 0.3 | 86.1 / 86.0 | 60,114 | 0.8 |
| | | | Spark | 58.2 / 43.4 | 55.4 / 41.8 | 680.0 / 650.4 | 60,114 | 0.8 |
| | | | Flink | 46.7 / 42.5 | - | 2,348.0 / 2,167.1 | 60,114 | 1.3 |
| 1 day (2026-10-19) | 1,042,693 | 37.0 | DuckDB | 4.7 / 2.9 | 2.3 / 1.4 | 369.3 / 453.3 | 1,146,460 | 12.8 |
| | | | Spark | 64.9 / 67.4 | 63.1 / 64.2 | 1,082.5 / 1,055.8 | 1,146,460 | 12.9 |
| | | | Flink | 92.5 / 81.3 | - | 2,553.3 / 2,578.9 | 1,146,460 | 19.6 |
| 1 day (2026-08-01) | 4,071,556 | 77.5 | DuckDB | 4.6 / 4.4 | 2.7 / 2.7 | 565.2 / 558.5 | 1,247,387 | 13.0 |
| | | | Spark | 71.1 / 71.4 | 67.1 / 69.3 | 1,188.3 / 1,004.1 | 1,247,387 | 13.1 |
| | | | Flink | 80.0 / 94.1 | - | 2,696.5 / 2,606.7 | 1,247,387 | 19.4 |

Spark's first 1-hour and 2026-10-19 runs (44.4 s and 62.1 s wall) recorded no memory (the cgroup v1
fallback was added after them); the table shows the two runs with memory. Flink's SQL client does
not report an in-engine time. Input MB is the prepared contract-projected Parquet (Snappy); the
silver behind it is 4.0 MB zstd for the hour, 88.8 MB and 267.0 MB for the days.

**Parity:** on every rung all three engines produced the same rows and the same sums of every
count measure (`keystone bench run` exits 1 otherwise, also against earlier recorded runs).

**Reading.** At these sizes DuckDB is 15 to 35x faster in wall time and uses a fraction of
the memory; the JVM engines' times are dominated by start-up, which is why the 1-hour and
1-day rungs cost them almost the same. Spark and Flink are kept for what they show (the
streaming path, lakehouse reads), not for speed here (ADR-024, ADR-029).

**Month rung: not run.** The disk would allow it (stream and delete), but parsing is the
bottleneck: one day of 2026-08 (24 hours, 502.5 MB gz, 4,071,556 events) took about 1.5 min to
download and about 12 min to parse in one process (from the files' times), so a month is 6 to 7
hours (estimate). The larger day rung (2026-08-01) stands in for it; parallel parsing comes
first (PROGRESS Next 3).

## 2. Storage: measured on the sample hours

Raw = gzip bytes from the manifest; silver = the parser's zstd Parquet (`events` +
`hour_types`); rollups = the DuckDB rollups of each *full* sample day as one zstd Parquet file
per rollup and day. 2026-08-01 was downloaded for this (raw deleted after parsing; its silver
is in this branch's `data/`, not the main checkout's).

#### Measured: sample dates (bytes on disk)

| date | era | hours | events | raw gz MB | silver MB | silver/raw | rollups MB (day) | rollup rows |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 2011-02-12 | E0 | 24 | 32,197 | 5.9 | 1.9 | 0.319 | 0.3 | 31,317 |
| 2012-03-10 | E0+E1 | 3 | 3,534 | 0.9 | 0.2 | 0.216 | - | - |
| 2012-03-11 | E1 | 21 | 81,477 | 20.0 | 3.5 | 0.177 | - | - |
| 2012-06-15 | E1 | 24 | 119,695 | 30.6 | 4.9 | 0.162 | 0.0 | 391 |
| 2013-06-15 | E1 | 1 | 6,954 | 2.2 | 0.3 | 0.146 | - | - |
| 2014-12-31 | E1 | 24 | 265,730 | 146.9 | 11.9 | 0.081 | 2.4 | 213,967 |
| 2015-01-01 | E2 | 24 | 218,939 | 73.8 | 14.4 | 0.195 | 2.5 | 224,946 |
| 2019-06-15 | E2 | 1 | 46,408 | 18.0 | 2.9 | 0.162 | - | - |
| 2025-05-23 | E2 | 1 | 213,245 | 111.0 | 14.7 | 0.133 | - | - |
| 2025-10-06 | E2 | 1 | 166,432 | 97.3 | 11.4 | 0.117 | - | - |
| 2025-10-08 | E2 | 1 | 163,656 | 97.4 | 11.5 | 0.118 | - | - |
| 2025-10-10 | E3 | 2 | 2,466 | 0.5 | 0.2 | 0.472 | - | - |
| 2026-08-01 | E3 | 24 | 4,071,556 | 502.5 | 267.0 | 0.531 | 13.0 | 1,247,387 |
| 2026-10-19 | E3 | 24 | 1,042,693 | 232.7 | 88.8 | 0.382 | 12.8 | 1,146,460 |

#### Measured candidates (full sample days; storage options, not pipeline tables)

| date | table | rows | MB |
|---|---|---:|---:|
| 2011-02-12 | contributors per repo and day (key-free) | 9,514 | 0.0 |
| 2011-02-12 | repo_hour summed to days | 12,549 | 0.1 |
| 2011-02-12 | silver without actor fields | 32,197 | 0.9 |
| 2012-06-15 | contributors per repo and day (key-free) | 0 | 0.0 |
| 2012-06-15 | repo_hour summed to days | 32 | 0.0 |
| 2012-06-15 | silver without actor fields | 119,695 | 2.7 |
| 2014-12-31 | contributors per repo and day (key-free) | 64,354 | 0.3 |
| 2014-12-31 | repo_hour summed to days | 90,747 | 0.5 |
| 2014-12-31 | silver without actor fields | 265,730 | 7.6 |
| 2015-01-01 | contributors per repo and day (key-free) | 66,304 | 0.3 |
| 2015-01-01 | repo_hour summed to days | 92,421 | 0.5 |
| 2015-01-01 | silver without actor fields | 218,939 | 10.0 |
| 2026-08-01 | contributors per repo and day (key-free) | 356,075 | 1.9 |
| 2026-08-01 | repo_hour summed to days | 534,057 | 3.2 |
| 2026-08-01 | silver without actor fields | 4,071,556 | 247.2 |
| 2026-10-19 | contributors per repo and day (key-free) | 359,021 | 1.8 |
| 2026-10-19 | repo_hour summed to days | 535,063 | 3.1 |
| 2026-10-19 | silver without actor fields | 1,042,693 | 69.1 |

Rollup split on the full days (bytes): 2026-08-01 `repo_actor_day` 8,429,022 and `repo_hour`
4,595,886; 2026-10-19 8,787,489 and 4,047,371; 2015-01-01 1,814,420 and 649,900. The salted
actor keys make `repo_actor_day` two thirds of the rollup bytes. zstd output sizes vary by a
few kB between runs (multi-threaded writer); rows do not. 2012-06-15 rolls up almost nothing
because E1 2012 events have no repository id (ADR-028; resolved by name in Phase 7).

Two E3 days: 2026-08-01 has 3.9x the events of 2026-10-19 but 3.0x the silver and only 1.02x
the rollup bytes. Rollups follow distinct repositories and actors, not volume.

## 3. Storage projections (all **estimates**)

Method: raw bytes per day = mean size of that month's Phase 1 HEAD-sweep hours
(`data/spike/head_sweep.csv`, 822 hours: day 15 at 0/6/12/18h monthly since 2011-03, weekly
since 2025-06) x 24, nearest month when a month has none. Silver and rollups = raw x the
measured ratio of the same era, pooled by bytes over that era's sample dates; low/high = the
smallest/largest ratio of one sample date. Not subtracted: archive gaps. Upper-side: E2 has one
full sample day (2015-01-01) for its rollup ratio, and rollups grow far slower than raw (above),
so the rollup projection for 2016-2025 is high.

#### Ratios per era (measured)

| era | silver/raw pooled (low-high) | rollups/raw pooled (low-high) |
|---|---|---|
| E0 | 0.319 (0.319-0.319) | 0.0532 (0.0532-0.0532) |
| E1 | 0.103 (0.081-0.177) | 0.0137 (0.0002-0.0165) |
| E2 | 0.138 (0.117-0.195) | 0.0333 (0.0333-0.0333) |
| E3 | 0.484 (0.382-0.531) | 0.0352 (0.0260-0.0552) |

#### Per year (estimate)

| year | eras | days | raw MB/day | silver MB/day | rollups MB/day | raw GB | silver GB (low-high) | rollups GB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 2011 | E0 | 323 | 11.7 | 3.7 | 0.6 | 3.8 | 1.2 (1.2-1.2) | 0.2 |
| 2012 | E0+E1 | 366 | 30.7 | 3.9 | 0.6 | 11.2 | 1.4 (1.2-2.2) | 0.2 |
| 2013 | E1 | 365 | 73.8 | 7.6 | 1.0 | 26.9 | 2.8 (2.2-4.8) | 0.4 |
| 2014 | E1 | 365 | 167.2 | 17.3 | 2.3 | 61.0 | 6.3 (4.9-10.8) | 0.8 |
| 2015 | E2 | 365 | 202.4 | 28.0 | 6.7 | 73.9 | 10.2 (8.7-14.4) | 2.5 |
| 2016 | E2 | 366 | 324.7 | 44.9 | 10.8 | 118.9 | 16.4 (13.9-23.2) | 4.0 |
| 2017 | E2 | 365 | 392.3 | 54.2 | 13.1 | 143.2 | 19.8 (16.8-27.9) | 4.8 |
| 2018 | E2 | 365 | 469.0 | 64.8 | 15.6 | 171.2 | 23.7 (20.1-33.4) | 5.7 |
| 2019 | E2 | 365 | 718.0 | 99.3 | 23.9 | 262.1 | 36.2 (30.7-51.1) | 8.7 |
| 2020 | E2 | 366 | 1,180.4 | 163.2 | 39.3 | 432.0 | 59.7 (50.6-84.3) | 14.4 |
| 2021 | E2 | 365 | 1,470.6 | 203.3 | 49.0 | 536.8 | 74.2 (62.9-104.8) | 17.9 |
| 2022 | E2 | 365 | 1,807.3 | 249.9 | 60.2 | 659.7 | 91.2 (77.3-128.8) | 22.0 |
| 2023 | E2 | 365 | 1,811.8 | 250.5 | 60.4 | 661.3 | 91.4 (77.5-129.1) | 22.0 |
| 2024 | E2 | 366 | 2,224.0 | 307.5 | 74.1 | 814.0 | 112.6 (95.4-158.9) | 27.1 |
| 2025 | E2+E3 | 365 | 1,760.4 | 308.0 | 59.0 | 642.5 | 112.4 (93.3-148.3) | 21.5 |
| 2026 | E3 | 293 | 703.4 | 340.5 | 24.8 | 206.1 | 99.8 (78.7-109.5) | 7.3 |

#### Windows (estimate)

| window | from | to | days | raw GB | silver GB (low-high) | rollups GB (low-high) |
|---|---|---|---:|---:|---:|---:|
| full archive | 2011-02-12 | 2026-10-21 | 5730 | 4,824.6 | 759.5 (635.3-1,032.7) | 159.5 (155.6-165.2) |
| 24 months before 2018-11 | 2016-11-01 | 2018-11-01 | 730 | 309.6 | 42.8 (36.3-60.4) | 10.3 (10.3-10.3) |
| 24 months before 2022-01 | 2020-01-01 | 2022-01-01 | 731 | 968.8 | 134.0 (113.5-189.1) | 32.3 (32.3-32.3) |
| 24 months before 2024-03 | 2022-03-01 | 2024-03-01 | 731 | 1,349.2 | 186.6 (158.1-263.3) | 45.0 (45.0-45.0) |
| last 24 months | 2024-10-21 | 2026-10-21 | 730 | 1,006.6 | 234.0 (190.5-288.7) | 34.1 (31.5-39.5) |

**Conclusions** (inputs to ADR-039):
- The earlier "~490 GB of silver (85 MB x days)" was not an upper bound: 2020-2024 hours are 3
  to 10x larger than 2026-10's. Silver for the full archive is about 760 GB (estimate; range
  635 GB to 1.03 TB).
- No 24-month incident window fits the 17.9 GB budget as silver (43 to 234 GB, estimate), and
  neither do the rollups for all history at today's grains (~160 GB, upper-side estimate).
- The owner therefore decided (2026-10-21): download only the incident windows and the live
  window, keep 30 days of silver and the rollups for what is downloaded, compute on GitHub
  Actions, store curated data as Release assets (section 4, ADR-039).
- Parse is bound by events, not bytes: 166,432 events of 2025-10-06 12h (97.3 MB gz) in 25.2 s
  (6,600 events/s, timed) and 4,071,556 events of 2026-08-01 (502.5 MB gz) in about 12 min
  (about 5,600 events/s, file times), one process. E3's trimmed payloads pack about 3x the
  events per gz byte of E2 (events/raw ratios in `storage.json`).

## 4. The decided design, sized (all **estimates**)

Owner decisions of 2026-10-21 (relayed by the Phase 4 kickoff session; ADR-039): a 30-day
rolling silver window plus rollups for all downloaded history; download only about 24 months
before each backtest incident plus the live score's trailing window; compute on GitHub Actions
(the repo is public since 2026-10-21: standard runners free and unlimited); curated data as
GitHub Release assets; never publish `repo_actor_day` or any actor key.

Windows come from `config/incidents.yml`: from the first day of the incident month minus 24
months to the end of the incident month (so any point-in-time cut in that month is covered),
and the live window from the first day of the month 24 months back to today (the live score
uses the backtest's lookback). Windows that overlap are downloaded once.

What is published (monthly zstd Parquet Release assets): `repo_hour` and a key-free
contributor table derived inside each job from `repo_actor_day`: per repository and day, the
number of human actors, their events and the largest actor's events (since ADR-081 its share, withheld below 3 actors; measured above: 1.8 to
1.9 MB per E3 day). `repo_actor_day` itself exists only on the runner's disk during a job.
The 30-day silver window is sized with and without the actor fields.

Job model: one job = consecutive days, each day = download its 24 hours at the measured
5.06 MB/s (2 connections) then parse at 5,600 events/s (one process), within a 5 h budget
(the 6 h limit less an hour). Jobs run one at a time so GH Archive never sees more than 2
connections. Parallel parsing on the runner's 4 CPUs is not measured and not assumed; it would
shorten the job-hours. Runner speeds are not measured either: these are this machine's rates.


#### ADR-039 design: downloaded windows (estimate)

| window | from | to | days | raw downloaded GB | events (M) | silver made GB | published repo_hour GB | published contributors GB | in-job repo_actor_day GB | jobs | job-hours | longest job h | job disk peak GB | largest monthly asset MB |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| event-stream | 2016-11-01 | 2018-12-01 | 760 | 328.4 | 668 | 45.4 | 2.9 | 1.4 | 8.1 | 11 | 51 | 5.0 | 0.5 | 252.3 |
| colors-faker | 2020-01-01 | 2022-02-01 | 762 | 1,028.0 | 2,092 | 142.1 | 9.1 | 4.4 | 25.2 | 33 | 160 | 5.0 | 0.8 | 775.9 |
| xz | 2022-03-01 | 2024-04-01 | 762 | 1,424.0 | 2,898 | 196.9 | 12.5 | 6.1 | 34.9 | 46 | 222 | 5.0 | 0.9 | 996.5 |
| live | 2024-10-01 | 2026-10-21 | 750 | 1,052.1 | 3,491 | 240.3 | 10.1 | 4.7 | 25.5 | 48 | 231 | 5.0 | 1.0 | 1,116.9 |
| **total** | | | | 3,832.5 | 9,148 | 624.8 | 34.6 | 16.7 | 93.7 | 138 | 664 | | | |

Rolling silver window (30 days ending today): 7.1 GB as parsed; 6.3 GB without actor fields (estimate).

**Reading.** The decided design downloads 3.8 TB of gz (estimate) but keeps little: about
51 GB published as Release assets (34.6 GB `repo_hour`, 16.7 GB key-free contributor
aggregates; upper-side for the E2 years), 50 monthly assets per window, the largest about
1.1 GB, under the 2 GiB limit. `repo_actor_day` (93.7 GB if it were kept) never leaves a
runner. Each job stays within 5 h and about 1 GB of the runner's 14 GB disk; 138 jobs and
664 job-hours run one at a time, about 28 days end to end at this machine's rates. (Superseded:
the backfill's live window is fixed at 2026-10-27; ingest keeps the radar current, ADR-074.)

## 5. Backfill rungs on GitHub Actions (ADR-050)

Rungs from the published chunk records (`chunk-*.json`), summed by `keystone backfill totals`
(a chunk redone after an incomplete run counts once). Cost $0 throughout (public repository,
standard runners). The sizes in section 4 are for the superseded 24-month design.

**Month rung** (one chunk; fetch time dominates, since GH Archive allows 2 connections):

| chunk | events | job s | fetch s | peak RSS self / children MB |
|---|---:|---:|---:|---:|
| event-stream 2018-11 | 41,599,234 | 854 | 815 | 9,263 / 886 |
| colors-faker 2021-02 (ADR-066 cap; 13.6 GB peak before it) | 75,290,616 | 2,103 | 2,020 | 9,115 / 1,527 |
| xz 2023-03-01..17 (half month, ADR-066 cap) | 68,504,380 | 2,531 | 2,427 | 8,477 / 1,896 |

**Month rung, largest half months** (after ADR-066):

| chunk | events | job s | fetch s | peak RSS self / children MB |
|---|---:|---:|---:|---:|
| xz 2024-03-01..17 (slowest and largest) | 87,760,116 | 3,009 | 2,909 | 8,511 / 2,231 |
| live 2026-05-01..17 (highest RSS in live) | 57,573,212 | 1,084 | 1,019 | 8,510 / 1,293 |

Peak RSS (self) per window, highest chunk:
- event-stream: 9,474 MB (whole 2018 months, run before the cap);
- colors-faker: 9,238 MB (whole 2021 months, 8 GB merge cap, ADR-066);
- xz: 8,511 MB and live: 8,510 MB (half months, cap).

The uncapped 2021-02 attempt reached 13.6 GB on the 16 GB runner.

**Year rung** (one window; final, every chunk complete, 2026-10-29):

| window | chunks | days | events in chunks | downloaded GB | repo_hour rows | contributors rows | sealed repo_actor_month rows | repo_hour GB | contributors GB | job hours | hours missing at source |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| event-stream (2017-11..2018-11) | 13 | 395 | 517,146,065 | 200.9 | 287,653,502 | 126,470,684 | 107,808,370 | 0.92 | 0.36 | 3.97 | 3 |
| colors-faker (2021-01..2022-01) | 13 | 396 | 1,121,147,780 | 604.9 | 607,736,949 | 207,186,513 | 168,508,949 | 1.87 | 0.59 | 10.22 | 225 |
| xz (2023-03..2024-03, half months) | 26 | 397 | 1,678,142,834 | 781.1 | 803,615,825 | 276,412,877 | 246,599,034 | 2.60 | 0.79 | 15.15 | 0 |
| live (2025-10..2026-10, half months) | 26 | 391 | 1,257,623,050 | 286.3 | 643,971,153 | 236,529,253 | 161,816,825 | 2.03 | 0.65 | 8.54 | 0 |
| **all** | **78** | **1,579** | **4,574,059,729** | **1,873.2** | **2,342,977,429** | **846,599,327** | **684,733,178** | **7.42** | **2.39** | **37.88** | **228** |

- Two parse quarantines in the whole backfill (1 colors-faker, 1 xz).
- Job hours count the final successful attempt of each chunk; retries and the cancelled
  attempts are not included.
- Gaps at GH Archive (404 when fetched): 2021-08-25 17h to 08-27 22h, 2021-10-22 05h to 10-29
  17h, and 3 hours in the event-stream window.
- About 1.87 TB was downloaded and streamed through standard runners at $0. What is kept:
  9.8 GB of key-free Parquet in the Release assets, plus the encrypted repo_actor_month.
- The live window downloaded 2.7x fewer bytes than xz for 75% of its events: E3 payloads (from
  2025-10-07) are trimmed (`docs/era-map.md`).

## 6. BigQuery sandbox rung (ADR-068), measured 2026-10-26

Run 40374905775 (`cloud.yml`, project `keystone-pipeline`, no billing account). Bytes are the
dry-run `totalBytesProcessed` per day table of the public `githubarchive.day` dataset (dry runs
are free). BigQuery bills at least 10 MB per table referenced.

| columns read | 2015-01-01 (E2) | 2026-10-19 (E3) |
|---|---:|---:|
| `type` | 2,767,153 | 12,442,919 |
| `type, repo.id` | 4,518,657 | 20,784,431 |
| `type, repo.id, created_at` | 6,270,161 | 29,125,975 |
| `type, repo.id, actor.id, created_at` | 8,021,665 | 37,467,519 |
| `payload` | 406,327,240 | 588,908,830 |
| all columns | 462,435,781 | 912,799,908 |

- `COUNT(*)` per day table: 0 bytes billed (metadata). 2026-10-19: 1,042,693 events, equal to
  our parse of the raw hours (section 2). 2015-01-01: 218,938, one fewer than our 218,939.
- In-place repo-day rollup of 2026-10-19 (`repo.id, type, created_at`):
  - billed 29,360,128 bytes (dry run 29,125,975), 13.0 slot-seconds, 23.7 s;
  - 535,063 rows;
  - export: 11 result pages to 2.46 MB zstd Parquet in 0.4 s, event total 1,042,693.
- Whole run: 29.4 MB billed of the 1 TB monthly free tier (month usage before the run: 0).
- The rollup columns cost 2-5% of the payload, so in-place rollups of the needed columns fit the
  free tier with room to spare. Only `payload` (merge flags, release actions) is expensive.

## 7. Databricks Free Edition rung (ADR-067), measured 2026-10-26

Run 40374905775 (`cloud.yml`, serverless SQL warehouse, catalog `workspace`), on the
committed fixtures (as in CI). The workspace cannot fetch GH Archive, so these are integration
timings, not scale numbers.

| step | measured |
|---|---|
| stage 8 sources (Files API to a volume, then one table each) | 547 rows, 31.4 s |
| `dbt build --target databricks` (dbt-databricks 1.12.8) | 123 nodes, 94 tests passed, 0 failed, 152.6 s |
| Lakeflow Declarative Pipeline update (`repo_week`, serverless) | COMPLETED, 178 rows, 77.8 s |
| Unity Catalog lineage (`system.access.table_lineage`, 7 days) | readable, 0 edges right after the run |
| quota | no limit hit; cost $0 |

Rerun 40376934911 with the audited lock (dbt-databricks 1.10.9): `dbt build` 123 nodes, 94 tests
passed, 108.0 s; Lakeflow COMPLETED, 178 rows, 61.9 s; Unity Catalog lineage (system table,
7 days) 104 edges into 28 tables, from both runs.
