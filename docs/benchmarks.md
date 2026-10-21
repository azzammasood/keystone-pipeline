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
| 2012-03-10 | E1+E0 | 3 | 3,534 | 0.9 | 0.2 | 0.216 | - | - |
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


#### Measured candidate: hourly rollups summed to days (full sample days)

| date | table | rows | MB |
|---|---|---:|---:|
| 2011-02-12 | repo_hour summed to days | 12,549 | 0.1 |
| 2012-06-15 | repo_hour summed to days | 32 | 0.0 |
| 2014-12-31 | repo_hour summed to days | 90,747 | 0.5 |
| 2015-01-01 | repo_hour summed to days | 92,421 | 0.5 |
| 2026-08-01 | repo_hour summed to days | 534,057 | 3.2 |
| 2026-10-19 | repo_hour summed to days | 535,063 | 3.1 |

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
| E0 | 0.319 (0.319-0.319) | 0.0530 (0.0530-0.0530) |
| E1 | 0.103 (0.081-0.177) | 0.0137 (0.0002-0.0165) |
| E2 | 0.138 (0.117-0.195) | 0.0334 (0.0334-0.0334) |
| E3 | 0.484 (0.382-0.531) | 0.0352 (0.0259-0.0552) |


#### Per year (estimate)

| year | eras | days | raw MB/day | silver MB/day | rollups MB/day | raw GB | silver GB (low-high) | rollups GB |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 2011 | E0 | 323 | 11.7 | 3.7 | 0.6 | 3.8 | 1.2 (1.2-1.2) | 0.2 |
| 2012 | E0+E1 | 366 | 30.7 | 3.9 | 0.6 | 11.2 | 1.4 (1.2-2.2) | 0.2 |
| 2013 | E1 | 365 | 73.8 | 7.6 | 1.0 | 26.9 | 2.8 (2.2-4.8) | 0.4 |
| 2014 | E1 | 365 | 167.2 | 17.3 | 2.3 | 61.0 | 6.3 (4.9-10.8) | 0.8 |
| 2015 | E2 | 365 | 202.4 | 28.0 | 6.8 | 73.9 | 10.2 (8.7-14.4) | 2.5 |
| 2016 | E2 | 366 | 324.7 | 44.9 | 10.8 | 118.9 | 16.4 (13.9-23.2) | 4.0 |
| 2017 | E2 | 365 | 392.3 | 54.2 | 13.1 | 143.2 | 19.8 (16.8-27.9) | 4.8 |
| 2018 | E2 | 365 | 469.0 | 64.8 | 15.7 | 171.2 | 23.7 (20.1-33.4) | 5.7 |
| 2019 | E2 | 365 | 718.0 | 99.3 | 24.0 | 262.1 | 36.2 (30.7-51.1) | 8.8 |
| 2020 | E2 | 366 | 1,180.4 | 163.2 | 39.4 | 432.0 | 59.7 (50.6-84.3) | 14.4 |
| 2021 | E2 | 365 | 1,470.6 | 203.3 | 49.1 | 536.8 | 74.2 (62.9-104.8) | 17.9 |
| 2022 | E2 | 365 | 1,807.3 | 249.9 | 60.4 | 659.7 | 91.2 (77.3-128.8) | 22.0 |
| 2023 | E2 | 365 | 1,811.8 | 250.5 | 60.5 | 661.3 | 91.4 (77.5-129.1) | 22.1 |
| 2024 | E2 | 366 | 2,224.0 | 307.5 | 74.3 | 814.0 | 112.6 (95.4-158.9) | 27.2 |
| 2025 | E2+E3 | 365 | 1,760.4 | 308.0 | 59.1 | 642.5 | 112.4 (93.3-148.3) | 21.6 |
| 2026 | E3 | 293 | 703.4 | 340.5 | 24.7 | 206.1 | 99.8 (78.7-109.5) | 7.2 |


#### Windows (estimate)

| window | from | to | days | raw GB | silver GB (low-high) | rollups GB (low-high) |
|---|---|---|---:|---:|---:|---:|
| full archive | 2011-02-12 | 2026-10-21 | 5730 | 4,824.6 | 759.5 (635.3-1,032.7) | 159.8 (156.0-165.6) |
| 24 months before 2018-11 | 2016-11-01 | 2018-11-01 | 730 | 309.6 | 42.8 (36.3-60.4) | 10.3 (10.3-10.3) |
| 24 months before 2022-01 | 2020-01-01 | 2022-01-01 | 731 | 968.8 | 134.0 (113.5-189.1) | 32.4 (32.4-32.4) |
| 24 months before 2024-03 | 2022-03-01 | 2024-03-01 | 731 | 1,349.2 | 186.6 (158.1-263.3) | 45.1 (45.1-45.1) |
| last 24 months | 2024-10-21 | 2026-10-21 | 730 | 1,006.6 | 234.0 (190.5-288.7) | 34.1 (31.6-39.6) |

**Conclusions** (inputs to ADR-101):
- The earlier "~490 GB of silver (85 MB x days)" was not an upper bound: 2020-2024 hours are 3
  to 10x larger than 2026-10's. Silver for the full archive is about 760 GB (estimate; range
  635 GB to 1.03 TB).
- No 24-month incident window fits the 17.9 GB budget as silver (43 to 234 GB, estimate), and
  neither do the rollups for all history at today's grains (~160 GB, upper-side estimate).
- What fits is a stream-roll-delete backfill keeping a short silver window, actor-free history
  at day grain off the machine, and actor-level history compacted (ADR-101).
- Parse rate is the long pole: about 0.7 MB of gz per second in one process, so the raw
  archive (4.8 TB, estimate) is about 80 process-days (estimate) without parallel parsing.
