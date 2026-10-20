# Phase 1 spike report

Measured 2026-10-20 from this machine (Windows 11, 8 CPUs, 16.8 GB RAM) with the polite client
(`config/policy.yml`). Scripts: `scripts/spike/`. Downloads: 33 sample hours, 1.31 GB gz in total,
each deleted after measurement. Anything not measured is labelled **estimate** or **documented**.
Oracles: `tests/fixtures/oracles/gharchive_hours.json` and `docs/data-audit.md`.

## 1. GH Archive host
- `data.gharchive.org` is a Google Cloud Storage bucket behind Cloudflare (`Server: cloudflare`,
  `x-goog-*` headers, `Cache-Control: public, max-age=14400`).
- **ETag = MD5 of the file** (33/33 samples) and `Last-Modified` is set, so conditional requests and
  free integrity checks work. `x-goog-hash` also carries crc32c and md5.
- `robots.txt`: 24 lines, all comments (Cloudflare content-signal preamble), no directives.
- Download rate, single stream, files over 5 MB: min 1.94, **median 6.96**, max 10.64 MB/s (n = 29).
- No rate-limit headers or throttling seen at no more than 2 req/s.
- Terms: GH Archive code is MIT and its site content is CC-BY-4.0 (repo README). The event data
  has no stated licence; it is GitHub API data, so GitHub's terms apply. Keystone publishes
  aggregates only and credits GH Archive.

## 2. Coverage and size
- **First hour: 2011-02-12 00h** (all 2011-02-11 hours return 404; gharchive.org agrees).
- HEAD sweep of 822 hours (day 15 at 0/6/12/18h monthly since 2011-03, weekly 12h since 2025-06): all 200.
- Mean gz size per hour on the 15th: 0.5 MB (2011), 8.4 (2015), 29.9 (2019), 61.5 (2021),
  92.6 (2024), 77.9 (2025), 28.5 (2026).
- **Estimate** (`scripts/spike/size_estimate.py`, method in its docstring): the whole archive,
  2011-02-12 to 2026-10-20, is about **4.86 TB gz**. JSON is 4.4 to 10.2
  times larger (median 5.4), so roughly 26 TB uncompressed. This replaces the unverified "tens of TB".

## 3. Parse throughput (this machine)
- orjson, line by line: median **315k events/s** on hours with more than 100k events.
- DuckDB `read_json` (count over gz NDJSON): median **107 MB of JSON/s** on hours with more than
  100k events (96 MB/s over all 33 hours), 1 to 8 s per hour.
- stdlib `json` is also timed in the per-hour JSON; orjson is about 2 to 3 times faster.

## 4. Completeness: the most important finding
GH Archive's capture is **not uniform after May 2025**. Event counts at the same hour (12h UTC):

| Hour | Events | Push | PR | IssueComment | Watch | Release |
| --- | --- | --- | --- | --- | --- | --- |
| 2025-05-15 Thu | 260,758 | 170,779 | 16,036 | 10,071 | 8,538 | 1,292 |
| 2025-05-22 Thu | 261,910 | 169,093 | 16,736 | 10,601 | 8,263 | 1,308 |
| 2025-05-29 Thu | 166,442 | 110,100 | 9,999 | 6,363 | 5,096 | 748 |
| 2025-10-06 | 166,432 | 102,659 | 13,871 | 7,481 | 4,327 | 746 |
| 2025-10-15 | 156,052 | 116,639 | 7,245 | 3,435 | 2,528 | 396 |
| 2026-03-15 | 164,927 | 131,472 | 5,771 | 2,337 | 2,689 | 536 |
| 2026-06-15 | 162,822 | 138,311 | 494 | 184 | 119 | 29 |
| 2026-08-23 | 169,788 | 165,515 | 77 | 39 | 27 | 7 |
| 2026-09-06 | 114,010 | 37,148 | 27,438 | 13,450 | 6,524 | 2,642 |
| 2026-10-18 | 19,155 | 9,801 | 2,699 | 1,569 | 739 | 273 |

Completeness periods (separate from the schema eras in `era-map.md`):
- **C1 full:** up to 2025-05-23 about 17h.
- **C2 reduced:** from 2025-05-23 (bracketed 12h to 18h), every major type drops 35 to 43% at once (gz size goes
  from 126 to 74 MB within the day). This matches upstream issue igrigorik/gharchive.org#310.
- **Gap:** 2025-10-08 about 20h to 2025-10-14. The files exist but hold only 0.1 to 0.4 MB each.
  *Update 2026-10-20 (Phase 2, ADR-016): a HEAD scan shows the near-empty files start at 17h (16h is full).*
- **C3 trimmed:** 2025-10-15 to about 2026-03. GitHub's payload trim applies, and non-push types
  fall further.
- **C4 push-only:** about 2026-04 to 2026-08. Non-push types fall to near zero (77 PRs in an hour).
- **C5 erratic:** 2026-09 onwards. Some hours are near normal, most are sparse. Upstream #320
  reports WatchEvent degradation since June 2025.

Consequence: activity signals computed on data after 2025-05-23 would mostly measure capture
artifacts. ADR-012 sets out how Keystone handles this. The per-type completeness series becomes a
published metric (the quality section of Pipeline Live).

## 5. OSV
- Plain HTTPS works without a Google account: `https://osv-vulnerabilities.storage.googleapis.com/`.
- Sizes (2026-10-20): `all.zip` 2.78 GB; `modified_id.csv` 151 MB, sorted newest first, so
  incremental reads need only its head via HTTP Range; `npm/all.zip` 219 MB; `PyPI/all.zip` 36 MB.
  `ecosystems.txt` lists 46 ecosystems. ETag and Last-Modified are set; there is no robots.txt (404).
- Licences follow each upstream source (mostly CC-BY 4.0, Ubuntu CC-BY-SA 4.0); the site must
  give attribution.

## 6. Package-to-repo mapping and criticality (ADR-010)
| Option | Access | Terms | Notes |
| --- | --- | --- | --- |
| ecosyste.ms API | open; robots `Allow: /`; 5,000 req/h anonymous (`x-ratelimit-*`) | data CC BY-SA 4.0 (a commercial licence exists; Keystone is non-commercial) | per package: `repository_url`, `dependent_packages_count`, `dependent_repos_count`, monthly downloads, a `critical` flag; `/api/v1/packages/critical` lists flagged packages |
| ecosyste.ms dump | `packages-2026-02-05.tar.gz` | CC BY-SA | **63.7 GB**, more than the disk budget |
| deps.dev API | `api.deps.dev/robots.txt`: `Disallow: /` | - | excluded by the robots rule; its BigQuery dataset needs a project |
| npm and PyPI registry `repository` fields | open | registry terms | a good cross-check for exact matches in Phase 7 |

Example: npm `lodash` maps to `github.com/lodash/lodash`, with 159,122 dependent packages,
1,936,033 dependent repos, 718.5M downloads last month, and critical = true.

## 7. Hugging Face mirror `open-index/open-github` (ADR-011: rejected)
- Unofficial; licence ODC-By; created 2026-03-27, **last modified 2026-04-09**; 53.4 GB. Its card
  says it spans 2015-04-14 to 2015-08-09, although it has files from 2011.
- Counts against GH Archive: on 2011-02-12, CommitCommentEvent 410 against 410, with identical ids.
  On 2015-06-15, ReleaseEvent **2,113 against 2,146 (33 missing, 1.5%)**. Probes for 2019, 2024
  and 2026 found no files.

## 8. Platform and tooling facts
| Item | Value (2026-10-20) | Source |
| --- | --- | --- |
| GitHub-hosted ubuntu-latest | private repos: 2 CPU, 8 GB RAM, 14 GB SSD; public: 4 CPU, 16 GB, 14 GB | GitHub docs |
| BigQuery sandbox | 1 TiB of queries a month, 10 GiB storage (a lifetime cap), tables expire after 60 days, no DML or streaming, no billing account needed | Google docs |
| Apache Flink | 2.3.0 (2026-06-25); flink-connector-kafka 5.0.0 supports Flink 2.1 and 2.2 | flink.apache.org |
| Redpanda | v26.2.2 | git tags |
| Iceberg | 1.12.0; Lakekeeper v0.14.1; Apache Polaris 1.8.0 | git tags |
| OpenLineage / Marquez / DataHub | 1.54.0 / 0.51.1 / v1.7.0 | git tags |
| Airflow / Spark / Trino | 3.3.2 / 4.2.0 / 484 | git tags |
| MinIO | last tag RELEASE.2025-10-15; RustFS 1.0.3 is a maintained alternative | git tags |

Not measured yet: the memory Flink, Redpanda and DataHub need on this machine (Phases 4 to 6, one
Compose profile at a time). DataHub's documented quickstart needs are large next to the ~4.4 GB of
RAM free at measurement, so DataHub runs alone in the `lineage` profile.

## 8b. Free stores, lineage support, limits, and this machine against Compose profiles
| Free store | Free allowance | Fit for Keystone |
| --- | --- | --- |
| GitHub Releases | assets under 2 GiB each (documented) | curated Parquet snapshots for the site build; $0, no new account |
| Hugging Face datasets (public) | "best effort" free public storage; files under 200 GB and folders under 10k files recommended | publishing curated rollups as an open dataset; needs an owner account and token |
| Cloudflare R2 | 10 GB-month, 1M class A and 10M class B ops a month, free egress | needs an owner account; charges above the free tier are possible, so it is a money hard stop and unused unless the owner opts in |

| OpenLineage integration | Mechanism | Column lineage | Status |
| --- | --- | --- | --- |
| Flink 2.x | native lineage interfaces (FLIP-314), no job code changes | not stated in the docs | documented; verify in Phase 6 |
| Flink 1.x | JobListener-based connector | not stated | not needed (Keystone uses Flink 2.x) |
| Spark | `openlineage-spark` listener | expected yes | to verify in Phase 6 |
| dbt | `openlineage-dbt` (`dbt-ol` wrapper) | expected yes, from the manifest | to verify in Phase 6 |
| Airflow | `apache-airflow-providers-openlineage` | for supported SQL operators | to verify in Phase 6 |

Only the Flink row was confirmed against the OpenLineage docs on 2026-10-20 (the integrations
overview page did not render). The other rows are expectations, marked "to verify".

GitHub Actions (docs): 6 h per job, 35 days per workflow run, 20 concurrent jobs on Free, 500 MB
of artifact storage on Free, a 10 GB cache per repo, and 2,000 free minutes a month for private
repos (standard runners are free for public repos). Runner sizes are in section 8.

| Compose profile | Planned services | Memory need | Fits this machine (16.8 GB, ~4.4 GB free at measurement)? |
| --- | --- | --- | --- |
| `core` | Postgres (Airflow metadata), Airflow | estimate 2 to 3 GB | yes |
| `streaming` | Redpanda, schema registry, Console, Flink JobManager and 1 TaskManager | estimate 3 to 5 GB | yes, alone |
| `catalog` | Lakekeeper, Postgres, an S3-compatible store | estimate 1 to 2 GB | yes |
| `lineage` | Marquez (API, web, Postgres); DataHub separately | DataHub tested at 2 CPUs, 8 GB RAM, 2 GB swap, 13 GB disk (docs) | Marquez yes; DataHub only alone, after freeing RAM and disk |
| `obs` | Prometheus, Grafana, an OpenTelemetry collector | estimate about 1 GB | yes |
| `bench` | Spark (local), Trino | estimate 4 to 6 GB | yes, alone |

Estimates are labelled as such and become measurements in Phases 4 to 6 (`docker stats`, peak
RSS). DataHub's 13 GB of disk is 66% of the current 19.7 GB disk budget, one more reason to free
disk (a hard stop).

## 9. Rungs skipped (no credentials)
- Databricks Free Edition (no token), BigQuery sandbox dry runs (no project) and OpenRouter (no
  key). Each runs when the owner provides the credential (CLAUDE.md hard stops).

## 10. Scale decision (ADR-009)
- Rule: the largest $0 window with an estimated unattended, resumable wall time of 7 days or less,
  and at least 12 months.
- Chosen: **the whole archive, 2011-02-12 00h to the present**, an estimated 4.86 TB gz. At 2
  streams of 6.96 MB/s (the median), the download takes about 4.0 days. DuckDB parsing overlaps it
  (about 2.8 days if run serially). This meets the rule. Which periods feed the score windows is a
  separate question, settled by ADR-012: by default, signals end at the last complete period
  (2025-05-23, bracketed 12h to 18h).
- Disk: stream and delete, with a working set capped well below the 19.7 GB budget. The brief says
  to ask when free disk is below 50 GB, and it is 28.2 GB, so the Phase 9 backfill run waits on
  the owner.
