# Era map

Which fields exist in which date range, per event type. Measured on 33 sample hours (2026-10-20,
`scripts/spike/gharchive_sample.py` + `era_fields.py`). Era boundaries were then measured in Phase 3
(`scripts/phase3/era_probe.py`, parsed counts; ADR-020). Shares are the fraction of events carrying
the path. `config/sources.yml` (`eras`, `payload_trim_at`) is the machine-readable form.

## Eras

| Era | Range (UTC) | Source API | Evidence |
| --- | --- | --- | --- |
| E0 `timeline-early` | files 2011-02-12 00h to 2012-03-10 08h (UTC names) | Timeline API, GH Archive early capture | sample 2011-02-12-0; probe |
| E1 `timeline` | files 2012-03-10 09h to 2014-12-31 23h (**Pacific-local names**; 09h-21h empty, first data 22h) | Timeline API (deprecated) | samples 2012-03-10/11, 2012-06-15, 2013-06-15, 2014-12-31; probe |
| E2 `events` | files 2015-01-01 00h to 2025-10-08 16h (UTC names) | Events API, full payloads | samples 2015..2025-10-08 16h |
| E3 `events-trimmed` | files 2025-10-08 17h onward; pushes trimmed from **2025-10-08 16:59:22Z** by event time | Events API after GitHub's payload trim | probes 2025-10-08 17h to 2025-10-14 23h; samples 2025-10-15 to 2026-10-19 |

- Measured in Phase 3 (2026-10-20): in 2025-10-08 16h every push up to 16:59:22Z has `payload.size`
  (95,492) and every later one does not (69), so the trim is an event-time instant, and that file
  is E2 with trimmed tail records. The near-empty hours (2025-10-08 17h to 2025-10-14 20h) are
  already trimmed, and 2025-10-14 21h is the first full hour again (30 MB).
- **Completeness periods** (C1 to C5, spike-report section 4) are tracked separately from these
  schema eras: a schema era says which fields exist, a completeness period says how much was captured.
- First available hour: **2011-02-12 00h** (2011-02-11 hours return 404; gharchive.org: "available
  starting 2/12/2011").

## Measured in Phase 3 (parsing, 2026-10-20)

- **E1 file names are Pacific-local hours.** File `2012-06-15-12` holds events from 19:01 to 20:01
  UTC; on 2012-03-11 (US DST start) there is no file `-2`. Events inside an E1 file span about
  xx:05 to xx+1:05 local, not the clock hour. Silver's `date=` partition and `hour_types.hour`
  follow the file label; event-time windows must use `created_at` (UTC).
- **E0 to E1 capture gap:** the last E0 event is 2012-03-10 08:00:00Z (file 08h holds 1 event,
  09h to 21h are empty files of 20 B), and E1 starts at 2012-03-11 06:36Z (file 22h PST): about
  22.6 hours without events.
- **E1 to E2 overlap:** the last eight E1 files (2014-12-31 16h-23h PST) and the first eight E2
  files cover the same UTC window, 2015-01-01 00:00 to 08:00, with nearly the same events (for
  example ReleaseEvent 198 and 198, WatchEvent 5,402 and 5,396, PushEvent 32,513 and 32,226;
  IssuesEvent 3,237 and 2,107). Rollups use E2 from 2015-01-01 00:00Z and drop E1 rows from that
  instant (ADR-020), or they double-count about 8 hours.
- **Early E1 formats:** top-level `created_at` is `2012/03/10 22:36:55 -0800` (not ISO 8601) from
  the first E1 file to 2012-06-01 09h (a mixed hour, 86% ISO). `public` is `true` or the integer
  `0` in 2012-03 (parsed as booleans; other integers are quarantined).
- **E0 placeholder repo:** events without a repository (Follow, Gist, some Push and Create) carry
  `repo.name = "/"`; the parser stores null.
- **E0 and E1 IssuesEvent carry `payload.number`** (and the issue id as `payload.issue`, an
  integer), so issue numbers exist in E1 for IssuesEvent; E1 IssueCommentEvent has only
  `issue_id` and `comment_id`. Late E0 (2012-03) CreateEvents already use `ref`/`ref_type`.
- **Privacy:** E1 `actor_attributes` also holds `blog`, `company` and `location`, not only email
  and name. The parser copies none of them (allow-list projection).

## Identity fields (the keys)

| Field | E0 | E1 2012 | E1 2013-14 | E2 | E3 |
| --- | --- | --- | --- | --- | --- |
| `actor.id` | 0.99 | absent | absent | 1.00 | 1.00 |
| actor login | `actor.login` | `actor` (string), `actor_attributes.login` | same | `actor.login` | `actor.login` |
| `repo.id` | 0.94 | absent | `repository.id` | 1.00 | 1.00 |
| repo name | `repo.name` | `repository.owner` + `repository.name`, `repository.url` | same | `repo.name` | `repo.name` |
| `created_at` offset | `Z` | **local Pacific (-07:00/-08:00)** | same | `Z` | `Z` |

Rules:
- `repo.id` is the key from E2 on and in E0/E1 2013-14 (`repository.id`). E1 2012 has no repo id:
  resolve by name to an id (name-history table, Phase 5/7) with a stored confidence, never assume.
- E1 has no actor id: the salted hash uses the login there, flagged `actor_key_kind = login`.
  Metrics that count distinct actors are versioned at the E1/E2 boundary.
- Normalize every `created_at` to UTC at parse time; keep the original string in bronze.
- **Privacy:** E1 events carry `actor_attributes.email` and `.name`. Parsers drop both at read
  time; they never reach Parquet, logs or fixtures (fixtures are scrubbed).

## Payload fields by event type

| Event / field | E0 | E1 | E2 | E3 |
| --- | --- | --- | --- | --- |
| Push `payload.size` | yes | yes | yes | **absent** |
| Push `payload.distinct_size`, `commits[]`, `commits[].author` | no | no | yes | **absent** |
| Push `payload.shas` | yes | yes | no | no |
| Push `payload.head`, `ref` | yes | yes | yes | yes |
| Push `payload.before` | no | no | yes | yes |
| Push `payload.repository_id` | no | no | late E2 only (present by 2024-06) | yes |
| PR `action`, `number` | yes | yes | yes | yes |
| PR `pull_request.merged`, `merged_at`, `merged_by`, `user`, `created_at`, `closed_at`, `comments`, `additions` | no | yes | yes | **absent** (only `id`, `number`, `url`, `base`, `head`) |
| Issues `action` | yes | yes | yes | yes |
| Issues `issue.number`, `created_at`, `closed_at`, `user`, `comments` | no | no | yes | yes |
| IssueComment `issue.number`, `comment.created_at` | - | no | yes | yes (volume reduced; 0 in the 2026-10-19 12h hour only) |
| Release `release.tag_name`, `published_at`, `prerelease` | - | yes (2014-12 only seen) | yes | yes |
| Member `payload.member.id` | no | yes | yes | yes |
| Create `ref_type`, `ref` | no | yes | yes | yes |
| Discussion (new type) | - | - | - | yes |

## Consequences for metrics (brief section 8)

- **Push commits and authors: unusable across eras** (absent E0/E1 and E3). Count push *events* per
  actor instead; that exists in every era.
- **PR merged flag: absent in E3 and E0.** "Merged PRs" is defined only for E1-E2. In E3 only PR
  `closed` actions remain, and whether a close was a merge, or who authored the PR, is not
  derivable; version the metric (`maint_v1` E1-E2 with merged PRs, `maint_v2` E3 with PR closes)
  and flag the break.
- **Issue first-response time: E2 and E3** (E1 IssueCommentEvents carry no issue number; Phase 3
  found they carry `issue_id`, which E1 IssuesEvents also carry, so an E1 version joined on issue id
  is possible and is a Phase 6 choice). In E3 it is computed only for periods that pass the
  completeness check (ADR-012), because IssueComment volume falls sharply.
- Release cadence and MemberEvent counts are available E1-E3.
- Volume: most E3 hours hold about 155k events, but the mix shifts from about 2026-04 onwards to
  nearly all pushes (completeness periods C3 to C5, spike-report section 4). Activity-trend metrics
  never compare raw counts across completeness periods.
