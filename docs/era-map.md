# Era map

Which fields exist in which date range, per event type. Measured on 33 sample hours (2026-10-20,
`scripts/spike/gharchive_sample.py` + `era_fields.py`); boundaries between samples are bracketed,
not exact, until Phase 3 parses every day. Shares are the fraction of events carrying the path.

## Eras

| Era | Range (UTC) | Source API | Evidence |
| --- | --- | --- | --- |
| E0 `timeline-early` | 2011-02-12 00h to [2011-03 .. 2012-06) | Timeline API, GH Archive early capture | sample 2011-02-12-0 |
| E1 `timeline` | [2011-03 .. 2012-06) to 2014-12-31 | Timeline API (deprecated) | samples 2012-06-15, 2013-06-15, 2014-12-31; gharchive.org |
| E2 `events` | 2015-01-01 to the payload cut (see E3) | Events API, full payloads | samples 2015..2025-10-08 |
| E3 `events-trimmed` | 2025-10-15 00h onward (after a near-empty gap from 2025-10-08 20h) | Events API after GitHub's payload trim | samples 2025-10-15 to 2026-10-19 |

- GitHub applied the trim on 2025-10-07 (brief section 7), but GH Archive files keep full payloads
  up to 2025-10-08 16h (97 MB). Files from 2025-10-08 20h to 2025-10-14 are near-empty (0.1 to 0.4
  MB), and trimmed payloads start at 2025-10-15 00h. Phase 3 parsing fixes the exact hours.
- **Completeness periods** (C1 to C5, spike-report section 4) are tracked separately from these
  schema eras: a schema era says which fields exist, a completeness period says how much was captured.
- First available hour: **2011-02-12 00h** (2011-02-11 hours return 404; gharchive.org: "available
  starting 2/12/2011").

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
- **Issue first-response time: E2 and E3** (no issue numbers in E1). In E3 it is computed only for
  periods that pass the completeness check (ADR-012), because IssueComment volume falls sharply.
- Release cadence and MemberEvent counts are available E1-E3.
- Volume: most E3 hours hold about 155k events, but the mix shifts from about 2026-04 onwards to
  nearly all pushes (completeness periods C3 to C5, spike-report section 4). Activity-trend metrics
  never compare raw counts across completeness periods.
