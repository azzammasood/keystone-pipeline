"""Synthetic silver hours for rollup parity (SYNTHETIC TEST DATA: never published, never real).

Built straight at the silver level (typed rows, the same Parquet writer as the parser), so
each edge case is placed exactly. The arrival order is part of the scenario:

* out of order: every hour's events are shuffled;
* late hours: 2026-01-02 00h arrives after 03h (late for its hour windows), and 2026-01-01 22h
  arrives last (late for its hour and its day window);
* windows: events at exact hour and day starts, and at 23:59:59.9995 (milliseconds are floored
  by both engines);
* E1/E2 overlap (ADR-020): an E1 hour holding 2015-01-01 00h UTC (dropped) and one holding
  2014-12-31 23h (kept), then the E2 hour 2015-01-01 00h;
* bots, null repository ids, null actor keys, PR merged flags present (E1, E2) and absent (E3).
"""

import hashlib
import random
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

from keystone.parsers.models import EventRow, HourTypeRow
from keystone.parsers.parquet import arrow_schema, write_rows

FETCHED = datetime(2026, 1, 3, 6, 0, tzinfo=UTC)
TYPES = [
    ("PushEvent", None),
    ("PullRequestEvent", "opened"),
    ("PullRequestEvent", "closed"),
    ("IssuesEvent", "opened"),
    ("IssuesEvent", "closed"),
    ("IssueCommentEvent", "created"),
    ("ReleaseEvent", "published"),
    ("MemberEvent", "added"),
    ("WatchEvent", "started"),
    ("PullRequestReviewEvent", "created"),
]


def _key(i: int) -> str:
    return hashlib.sha256(f"synthetic-actor-{i}".encode()).hexdigest()[:32]


def _row(rng: random.Random, src: str, sha: str, line: int, era: str, when: datetime) -> EventRow:
    etype, action = rng.choice(TYPES)
    merged = None
    if etype == "PullRequestEvent" and action == "closed" and era in ("E1", "E2"):
        merged = rng.random() < 0.6
    actor = None if rng.random() < 0.05 else rng.randrange(25)
    kind = None if actor is None else ("login" if era == "E1" else "id")
    return EventRow(
        source_file=src,
        source_sha256=sha,
        fetched_at=FETCHED + timedelta(microseconds=line * 7),  # sub-millisecond on purpose
        source_line=line,
        era=era,  # type: ignore[arg-type]
        period="C1" if era != "E3" else "C3",
        event_id=None,
        event_type=etype,
        created_at=when,
        created_at_offset_min=-480 if era == "E1" else 0,
        public=True,
        actor_key=None if actor is None else _key(actor),
        actor_key_kind=kind,  # type: ignore[arg-type]
        actor_is_bot=actor is not None and actor >= 22,
        repo_id=None if rng.random() < 0.04 else rng.randrange(1, 16),
        repo_name=None,
        action=action,
        number=None,
        ref_type=None,
        ref=None,
        push_head=None,
        push_before=None,
        push_size=None,
        push_distinct_size=None,
        payload_repository_id=None,
        pr_merged=merged,
        pr_created_at=None,
        pr_merged_at=None,
        pr_closed_at=None,
        pr_author_key=None,
        issue_id=None,
        issue_created_at=None,
        issue_closed_at=None,
        issue_author_key=None,
        issue_is_pr=None,
        comment_id=None,
        comment_created_at=None,
        release_tag=None,
        release_published_at=None,
        release_prerelease=None,
    )


def _hour(name: str, era: str, start: datetime, n: int, seed: int) -> tuple[str, list[EventRow]]:
    rng = random.Random(seed)
    src = f"synthetic-{name}.json.gz"
    sha = hashlib.sha256(src.encode()).hexdigest()
    times = [start + timedelta(seconds=rng.randrange(3600)) for _ in range(n)]
    times += [start, start + timedelta(minutes=59, seconds=59, microseconds=999_500)]
    rng.shuffle(times)  # out of order inside the hour
    return name, [_row(rng, src, sha, i + 1, era, t) for i, t in enumerate(times)]


def hours() -> list[tuple[str, list[EventRow]]]:
    """All scenario hours, in arrival order."""
    d = datetime
    e3 = [d(2026, 1, 1, 23, tzinfo=UTC)] + [d(2026, 1, 2, h, tzinfo=UTC) for h in (1, 2, 3)]
    out = [
        _hour("2014-12-31-15", "E1", d(2014, 12, 31, 23, tzinfo=UTC), 60, 1),
        _hour("2014-12-31-16", "E1", d(2015, 1, 1, 0, tzinfo=UTC), 60, 2),  # overlap: dropped
        _hour("2015-01-01-0", "E2", d(2015, 1, 1, 0, tzinfo=UTC), 80, 3),
    ]
    out += [_hour(f"{t:%Y-%m-%d}-{t.hour}", "E3", t, 150, 10 + t.hour) for t in e3]
    out.append(_hour("2026-01-02-0", "E3", d(2026, 1, 2, 0, tzinfo=UTC), 150, 40))  # late
    out.append(_hour("2026-01-01-22", "E3", d(2026, 1, 1, 22, tzinfo=UTC), 150, 41))  # later
    return out


def write_hour(silver: Path, name: str, rows: list[EventRow]) -> Path:
    """Write one hour's events and hour_types like the parser's sink; returns the events file."""
    part = f"date={rows[0].created_at:%Y-%m-%d}"
    by_type: dict[str, list[EventRow]] = defaultdict(list)
    for r in rows:
        by_type[r.event_type].append(r)
    types = [
        HourTypeRow(
            source_file=rows[0].source_file,
            source_sha256=rows[0].source_sha256,
            fetched_at=rows[0].fetched_at,
            hour=min(r.created_at for r in rows).replace(minute=0, second=0, microsecond=0),
            era=rows[0].era,
            period=rows[0].period,
            event_type=t,
            events=len(rs),
            created_at_min=min(r.created_at for r in rs),
            created_at_max=max(r.created_at for r in rs),
        )
        for t, rs in sorted(by_type.items())
    ]
    events = silver / "events" / part / f"{name}.parquet"
    write_rows(rows, arrow_schema(EventRow), events)
    write_rows(types, arrow_schema(HourTypeRow), silver / "hour_types" / part / f"{name}.parquet")
    return events
