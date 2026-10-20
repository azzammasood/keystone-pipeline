"""Era-aware GH Archive parser: hourly `.json.gz` -> typed event rows, quarantine, type counts.

Eras (docs/era-map.md): E0 and E2/E3 records have `id`, `actor{}` and `repo{}`; E1 (Timeline
API) records have `url`, a string `actor`, `actor_attributes{}` and `repository{}`, and no ids
for events or actors. PushEvents after GitHub's payload trim (`payload_trim_at`, by event
time) lack `payload.size`. A file's era comes from its hour (config/sources.yml); every record
is checked against it and against the trim instant, and any mismatch is drift.

Privacy (ADR-013): the row is an allow-list projection. `actor_attributes` (email, name, blog,
company, location), logins, bodies and titles are never copied; people appear only as salted
keys, by account id, or by lower-cased login in E1 (`actor_key_kind = login`).
"""

import gzip
import hashlib
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import orjson
from pydantic import ValidationError

from keystone.parsers.drift import DriftFinding, Registry, ShapeObserver
from keystone.parsers.models import EventRow, HourTypeRow, KeyKind, QuarantineRow
from keystone.parsers.privacy import ActorHasher, is_bot

TIMELINE_ERAS = frozenset({"E1"})
# Early E1 created_at (2012-03-10 22h to 2012-06-01 09h, measured): `2012/03/10 22:36:55 -0800`.
TIMELINE_TS = "%Y/%m/%d %H:%M:%S %z"
# Records whose shape does not match the file's era. Any at all makes the file drift.
MAX_SHAPE_MISMATCH = 0


class RecordError(ValueError):
    """A record that cannot be parsed; the message is the quarantine reason (never a value)."""


@dataclass(frozen=True)
class FileContext:
    source_file: str
    source_sha256: str
    fetched_at: datetime
    hour: datetime
    era: str
    period: str
    trim_at: datetime  # pushes after it are trimmed, before it full (config payload_trim_at)


@dataclass
class ParseResult:
    events: list[EventRow] = field(default_factory=list)
    quarantine: list[QuarantineRow] = field(default_factory=list)
    hour_types: list[HourTypeRow] = field(default_factory=list)
    drift: list[DriftFinding] = field(default_factory=list)
    lines: int = 0
    observer: ShapeObserver = field(default_factory=ShapeObserver)


def parse_file(
    path: Path,
    ctx: FileContext,
    hasher: ActorHasher,
    registry: Registry | None,
) -> ParseResult:
    """Parse one hourly file. Drift findings are returned, not raised: the caller decides."""
    result = ParseResult()
    shape_mismatch = 0
    push_with_size = push_without_size = 0
    with gzip.open(path, "rb") as fh:
        for number, raw in enumerate(fh, start=1):
            result.lines = number
            line = raw.rstrip(b"\r\n")
            try:
                rec = _load(line)
                result.observer.observe(rec)
                if is_timeline(rec) != (ctx.era in TIMELINE_ERAS):
                    shape_mismatch += 1
                    raise RecordError("era_shape")
                row = _row(rec, number, ctx, hasher)
                if row.event_type == "PushEvent":
                    trimmed = row.push_size is None and "size" not in _dict(rec.get("payload"))
                    if trimmed and row.created_at < ctx.trim_at:
                        push_without_size += 1
                    elif not trimmed and row.created_at > ctx.trim_at:
                        push_with_size += 1
                result.events.append(row)
            except RecordError as exc:
                result.quarantine.append(_quarantine(ctx, number, line, str(exc)))
            except ValidationError as exc:
                err = exc.errors()[0]
                loc = ".".join(str(x) for x in err["loc"])
                reason = f"validation:{loc}:{err['type']}"
                result.quarantine.append(_quarantine(ctx, number, line, reason))

    if shape_mismatch > MAX_SHAPE_MISMATCH:
        detail = f"{shape_mismatch} records do not have the {ctx.era} shape"
        result.drift.append(DriftFinding("era_shape", "_envelope", "", detail))
    for n, has, side in (
        (push_without_size, "without", "before"),
        (push_with_size, "with", "after"),
    ):
        if n:
            detail = f"{n} PushEvents {has} payload.size {side} the trim ({ctx.era})"
            result.drift.append(DriftFinding("era_shape", "PushEvent", "payload.size", detail))
    if registry is not None:  # era-shape findings first: they explain the rest
        result.drift += result.observer.findings(registry, ctx.era)
    result.hour_types = _hour_types(result.events, ctx)
    return result


def _load(line: bytes) -> dict[str, Any]:
    if not line.strip():
        raise RecordError("blank_line")
    try:
        rec = orjson.loads(line)
    except orjson.JSONDecodeError as exc:
        raise RecordError("json_decode") from exc
    if not isinstance(rec, dict):
        raise RecordError("not_object")
    return rec


def is_timeline(rec: dict[str, Any]) -> bool:
    return "id" not in rec and ("repository" in rec or "actor_attributes" in rec or "url" in rec)


def _quarantine(ctx: FileContext, number: int, line: bytes, reason: str) -> QuarantineRow:
    return QuarantineRow(
        source_file=ctx.source_file,
        source_sha256=ctx.source_sha256,
        fetched_at=ctx.fetched_at,
        source_line=number,
        era=ctx.era,
        reason=reason,
        line_sha256=hashlib.sha256(line).hexdigest(),
        line_bytes=len(line),
    )


def _ts(value: Any, name: str) -> datetime | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RecordError(f"timestamp_type:{name}")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        try:  # early E1 (from 2012-03-10 22h): `2012/03/10 22:36:55 -0800`
            parsed = datetime.strptime(value, TIMELINE_TS)
        except ValueError as exc:
            raise RecordError(f"timestamp_format:{name}") from exc
    if parsed.tzinfo is None:
        raise RecordError(f"timestamp_naive:{name}")
    return parsed


def _public(value: Any) -> bool | None:
    """Early E1 mixes `true` with the integer `0` (2012-03, measured); 0 and 1 are booleans."""
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int) and value in (0, 1):
        return bool(value)
    raise RecordError("public_value")


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _row(rec: dict[str, Any], number: int, ctx: FileContext, hasher: ActorHasher) -> EventRow:
    if "created_at" not in rec:
        raise RecordError("missing:created_at")
    created = _ts(rec["created_at"], "created_at")
    assert created is not None
    offset = created.utcoffset()
    assert offset is not None
    etype = rec.get("type")
    if not isinstance(etype, str) or not etype:
        raise RecordError("missing:type")
    kind: KeyKind = "login" if ctx.era in TIMELINE_ERAS else "id"

    def person(user: Any) -> str | None:
        u = _dict(user)
        value = u.get("login") if kind == "login" else u.get("id")
        return hasher.key(kind, value) if value not in (None, "") else None

    if kind == "login":
        attrs = _dict(rec.get("actor_attributes"))
        actor = rec.get("actor")
        login = actor if isinstance(actor, str) and actor else attrs.get("login")
        actor_key = person({"login": login})
        repo = _dict(rec.get("repository"))
        owner, name = repo.get("owner"), repo.get("name")
        repo_name = f"{owner}/{name}" if owner and name else None
        event_id = None
    else:
        actor_obj = _dict(rec.get("actor"))
        login = actor_obj.get("login")
        actor_key = person(actor_obj)
        repo = _dict(rec.get("repo"))
        repo_name = repo.get("name") or None
        if repo_name == "/":  # E0 placeholder for events without a repository
            repo_name = None
        event_id = rec.get("id")

    p = _dict(rec.get("payload"))
    pr = _dict(p.get("pull_request"))
    issue_raw = p.get("issue")
    issue = _dict(issue_raw)
    comment = _dict(p.get("comment"))
    release = _dict(p.get("release"))
    is_pr_event = etype == "PullRequestEvent"
    is_issue_event = etype in ("IssuesEvent", "IssueCommentEvent")

    number_value = p.get("number")
    if number_value is None:
        number_value = pr.get("number") if pr else issue.get("number")
    ref_type, ref = p.get("ref_type"), p.get("ref")
    if ctx.era == "E0" and etype in ("CreateEvent", "DeleteEvent"):
        ref_type = ref_type if ref_type is not None else p.get("object")
        ref = ref if ref is not None else p.get("object_name")
    if isinstance(issue_raw, int):  # E0/E1: the issue id itself
        issue_id: Any = issue_raw
    else:
        issue_id = issue.get("id") if issue else p.get("issue_id")
    is_push = etype == "PushEvent"
    return EventRow(
        source_file=ctx.source_file,
        source_sha256=ctx.source_sha256,
        fetched_at=ctx.fetched_at,
        source_line=number,
        era=ctx.era,  # type: ignore[arg-type]
        period=ctx.period,
        event_id=event_id,
        event_type=etype,
        created_at=created,
        created_at_offset_min=int(offset.total_seconds() // 60),
        public=_public(rec.get("public")),
        actor_key=actor_key,
        actor_key_kind=kind if actor_key else None,
        actor_is_bot=is_bot(login if isinstance(login, str) else None),
        repo_id=repo.get("id"),
        repo_name=repo_name,
        action=p.get("action"),
        number=number_value,
        ref_type=ref_type if etype in ("CreateEvent", "DeleteEvent") else None,
        ref=ref if etype in ("CreateEvent", "DeleteEvent", "PushEvent") else None,
        push_head=p.get("head") if is_push else None,
        push_before=p.get("before") if is_push else None,
        push_size=p.get("size") if is_push else None,
        push_distinct_size=p.get("distinct_size") if is_push else None,
        payload_repository_id=p.get("repository_id"),
        pr_merged=pr.get("merged") if is_pr_event else None,
        pr_created_at=_ts(pr.get("created_at"), "pr.created_at") if is_pr_event else None,
        pr_merged_at=_ts(pr.get("merged_at"), "pr.merged_at") if is_pr_event else None,
        pr_closed_at=_ts(pr.get("closed_at"), "pr.closed_at") if is_pr_event else None,
        pr_author_key=person(pr.get("user")) if is_pr_event else None,
        issue_id=issue_id if is_issue_event else None,
        issue_created_at=_ts(issue.get("created_at"), "issue.created_at")
        if is_issue_event
        else None,
        issue_closed_at=_ts(issue.get("closed_at"), "issue.closed_at") if is_issue_event else None,
        issue_author_key=person(issue.get("user")) if is_issue_event else None,
        issue_is_pr=("pull_request" in issue) if is_issue_event and issue else None,
        comment_id=(comment.get("id") or p.get("comment_id"))
        if etype == "IssueCommentEvent"
        else None,
        comment_created_at=_ts(comment.get("created_at"), "comment.created_at")
        if etype == "IssueCommentEvent"
        else None,
        release_tag=release.get("tag_name"),
        release_published_at=_ts(release.get("published_at"), "release.published_at"),
        release_prerelease=release.get("prerelease"),
    )


def _hour_types(events: list[EventRow], ctx: FileContext) -> list[HourTypeRow]:
    counts: Counter[str] = Counter()
    lo: dict[str, datetime] = {}
    hi: dict[str, datetime] = {}
    for e in events:
        t = e.event_type
        counts[t] += 1
        if t not in lo or e.created_at < lo[t]:
            lo[t] = e.created_at
        if t not in hi or e.created_at > hi[t]:
            hi[t] = e.created_at
    return [
        HourTypeRow(
            source_file=ctx.source_file,
            source_sha256=ctx.source_sha256,
            fetched_at=ctx.fetched_at,
            hour=ctx.hour,
            era=ctx.era,  # type: ignore[arg-type]
            period=ctx.period,
            event_type=t,
            events=n,
            created_at_min=lo[t],
            created_at_max=hi[t],
        )
        for t, n in sorted(counts.items())
    ]
