"""GH Archive hourly files: polite, resumable, idempotent ingestion and gap detection.

Each hour `YYYY-MM-DD-H.json.gz` is checked with a conditional GET (If-None-Match on the stored
ETag, only once a verified SHA-256 is stored). A verified body is handed to `on_file` (Phase 3
parses it there) and then deleted unless `keep` is set, so the working set stays at most
`max_concurrent` files (stream and delete). The manifest is updated per hour, so an interrupted
run resumes where it stopped and a re-run reports `unchanged`.
"""

import asyncio
import statistics
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from keystone.fetch.client import DiskGuard, FetchError, PoliteClient
from keystone.fetch.manifest import FileRecord, Manifest, Status
from keystone.sources import GhArchiveConfig

SOURCE = "gharchive"
HOUR = timedelta(hours=1)

OnFile = Callable[[Path, FileRecord], None]


def hour_floor(value: datetime) -> datetime:
    value = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    return value.replace(minute=0, second=0, microsecond=0)


def hours(start: datetime, end: datetime) -> Iterator[datetime]:
    """UTC hours with `start <= hour < end`."""
    hour = hour_floor(start)
    while hour < end:
        yield hour
        hour += HOUR


def hour_name(hour: datetime) -> str:
    """GH Archive file name: the hour is not zero-padded (2015-01-01-15, 2015-01-01-0)."""
    return f"{hour:%Y-%m-%d}-{hour.hour}.json.gz"


def hour_url(cfg: GhArchiveConfig, hour: datetime) -> str:
    return cfg.base_url.rstrip("/") + "/" + hour_name(hour)


def is_near_empty(cfg: GhArchiveConfig, hour: datetime, size: int | None) -> bool:
    return size is not None and hour >= cfg.near_empty_from and size < cfg.near_empty_bytes


def is_late(cfg: GhArchiveConfig, hour: datetime, last_modified: datetime | None) -> bool:
    if last_modified is None:
        return False
    return last_modified > hour + HOUR + timedelta(hours=cfg.late_after_hours)


@dataclass
class RunSummary:
    counts: Counter[str]
    skipped_unpublished: int
    downloaded_bytes: int
    requests: int

    def line(self) -> str:
        parts = [f"{k}={v}" for k, v in sorted(self.counts.items())]
        return (
            " ".join(parts)
            + f" skipped_unpublished={self.skipped_unpublished}"
            + f" downloaded_bytes={self.downloaded_bytes} requests={self.requests}"
        )


async def ingest_hours(
    client: PoliteClient,
    manifest: Manifest,
    cfg: GhArchiveConfig,
    start: datetime,
    end: datetime,
    work_dir: Path,
    disk: DiskGuard,
    *,
    keep: bool = False,
    head_only: bool = False,
    on_file: OnFile | None = None,
    now: datetime | None = None,
) -> RunSummary:
    """Check every hour in `[start, end)`; returns status counts for this run."""
    now = now or datetime.now(UTC)
    start, end = max(hour_floor(start), cfg.first_hour), hour_floor(end)
    newest = hour_floor(now) - timedelta(hours=cfg.publish_lag_hours)
    todo = [h for h in hours(start, end) if h <= newest]
    skipped = sum(1 for h in hours(start, end) if h > newest)
    counts: Counter[str] = Counter()
    downloaded = 0
    limits = client.policy.limits_for(_host(cfg.base_url))
    queue: asyncio.Queue[datetime] = asyncio.Queue()
    for hour in todo:
        queue.put_nowait(hour)

    async def worker() -> None:
        nonlocal downloaded
        while not queue.empty():
            hour = queue.get_nowait()
            record = await check_hour(
                client,
                manifest,
                cfg,
                hour,
                work_dir,
                disk,
                keep=keep,
                head_only=head_only,
                on_file=on_file,
            )
            counts[record.status] += 1
            if record.status in ("new", "changed", "empty") and not head_only:
                downloaded += record.bytes or 0

    await asyncio.gather(*(worker() for _ in range(limits.max_concurrent)))
    return RunSummary(counts, skipped, downloaded, client.requests)


async def check_hour(
    client: PoliteClient,
    manifest: Manifest,
    cfg: GhArchiveConfig,
    hour: datetime,
    work_dir: Path,
    disk: DiskGuard,
    *,
    keep: bool = False,
    head_only: bool = False,
    on_file: OnFile | None = None,
) -> FileRecord:
    url = hour_url(cfg, hour)
    previous = manifest.get(url)
    verified = previous is not None and previous.sha256 is not None
    dest = work_dir / f"{hour:%Y}" / f"{hour:%m}" / hour_name(hour)
    have_copy = dest.exists()
    # Conditional only when the stored copy was verified and is not needed again locally.
    conditional = verified and (not keep or have_copy)
    fetched_at = datetime.now(UTC)
    base = FileRecord(
        url=url,
        source=SOURCE,
        fetched_at=fetched_at,
        status="error",
        hour=hour,
        period=cfg.period_for(hour),
    )
    try:
        result = await client.download(
            url,
            dest,
            etag=previous.etag if conditional and previous else None,
            disk=disk,
            method="HEAD" if head_only else "GET",
        )
    except FetchError as exc:
        base.error = str(exc)
        return _keep_previous_fields(manifest, base, previous)

    base.http_status = result.http_status
    if result.http_status == 304 and previous is not None:
        return manifest.upsert(_carry(previous, base, "unchanged"))
    if result.http_status in (404, 410):
        base.status = "missing"
        return manifest.upsert(base)
    if result.http_status != 200:
        base.error = f"HTTP {result.http_status}"
        return _keep_previous_fields(manifest, base, previous)

    base.etag = result.etag
    base.last_modified = result.last_modified
    base.bytes = result.bytes
    base.md5 = result.md5
    base.sha256 = result.sha256
    base.near_empty = is_near_empty(cfg, hour, result.bytes)
    base.late = is_late(cfg, hour, result.last_modified)
    if head_only and previous is not None and previous.etag == base.etag:
        # A HEAD cannot prove content; keep the verified hashes while the ETag still matches.
        base.md5, base.sha256 = previous.md5, previous.sha256
    base.status = _status(previous, base, verified_body=not head_only)
    try:
        if result.path is not None and on_file is not None and base.status != "unchanged":
            on_file(result.path, base)
    except Exception as exc:  # a consumer failure is this hour's error, not the run's
        base.status, base.error = "error", f"on_file: {exc!r}"
        base.md5 = base.sha256 = None  # not processed: the next run fetches it again
    finally:
        if result.path is not None and not keep:
            result.path.unlink(missing_ok=True)
            disk.release(result.bytes or 0)
    return manifest.upsert(base)


def _status(previous: FileRecord | None, current: FileRecord, *, verified_body: bool) -> Status:
    """A verified body is compared only with a verified copy (SHA-256); the first verified copy
    after a HEAD-only check is `new`/`empty`. HEAD-only checks compare ETags."""
    first: Status = "empty" if current.near_empty else "new"
    if previous is None:
        return first
    if verified_body:
        if previous.sha256 is None:
            return first
        same = previous.sha256 == current.sha256
    else:
        if previous.etag is None:
            return first
        same = previous.etag == current.etag
    if same:
        return "unchanged"
    return "changed"


def _carry(previous: FileRecord, base: FileRecord, status: Status) -> FileRecord:
    """A 304: keep the stored file facts, record the new check."""
    for name in ("etag", "last_modified", "bytes", "md5", "sha256", "near_empty", "late"):
        setattr(base, name, getattr(previous, name))
    base.status = status
    return base


def _keep_previous_fields(
    manifest: Manifest, base: FileRecord, previous: FileRecord | None
) -> FileRecord:
    """An error must not erase what an earlier check verified."""
    if previous is not None:
        for name in ("etag", "last_modified", "bytes", "md5", "sha256", "near_empty", "late"):
            setattr(base, name, getattr(previous, name))
    return manifest.upsert(base)


def _host(url: str) -> str:
    return url.split("://", 1)[-1].split("/", 1)[0].lower()


# --- gap and late-file detection -------------------------------------------------------------


@dataclass(frozen=True)
class Finding:
    hour: datetime
    period: str
    kind: str  # unchecked | missing | error | near_empty | low | late | changed
    detail: str


def find_gaps(
    manifest: Manifest,
    cfg: GhArchiveConfig,
    start: datetime,
    end: datetime,
    *,
    low_fraction: float = 0.1,
    now: datetime | None = None,
) -> list[Finding]:
    """Hours in `[start, end)` that are unchecked, missing, failed, near-empty, unusually small
    against the range median, late (Last-Modified after the grace), or rewritten (changed)."""
    now = now or datetime.now(UTC)
    start, end = max(hour_floor(start), cfg.first_hour), hour_floor(end)
    newest = hour_floor(now) - timedelta(hours=cfg.publish_lag_hours)
    rows = manifest.records(SOURCE, start, end)
    sizes = [r.bytes for r in rows.values() if r.bytes and r.status not in ("missing", "error")]
    median = statistics.median(sizes) if sizes else 0
    findings: list[Finding] = []
    for hour in hours(start, min(end, newest + HOUR)):
        period = cfg.period_for(hour)
        rec = rows.get(hour)

        def add(kind: str, detail: str, hour: datetime = hour, period: str = period) -> None:
            findings.append(Finding(hour, period, kind, detail))

        if rec is None:
            add("unchecked", "no manifest row")
            continue
        if rec.status == "missing":
            add("missing", f"HTTP {rec.http_status}")
        if rec.status == "error":
            add("error", rec.error or "unknown")
        if rec.near_empty:
            add("near_empty", f"{rec.bytes:,} B < {cfg.near_empty_bytes:,} B")
        elif rec.bytes is not None and median and rec.bytes < low_fraction * median:
            add("low", f"{rec.bytes:,} B < {low_fraction:.0%} of range median {median:,.0f} B")
        if rec.late and rec.last_modified is not None:
            add("late", f"Last-Modified {rec.last_modified:%Y-%m-%d %H:%M}Z")
        if rec.status == "changed":
            add("changed", "ETag/SHA-256 differs from the stored copy")
    return findings
