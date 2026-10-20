"""OSV incremental snapshots over plain HTTPS (no Google account).

- `modified_id.csv` (about 151 MB) is sorted newest first, so an increment reads only its head
  with HTTP Range, chunk by chunk, until it reaches the stored cursor. Chunks after the first send
  `If-Range: <etag>`; if the file is regenerated mid-read the server answers 200 and the read
  restarts. New rows go to Parquet with `source_file`, `source_sha256` and `fetched_at`;
  unparseable lines go to quarantine with a reason.
- Per-ecosystem `<ecosystem>/all.zip` files are the baseline: conditional GET on the stored
  ETag, MD5-verified (the GCS ETag and `x-goog-hash`), recorded in the manifest.
"""

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote

import duckdb

from keystone.fetch.client import DiskGuard, FetchError, PoliteClient
from keystone.fetch.manifest import FileRecord, Manifest, Status
from keystone.sources import OsvConfig

SOURCE = "osv"
_TS = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?Z$")
MAX_RESTARTS = 3


def normalize_ts(raw: str) -> str | None:
    """`2026-10-20T06:15:07.31684656Z` -> nanosecond-padded form that sorts as text."""
    match = _TS.match(raw)
    if not match:
        return None
    return f"{match.group(1)}.{(match.group(2) or '').ljust(9, '0')}Z"


@dataclass(frozen=True)
class ModifiedRow:
    modified: str  # normalized, nanosecond precision, UTC
    ecosystem: str
    id: str


@dataclass
class IncrementResult:
    new_rows: int
    bytes_read: int
    chunks: int
    restarts: int
    quarantined: int
    cursor: str | None
    output: Path | None
    etag: str | None
    http_status: int | None = None


def parse_line(line: str) -> ModifiedRow | str:
    """A row, or the reason it cannot be parsed."""
    ts_raw, sep, key = line.partition(",")
    if not sep:
        return "no comma"
    ts = normalize_ts(ts_raw.strip())
    if ts is None:
        return f"bad timestamp {ts_raw[:40]!r}"
    ecosystem, slash, vuln_id = key.strip().rpartition("/")
    if not slash or not ecosystem or not vuln_id:
        return f"bad key {key[:80]!r}"
    return ModifiedRow(ts, ecosystem, vuln_id)


def _load_state(path: Path) -> dict[str, object]:
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    return {}


async def read_increment(
    client: PoliteClient,
    cfg: OsvConfig,
    state_path: Path,
) -> tuple[list[ModifiedRow], list[tuple[str, str]], IncrementResult, bytes]:
    """Rows newer than the cursor (plus unseen rows at the cursor timestamp)."""
    url = cfg.base_url.rstrip("/") + "/modified_id.csv"
    state = _load_state(state_path)
    raw_cursor = state.get("cursor")
    cursor = raw_cursor if isinstance(raw_cursor, str) else None
    seen_raw = state.get("cursor_keys")
    seen = set(seen_raw) if isinstance(seen_raw, list) else set()
    limit = None if cursor else cfg.first_run_max_bytes
    restarts = 0
    while True:
        rows: list[ModifiedRow] = []
        bad: list[tuple[str, str]] = []
        raw = bytearray()
        pending = b""
        etag: str | None = None
        first_status: int | None = None
        chunks = 0
        done = False
        restart = False
        offset = 0
        while not done:
            end = offset + cfg.head_chunk_bytes - 1
            status, body, headers = await client.get_range(url, offset, end, if_range=etag)
            chunks += 1
            if status == 200 and etag is not None:
                restart = True  # regenerated between chunks
                break
            if status not in (200, 206):
                raise FetchError(f"{url}: HTTP {status}")
            short = len(body) < cfg.head_chunk_bytes
            if status == 200 and not short:
                raise FetchError(f"{url}: server ignored the Range header")
            etag = etag or headers.get("etag")
            first_status = first_status or status
            raw.extend(body)
            total = _total_size(headers.get("content-range"))
            offset += len(body)
            eof = short or (total is not None and offset >= total)
            lines = (pending + body).split(b"\n")
            pending = b"" if eof else lines.pop()
            for line_bytes in lines:
                line = line_bytes.decode("utf-8", errors="replace").strip()
                if not line:
                    continue
                parsed = parse_line(line)
                if isinstance(parsed, str):
                    bad.append((line[:200], parsed))
                    continue
                if cursor is not None and (
                    parsed.modified < cursor
                    or (parsed.modified == cursor and f"{parsed.ecosystem}/{parsed.id}" in seen)
                ):
                    if parsed.modified < cursor:
                        done = True
                        break
                    continue
                rows.append(parsed)
            if eof or (limit is not None and offset >= limit):
                done = True
        if not restart:
            break
        restarts += 1
        if restarts > MAX_RESTARTS:
            raise FetchError(f"{url}: changed during {restarts} consecutive reads")
    new_cursor = max((r.modified for r in rows), default=cursor)
    result = IncrementResult(
        new_rows=len(rows),
        bytes_read=len(raw),
        chunks=chunks,
        restarts=restarts,
        quarantined=len(bad),
        cursor=new_cursor,
        output=None,
        etag=etag,
        http_status=first_status,
    )
    return rows, bad, result, bytes(raw)


def _slug(ts: str) -> str:
    """Normalized timestamp -> compact, file-name-safe digits."""
    return re.sub(r"[^0-9]", "", ts)


def _total_size(content_range: str | None) -> int | None:
    if not content_range or "/" not in content_range:
        return None
    total = content_range.rsplit("/", 1)[1]
    return int(total) if total.isdigit() else None


async def snapshot_increment(
    client: PoliteClient,
    manifest: Manifest,
    cfg: OsvConfig,
    out_dir: Path,
) -> IncrementResult:
    """Read the increment, write Parquet + quarantine, then advance the cursor."""
    state_path = out_dir / "modified_state.json"
    fetched_at = datetime.now(UTC)
    rows, bad, result, raw = await read_increment(client, cfg, state_path)
    url = cfg.base_url.rstrip("/") + "/modified_id.csv"
    sha256 = hashlib.sha256(raw).hexdigest()
    stamp = f"{fetched_at:%Y%m%dT%H%M%SZ}"
    if rows:
        # Named by the rows' modified range: a re-run after a crash rewrites the same file.
        low = min(r.modified for r in rows)
        name = f"modified-{_slug(low)}-{_slug(result.cursor or low)}.parquet"
        result.output = out_dir / "modified" / name
        _write_parquet(result.output, rows, url, sha256, fetched_at)
    if bad:
        qpath = out_dir / "quarantine" / f"modified-{stamp}.jsonl"
        qpath.parent.mkdir(parents=True, exist_ok=True)
        with qpath.open("w", encoding="utf-8") as fh:
            for line, reason in bad:
                record = {
                    "source_file": url,
                    "source_sha256": sha256,
                    "fetched_at": fetched_at.isoformat(),
                    "line": line,
                    "reason": reason,
                }
                fh.write(json.dumps(record) + "\n")
    if result.cursor is not None:
        at_cursor = [f"{r.ecosystem}/{r.id}" for r in rows if r.modified == result.cursor]
        state = _load_state(state_path)
        if state.get("cursor") == result.cursor:
            previous_keys = state.get("cursor_keys")
            if isinstance(previous_keys, list):
                at_cursor = sorted(set(at_cursor) | {str(k) for k in previous_keys})
        state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = state_path.with_name(state_path.name + ".tmp")
        tmp.write_text(
            json.dumps(
                {
                    "cursor": result.cursor,
                    "cursor_keys": at_cursor,
                    "etag": result.etag,
                    "updated_at": fetched_at.isoformat(),
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        tmp.replace(state_path)  # atomic: a crash keeps the old cursor, never bad JSON
    previous = manifest.get(url)
    status: Status = (
        "new" if previous is None else ("unchanged" if previous.etag == result.etag else "changed")
    )
    manifest.upsert(
        FileRecord(
            url=url,
            source=SOURCE,
            fetched_at=fetched_at,
            status=status,
            http_status=result.http_status,
            etag=result.etag,
            bytes=result.bytes_read,
            sha256=sha256,
        )
    )
    return result


def _write_parquet(
    path: Path, rows: list[ModifiedRow], url: str, sha256: str, fetched_at: datetime
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        con.execute(
            "CREATE TABLE t (modified_raw VARCHAR, ecosystem VARCHAR, id VARCHAR, "
            "source_file VARCHAR, source_sha256 VARCHAR, fetched_at TIMESTAMP)"
        )
        naive = fetched_at.astimezone(UTC).replace(tzinfo=None)
        con.executemany(
            "INSERT INTO t VALUES (?, ?, ?, ?, ?, ?)",
            [(r.modified, r.ecosystem, r.id, url, sha256, naive) for r in rows],
        )
        # Microsecond TIMESTAMP for joins; the nanosecond text stays in modified_raw.
        con.execute(
            "COPY (SELECT CAST(strptime(left(modified_raw, 26), '%Y-%m-%dT%H:%M:%S.%f') "
            "AS TIMESTAMP) AS modified, * FROM t) TO ? (FORMAT parquet)",
            [str(path)],
        )
    finally:
        con.close()


@dataclass
class ZipResult:
    ecosystem: str
    status: Status
    bytes: int | None
    path: Path | None


async def fetch_zips(
    client: PoliteClient,
    manifest: Manifest,
    cfg: OsvConfig,
    ecosystems: list[str],
    out_dir: Path,
    disk: DiskGuard,
) -> list[ZipResult]:
    """Conditional, MD5-verified download of `<ecosystem>/all.zip`; files are kept."""
    results: list[ZipResult] = []
    for ecosystem in ecosystems:
        url = zip_url(cfg, ecosystem)
        dest = out_dir / "zips" / safe_dir(ecosystem) / "all.zip"
        previous = manifest.get(url)
        conditional = previous is not None and previous.sha256 is not None and dest.exists()
        fetched_at = datetime.now(UTC)
        record = FileRecord(url=url, source=SOURCE, fetched_at=fetched_at, status="error")
        try:
            got = await client.download(
                url, dest, etag=previous.etag if conditional and previous else None, disk=disk
            )
        except FetchError as exc:
            record.error = str(exc)
            manifest.upsert(_carry(previous, record))
            results.append(ZipResult(ecosystem, "error", None, None))
            continue
        record.http_status = got.http_status
        if got.http_status == 304 and previous is not None:
            record.status = "unchanged"
            for name in ("etag", "last_modified", "bytes", "md5", "sha256"):
                setattr(record, name, getattr(previous, name))
        elif got.http_status in (404, 410):
            record.status = "missing"
        elif got.http_status == 200:
            record.etag, record.last_modified = got.etag, got.last_modified
            record.bytes, record.md5, record.sha256 = got.bytes, got.md5, got.sha256
            if previous is None or previous.sha256 is None:
                record.status = "new"
            elif previous.sha256 == got.sha256:
                record.status = "unchanged"
            else:
                record.status = "changed"
        else:
            record.error = f"HTTP {got.http_status}"
            _carry(previous, record)
        manifest.upsert(record)
        results.append(ZipResult(ecosystem, record.status, record.bytes, got.path or dest))
    return results


def zip_url(cfg: OsvConfig, ecosystem: str) -> str:
    return cfg.base_url.rstrip("/") + f"/{quote(ecosystem)}/all.zip"


def safe_dir(ecosystem: str) -> str:
    """`Debian:11` -> `Debian_11`: no NTFS stream separators or path characters."""
    return re.sub(r"[^A-Za-z0-9._-]", "_", ecosystem)


def _carry(previous: FileRecord | None, record: FileRecord) -> FileRecord:
    """An error must not erase what an earlier check verified."""
    if previous is not None:
        for name in ("etag", "last_modified", "bytes", "md5", "sha256"):
            setattr(record, name, getattr(previous, name))
    return record
