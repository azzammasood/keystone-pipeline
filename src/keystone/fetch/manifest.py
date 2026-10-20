"""File manifest in DuckDB: latest state per URL plus an append-only check log.

`files` holds one row per URL (idempotent upsert); `checks` logs every check so a re-run is
auditable. Timestamps are stored as naive UTC `TIMESTAMP`s.

Status of the latest check:
- new        first verified copy (200)
- unchanged  304, or 200 with the stored ETag/SHA-256
- changed    200 with a different ETag/SHA-256 than the stored copy (a late rewrite)
- missing    404/410 (retried on every run until it appears)
- empty      first copy is near-empty for its period (see sources.yml `near_empty_bytes`)
- error      verification or transport failure (retried on every run; `error` says why)
"""

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass, fields
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

import duckdb

Status = Literal["new", "unchanged", "changed", "missing", "empty", "error"]
STATUSES: tuple[Status, ...] = ("new", "unchanged", "changed", "missing", "empty", "error")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    url            VARCHAR PRIMARY KEY,
    source         VARCHAR NOT NULL,
    hour           TIMESTAMP,
    period         VARCHAR,
    http_status    INTEGER,
    etag           VARCHAR,
    last_modified  TIMESTAMP,
    bytes          BIGINT,
    md5            VARCHAR,
    sha256         VARCHAR,
    fetched_at     TIMESTAMP NOT NULL,
    status         VARCHAR NOT NULL,
    near_empty     BOOLEAN NOT NULL DEFAULT FALSE,
    late           BOOLEAN NOT NULL DEFAULT FALSE,
    error          VARCHAR,
    first_seen_at  TIMESTAMP NOT NULL,
    checks         INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS checks (
    url          VARCHAR NOT NULL,
    fetched_at   TIMESTAMP NOT NULL,
    http_status  INTEGER,
    etag         VARCHAR,
    bytes        BIGINT,
    status       VARCHAR NOT NULL,
    error        VARCHAR
);
"""


def utc_naive(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is not None:
        value = value.astimezone(UTC)
    return value.replace(tzinfo=None)


def as_utc(value: datetime | None) -> datetime | None:
    return None if value is None else value.replace(tzinfo=UTC)


@dataclass
class FileRecord:
    url: str
    source: str
    fetched_at: datetime
    status: Status
    hour: datetime | None = None
    period: str | None = None
    http_status: int | None = None
    etag: str | None = None
    last_modified: datetime | None = None
    bytes: int | None = None
    md5: str | None = None
    sha256: str | None = None
    near_empty: bool = False
    late: bool = False
    error: str | None = None
    first_seen_at: datetime | None = None
    checks: int = 1


_COLUMNS = [f.name for f in fields(FileRecord)]
_TIME_COLUMNS = {"fetched_at", "hour", "last_modified", "first_seen_at"}


class Manifest:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.db = duckdb.connect(str(path))
        self.db.execute(_SCHEMA)

    def close(self) -> None:
        self.db.close()

    def get(self, url: str) -> FileRecord | None:
        row = self.db.execute(
            f"SELECT {', '.join(_COLUMNS)} FROM files WHERE url = ?", [url]
        ).fetchone()
        return None if row is None else _record(row)

    def records(self, source: str, start: datetime, end: datetime) -> dict[datetime, FileRecord]:
        """Rows with `start <= hour < end`, keyed by UTC hour."""
        rows = self.db.execute(
            f"SELECT {', '.join(_COLUMNS)} FROM files WHERE source = ? AND hour >= ? AND hour < ?",
            [source, utc_naive(start), utc_naive(end)],
        ).fetchall()
        out: dict[datetime, FileRecord] = {}
        for row in rows:
            rec = _record(row)
            if rec.hour is not None:
                out[rec.hour] = rec
        return out

    def upsert(self, record: FileRecord) -> FileRecord:
        """Insert or replace the row for `record.url` and log the check. Idempotent per check."""
        previous = self.get(record.url)
        record.first_seen_at = previous.first_seen_at if previous else record.fetched_at
        record.checks = previous.checks + 1 if previous else 1
        values = asdict(record)
        params = [utc_naive(values[c]) if c in _TIME_COLUMNS else values[c] for c in _COLUMNS]
        with self.transaction():
            self.db.execute(
                f"INSERT OR REPLACE INTO files ({', '.join(_COLUMNS)}) "
                f"VALUES ({', '.join('?' for _ in _COLUMNS)})",
                params,
            )
            self.db.execute(
                "INSERT INTO checks VALUES (?, ?, ?, ?, ?, ?, ?)",
                [
                    record.url,
                    utc_naive(record.fetched_at),
                    record.http_status,
                    record.etag,
                    record.bytes,
                    record.status,
                    record.error,
                ],
            )
        return record

    def delete(self, url: str) -> None:
        self.db.execute("DELETE FROM files WHERE url = ?", [url])

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.db.execute("BEGIN TRANSACTION")
        try:
            yield
        except BaseException:
            self.db.execute("ROLLBACK")
            raise
        self.db.execute("COMMIT")


def _record(row: tuple[object, ...]) -> FileRecord:
    data = dict(zip(_COLUMNS, row, strict=True))
    for column in _TIME_COLUMNS:
        value = data[column]
        data[column] = as_utc(value) if isinstance(value, datetime) else None
    return FileRecord(**data)  # type: ignore[arg-type]
