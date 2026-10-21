"""Arrow schemas derived from the pydantic row models, atomic Parquet writes, and the ingest sink.

`GhArchiveSink` is the `on_file` hook for `ingest_hours`: it parses a verified hour, fails
loudly on drift (the hour becomes `error` in the manifest and is retried), and otherwise writes
one Parquet file per hour and table, so a re-run overwrites the same files (idempotent).
"""

import json
import types
import typing
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Literal, Union

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel

from keystone.fetch.manifest import FileRecord
from keystone.parsers.drift import DriftError, Registry
from keystone.parsers.gharchive import FileContext, ParseResult, parse_file
from keystone.parsers.models import (
    EventRow,
    HourTypeRow,
    OsvAffectedRow,
    OsvQuarantineRow,
    OsvRepoRefRow,
    OsvVulnRow,
    QuarantineRow,
)
from keystone.parsers.osv import OsvResult
from keystone.parsers.privacy import ActorHasher
from keystone.sources import GhArchiveConfig
from keystone.sources.gharchive import hour_name

TIMESTAMP = pa.timestamp("us", tz="UTC")


def _arrow_type(annotation: Any) -> tuple[pa.DataType, bool]:
    """(Arrow type, nullable) for a field annotation."""
    origin = typing.get_origin(annotation)
    if origin is Annotated:
        return _arrow_type(typing.get_args(annotation)[0])
    if origin in (Union, types.UnionType):
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        inner, _ = _arrow_type(args[0])
        return inner, True
    if origin is Literal:
        return pa.string(), False
    mapping: dict[Any, pa.DataType] = {
        str: pa.string(),
        int: pa.int64(),
        float: pa.float64(),
        bool: pa.bool_(),
        datetime: TIMESTAMP,
    }
    if annotation not in mapping:
        raise TypeError(f"no Arrow type for {annotation!r}")
    return mapping[annotation], False


def arrow_schema(model: type[BaseModel]) -> pa.Schema:
    fields = []
    for name, info in model.model_fields.items():
        dtype, nullable = _arrow_type(info.annotation)
        fields.append(pa.field(name, dtype, nullable=nullable))
    return pa.schema(fields)


EVENT_SCHEMA = arrow_schema(EventRow)
QUARANTINE_SCHEMA = arrow_schema(QuarantineRow)
HOUR_TYPE_SCHEMA = arrow_schema(HourTypeRow)


def write_rows(rows: Sequence[BaseModel], schema: pa.Schema, dest: Path) -> int:
    """Write rows atomically (temp file, then rename). Returns the row count."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist([r.model_dump() for r in rows], schema=schema)
    tmp = dest.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp, compression="zstd")
    tmp.replace(dest)
    return int(table.num_rows)


class GhArchiveSink:
    """Parses verified GH Archive hours into `silver/` and `quarantine/` Parquet."""

    def __init__(
        self,
        cfg: GhArchiveConfig,
        silver_dir: Path,
        quarantine_dir: Path,
        drift_dir: Path,
        hasher: ActorHasher,
        registry: Registry | None,
    ) -> None:
        self.cfg = cfg
        self.silver_dir = silver_dir
        self.quarantine_dir = quarantine_dir
        self.drift_dir = drift_dir
        self.hasher = hasher
        self.registry = registry
        self.totals = {"files": 0, "events": 0, "quarantined": 0, "bytes_in": 0, "bytes_out": 0}
        self.stems: list[str] = []  # hours written by this sink (lineage inputs)

    def context(self, record: FileRecord) -> FileContext:
        if record.hour is None or record.sha256 is None:
            raise ValueError(f"{record.url}: manifest record lacks hour or sha256")
        return FileContext(
            source_file="gharchive/" + record.url.rsplit("/", 1)[-1],
            source_sha256=record.sha256,
            fetched_at=record.fetched_at,
            hour=record.hour,
            era=self.cfg.era_for(record.hour),
            period=self.cfg.period_for(record.hour),  # config is the truth (refined)
            trim_at=self.cfg.payload_trim_at,
        )

    def is_done(self, hour: datetime) -> bool:
        """The hour's events file exists (an earlier run parsed it)."""
        stem = hour_name(hour).removesuffix(".json.gz")
        return (self.silver_dir / "events" / f"date={hour:%Y-%m-%d}" / f"{stem}.parquet").exists()

    def __call__(self, path: Path, record: FileRecord) -> None:
        ctx = self.context(record)
        if ctx.era == "PRE":
            raise ValueError(f"{ctx.source_file}: before the first era")
        size = path.stat().st_size
        result = parse_file(path, ctx, self.hasher, self.registry)
        stem = ctx.source_file.rsplit("/", 1)[-1].removesuffix(".json.gz")
        if result.drift:
            self._report_drift(stem, ctx, result)
            lines = "; ".join(f.line() for f in result.drift[:5])
            more = f" (+{len(result.drift) - 5} more)" if len(result.drift) > 5 else ""
            raise DriftError(f"{ctx.source_file} ({ctx.era}): {lines}{more}")
        self.write(stem, ctx, result)
        self.totals["bytes_in"] += size

    def write(self, stem: str, ctx: FileContext, result: ParseResult) -> None:
        # The events file is written last: `is_done` checks it, so a crash part-way leaves the
        # hour not done and the next run parses it again.
        part = f"date={ctx.hour:%Y-%m-%d}"
        write_rows(
            result.hour_types,
            HOUR_TYPE_SCHEMA,
            self.silver_dir / "hour_types" / part / f"{stem}.parquet",
        )
        qpath = self.quarantine_dir / part / f"{stem}.parquet"
        if result.quarantine:
            write_rows(result.quarantine, QUARANTINE_SCHEMA, qpath)
        else:
            qpath.unlink(missing_ok=True)  # a re-parse that is now clean
        epath = self.silver_dir / "events" / part / f"{stem}.parquet"
        write_rows(result.events, EVENT_SCHEMA, epath)
        written = [epath, self.silver_dir / "hour_types" / part / f"{stem}.parquet"]
        if result.quarantine:
            written.append(qpath)
        self.totals["bytes_out"] += sum(p.stat().st_size for p in written)
        self.stems.append(stem)
        self.totals["files"] += 1
        self.totals["events"] += len(result.events)
        self.totals["quarantined"] += len(result.quarantine)

    def _report_drift(self, stem: str, ctx: FileContext, result: ParseResult) -> None:
        self.drift_dir.mkdir(parents=True, exist_ok=True)
        report = {
            "source_file": ctx.source_file,
            "source_sha256": ctx.source_sha256,
            "era": ctx.era,
            "findings": [f.__dict__ for f in result.drift],
        }
        (self.drift_dir / f"{stem}.json").write_text(json.dumps(report, indent=1), "utf-8")


OSV_SCHEMAS = {
    "vulns": arrow_schema(OsvVulnRow),
    "affected": arrow_schema(OsvAffectedRow),
    "repo_refs": arrow_schema(OsvRepoRefRow),
}
OSV_QUARANTINE_SCHEMA = arrow_schema(OsvQuarantineRow)


def write_osv(name: str, result: OsvResult, silver_dir: Path, quarantine_dir: Path) -> None:
    """One file per ecosystem zip and table (`<table>/<name>.parquet`), replaced atomically, so
    a re-parse of a newer zip overwrites the old rows. `vulns` is written last (done marker)."""
    write_rows(
        result.affected, OSV_SCHEMAS["affected"], silver_dir / "affected" / f"{name}.parquet"
    )
    write_rows(
        result.repo_refs, OSV_SCHEMAS["repo_refs"], silver_dir / "repo_refs" / f"{name}.parquet"
    )
    qpath = quarantine_dir / f"{name}.parquet"
    if result.quarantine:
        write_rows(result.quarantine, OSV_QUARANTINE_SCHEMA, qpath)
    else:
        qpath.unlink(missing_ok=True)
    write_rows(result.vulns, OSV_SCHEMAS["vulns"], silver_dir / "vulns" / f"{name}.parquet")
