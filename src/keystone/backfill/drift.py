"""Backfill drift review (ADR-051 follow-up, ADR-069): sum the chunk records' `hours.drift`
per schema era and learn the reviewed shapes into `contracts/drift/gharchive.yml`.

A chunk record's findings carry no era; it comes from the chunk's dates (`config/sources.yml`
eras). A chunk spanning an era break is ambiguous: its findings are reported, never learned.
Unknown event types are reported only: the parser does not walk an unknown type's payload, so
its fields are not in the findings and its shape cannot be learned from them.
"""

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from keystone.parsers.drift import Registry
from keystone.sources import load_sources

LEARNABLE = {"unknown_field", "unknown_type"}


@dataclass
class DriftReview:
    findings: Counter[tuple[str, str]] = field(default_factory=Counter)  # (era, key) -> hours
    chunks: Counter[tuple[str, str]] = field(default_factory=Counter)  # (era, key) -> chunks
    ambiguous: list[str] = field(default_factory=list)  # chunk names spanning an era break


def chunk_era(record: dict[str, Any]) -> str | None:
    cfg = load_sources().gharchive
    start = datetime.fromisoformat(record["start"])
    end = datetime.fromisoformat(record["end"]) - timedelta(hours=1)
    first, last = cfg.era_for(start), cfg.era_for(end)
    return first if first == last else None


def review(records: list[dict[str, Any]]) -> DriftReview:
    out = DriftReview()
    for rec in records:
        drift = (rec.get("hours") or {}).get("drift") or {}
        if not drift:
            continue
        era = chunk_era(rec)
        if era is None:
            out.ambiguous.append(str(rec.get("chunk", rec.get("start"))))
            era = "ambiguous"
        for key, hours in drift.items():
            out.findings[(era, key)] += int(hours)
            out.chunks[(era, key)] += 1
    return out


def parse_key(key: str) -> tuple[str, str, str, str]:
    """`kind scope [path jtype ...]` (as `backfill chunk` writes it) -> its four parts."""
    parts = key.split(" ")
    kind, scope = parts[0], parts[1]
    if kind not in LEARNABLE:
        return kind, scope, "", ""
    return kind, scope, parts[2], parts[3]


def learn(rev: DriftReview, registry: Registry) -> list[str]:
    """Add every learnable finding of an unambiguous era; returns what was added."""
    added = []
    for era, key in sorted(rev.findings):
        kind, scope, path, jtype = parse_key(key)
        if era == "ambiguous" or kind not in LEARNABLE:
            continue
        paths = registry.get(era, {}).get(scope)
        if paths is None:
            continue  # its event type is unknown in this era: reported, not learned
        types = paths.setdefault(path, [])
        if jtype not in types:
            types.append(jtype)
            added.append(f"{era} {scope} {path} {jtype}")
    return added


def load_records(records: Path) -> list[dict[str, Any]]:
    import json

    from keystone.backfill.cli import record_files

    dirs = sorted({p.parent for p in records.rglob("chunk-*.json")})
    return [json.loads(p.read_text(encoding="utf-8")) for d in dirs for p in record_files(d)]
