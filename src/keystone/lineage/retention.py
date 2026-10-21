"""Retention of the OpenLineage event log (brief 9b): compressed JSONL with a size cap.

Day files before today are gzipped (`YYYY-MM-DD.jsonl.gz`). While the compressed files exceed
the cap, the oldest day is summarised into `lineage/summary.jsonl` (events, runs and datasets
per job, first and last event time) and removed. The graph is rebuilt from what remains, so
the summary keeps the history countable after the detail is gone.
"""

import gzip
import shutil
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import orjson

from keystone.lineage.emit import events_dir


@dataclass(frozen=True)
class Compaction:
    compressed: int
    summarised: int
    bytes_kept: int


def _summary(path: Path) -> dict[str, Any]:
    jobs: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"events": 0, "runs": set(), "datasets": set()}
    )
    times = []
    with gzip.open(path, "rb") as fh:
        for line in fh:
            if not line.strip():
                continue
            ev = orjson.loads(line)
            job = jobs[f"{ev['job']['namespace']}/{ev['job']['name']}"]
            job["events"] += 1
            job["runs"].add(ev["run"]["runId"])
            for d in (ev.get("inputs") or []) + (ev.get("outputs") or []):
                job["datasets"].add(f"{d['namespace']}/{d['name']}")
            times.append(ev["eventTime"])
    return {
        "day": path.name.split(".")[0],
        "first_event": min(times, default=None),
        "last_event": max(times, default=None),
        "jobs": {
            k: {"events": v["events"], "runs": len(v["runs"]), "datasets": len(v["datasets"])}
            for k, v in sorted(jobs.items())
        },
    }


def compact(root: Path, max_bytes: int, today: date) -> Compaction:
    d = events_dir(root)
    compressed = summarised = 0
    for path in sorted(d.glob("*.jsonl")):
        if date.fromisoformat(path.stem) >= today:
            continue
        with path.open("rb") as src, gzip.open(path.with_suffix(".jsonl.gz"), "wb") as dst:
            shutil.copyfileobj(src, dst)
        path.unlink()
        compressed += 1
    archives = sorted(d.glob("*.jsonl.gz"))
    total = sum(p.stat().st_size for p in archives)
    while archives and total > max_bytes:
        oldest = archives.pop(0)
        with (root / "lineage" / "summary.jsonl").open("ab") as fh:
            fh.write(orjson.dumps(_summary(oldest)) + b"\n")
        total -= oldest.stat().st_size
        oldest.unlink()
        summarised += 1
    return Compaction(compressed, summarised, total)
