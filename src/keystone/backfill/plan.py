"""Backfill chunks: the incident and live windows cut into calendar months (ADR-050).

A chunk is a half-open run of whole UTC days inside one window and one month. Its outputs are
three Release assets in the window's release (`data-<window>`): `repo_hour-<name>.parquet`,
`contributors-<name>.parquet` and `chunk-<name>.json` (counts and the fetch manifest). A chunk
whose JSON exists is done; a chunk that lost hours publishes `chunk-<name>.incomplete.json`
instead and is retried only on request.
"""

import math
from dataclasses import dataclass
from datetime import date, timedelta

from keystone.bench.storage import _add_months, design_windows, load_incidents


@dataclass(frozen=True)
class Chunk:
    window: str
    start: date
    end: date  # exclusive

    @property
    def name(self) -> str:
        return f"{self.start:%Y%m%d}-{self.end:%Y%m%d}"

    @property
    def tag(self) -> str:
        return f"data-{self.window}"

    @property
    def done_asset(self) -> str:
        return f"chunk-{self.name}.json"

    @property
    def actor_asset(self) -> str:
        """The sealed internal table (ADR-045); a chunk without it is redone."""
        return f"repo_actor_month-{self.name}.parquet.enc"

    @property
    def incomplete_asset(self) -> str:
        return f"chunk-{self.name}.incomplete.json"

    def matrix(self) -> dict[str, str]:
        return {
            "window": self.window,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "name": self.name,
            "tag": self.tag,
        }


def windows(end: date) -> dict[str, tuple[date, date]]:
    """`config/incidents.yml` windows; `end` (exclusive) closes the live window."""
    return design_windows(load_incidents(), end)


def _split(start: date, end: date, chunk_days: int) -> list[tuple[date, date]]:
    days = (end - start).days
    if chunk_days <= 0 or days <= chunk_days:
        return [(start, end)]
    parts = math.ceil(days / chunk_days)
    bounds = [start + timedelta(days=round(i * days / parts)) for i in range(parts + 1)]
    return list(zip(bounds[:-1], bounds[1:], strict=True))


def chunks(wins: dict[str, tuple[date, date]], chunk_days: int = 0) -> list[Chunk]:
    """Each window cut at month boundaries, each month into parts of at most `chunk_days`
    (0: whole months). Windows are disjoint in the current config; an overlap would be
    planned twice, so it is refused."""
    out: list[Chunk] = []
    spans = sorted(wins.values())
    for (_, a_end), (b_start, _) in zip(spans, spans[1:], strict=False):
        if b_start < a_end:
            raise ValueError("incident windows overlap; plan them as one window")
    for name, (start, end) in wins.items():
        month = date(start.year, start.month, 1)
        while month < end:
            nxt = _add_months(month, 1)
            lo, hi = max(start, month), min(end, nxt)
            out.extend(Chunk(name, a, b) for a, b in _split(lo, hi, chunk_days))
            month = nxt
    return sorted(out, key=lambda c: c.start)


def pending(
    plan: list[Chunk], published: set[str], *, redo_incomplete: bool = False
) -> list[Chunk]:
    """Chunks with no published JSON (`published` = asset names in the window releases), or
    done before ADR-045 without the sealed `repo_actor_month` (redone to add it)."""
    out = []
    for c in plan:
        if c.done_asset in published and c.actor_asset in published:
            continue
        if c.incomplete_asset in published and not redo_incomplete:
            continue
        out.append(c)
    return out
