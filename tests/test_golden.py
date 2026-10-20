"""Golden tests: parsing real oracle hours reproduces the Phase 1 oracles exactly.

Each oracle hour is tested when its raw file is present under data/raw/gharchive (kept by
`keystone ingest gharchive --keep --no-parse`); CI fetches a few small hours once and caches
them. Oracle numbers are never edited here: a mismatch means the source or the parser changed.
"""

import hashlib
import json
import os
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import pytest
from conftest import DataDir, hour_of, record_for

from keystone.parsers.gharchive import parse_file
from keystone.paths import DATA_DIR, REPO_ROOT

ORACLES = json.loads(
    (REPO_ROOT / "tests" / "fixtures" / "oracles" / "gharchive_hours.json").read_text("utf-8")
)["hours"]
RAW = DATA_DIR / "raw" / "gharchive"


def _raw(hour: str) -> Path:
    return RAW / hour[:4] / hour[5:7] / f"{hour}.json.gz"


AVAILABLE = [h for h in ORACLES if _raw(h).exists()]


def test_golden_hours_present_or_skipped() -> None:
    required = [h for h in os.environ.get("KEYSTONE_GOLDEN_REQUIRED", "").split(",") if h]
    assert set(required) <= set(AVAILABLE), (
        f"missing golden hours: {set(required) - set(AVAILABLE)}"
    )
    if not AVAILABLE:
        pytest.skip("no raw oracle hours under data/raw/gharchive")
    assert set(AVAILABLE) <= set(ORACLES)


@pytest.mark.parametrize("hour", AVAILABLE)
def test_parse_reproduces_oracle(hour: str, data_dir: DataDir) -> None:
    oracle = ORACLES[hour]
    path = _raw(hour)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == oracle["sha256"], "file changed"
    ctx = data_dir.sink.context(record_for(path, hour))
    result = parse_file(path, ctx, data_dir.sink.hasher, data_dir.sink.registry)
    assert result.drift == [], [f.line() for f in result.drift[:5]]
    assert result.quarantine == []
    assert len(result.events) == result.lines == oracle["events"]
    assert dict(Counter(e.event_type for e in result.events)) == oracle["types"]
    lo = min(e.created_at for e in result.events)
    hi = max(e.created_at for e in result.events)
    assert lo == datetime.fromisoformat(oracle["created_at_min"]).astimezone(UTC)
    assert hi == datetime.fromisoformat(oracle["created_at_max"]).astimezone(UTC)
    assert ctx.hour == hour_of(hour)
