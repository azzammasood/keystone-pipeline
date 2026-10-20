"""Find a schema-era boundary by binary search on record shape (Phase 3, docs/era-map.md).

Usage:
  uv run python scripts/phase3/era_probe.py timeline 2011-02-12T12 2012-06-15T12
  uv run python scripts/phase3/era_probe.py trimmed 2025-10-08T16 2025-10-15T00
  uv run python scripts/phase3/era_probe.py isots 2012-03-10T22 2012-06-15T12

`timeline`: share of records with the E1 (Timeline API) shape. `isots`: share of records whose
created_at is ISO 8601 (early E1 uses `2012/03/10 22:36:55 -0800`). `trimmed`: share of PushEvents
without payload.size (E3). Each probe fetches one hour through `keystone ingest gharchive`
(polite client, manifest), measures the share, and deletes the file. Assumes the share goes
from 0 at `lo` to 1 at `hi`; every probe is printed, so a mixed hour is visible.
"""

import gzip
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import orjson

from keystone.parsers.gharchive import is_timeline
from keystone.paths import DATA_DIR
from keystone.sources.gharchive import hour_name

RAW = DATA_DIR / "raw" / "gharchive"


def fetch(hour: datetime) -> Path | None:
    end = hour + timedelta(hours=1)
    subprocess.run(
        [
            "uv",
            "run",
            "keystone",
            "ingest",
            "gharchive",
            "--start",
            f"{hour:%Y-%m-%dT%H}",
            "--end",
            f"{end:%Y-%m-%dT%H}",
            "--keep",
            "--no-parse",
        ],
        check=False,
        capture_output=True,
    )
    path = RAW / f"{hour:%Y}" / f"{hour:%m}" / hour_name(hour)
    return path if path.exists() else None


def share(path: Path, test: str) -> tuple[float, int]:
    hits = total = 0
    with gzip.open(path, "rb") as fh:
        for line in fh:
            rec = orjson.loads(line)
            if test == "timeline":
                total += 1
                hits += is_timeline(rec)
            elif test == "isots":  # created_at in ISO 8601 rather than `2012/03/10 ... -0800`
                total += 1
                hits += "/" not in str(rec.get("created_at"))
            elif rec.get("type") == "PushEvent":
                total += 1
                hits += "size" not in (rec.get("payload") or {})
    return (hits / total if total else float("nan")), total


def probe(hour: datetime, test: str, keep: set[Path]) -> float:
    path = fetch(hour)
    if path is None:
        print(f"{hour:%Y-%m-%dT%H} missing")
        return float("nan")
    value, n = share(path, test)
    print(f"{hour:%Y-%m-%dT%H} share={value:.3f} n={n} bytes={path.stat().st_size}")
    if path not in keep:
        path.unlink()
    return value


def main() -> None:
    test, lo_s, hi_s = sys.argv[1:4]
    lo = datetime.fromisoformat(lo_s).replace(tzinfo=UTC)
    hi = datetime.fromisoformat(hi_s).replace(tzinfo=UTC)
    keep = set(RAW.rglob("*.json.gz"))  # never delete files that were kept before
    assert probe(lo, test, keep) == 0 and probe(hi, test, keep) == 1, "bracket does not hold"
    while hi - lo > timedelta(hours=1):
        mid = lo + timedelta(hours=((hi - lo) // timedelta(hours=1)) // 2)
        value = probe(mid, test, keep)
        if value != value:  # missing or no records: step one hour later
            lo = mid
            continue
        if value >= 1:
            hi = mid
        elif value <= 0:
            lo = mid
        else:
            print(f"mixed hour {mid:%Y-%m-%dT%H}: stop")
            return
    print(f"last {test}=0 hour {lo:%Y-%m-%dT%H}, first {test}=1 hour {hi:%Y-%m-%dT%H}")


if __name__ == "__main__":
    main()
