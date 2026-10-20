"""Phase 1 spike: archive size estimate from data/spike/head_sweep.csv (an estimate, not a count).

Method: per year, mean gz bytes of the swept hours on day 15 (0/6/12/18h), times the hours of that
year inside the window. Partial first and last years are prorated by hours.
Usage: uv run python scripts/spike/size_estimate.py [START_HOUR] [END_HOUR]  (YYYY-MM-DDTHH)
"""

import csv
import sys
from collections import defaultdict
from datetime import UTC, datetime

from keystone.paths import DATA_DIR


def main(start: str = "2011-02-12T00", end: str | None = None) -> None:
    t0 = datetime.strptime(start, "%Y-%m-%dT%H").replace(tzinfo=UTC)
    t1 = (
        datetime.strptime(end, "%Y-%m-%dT%H").replace(tzinfo=UTC)
        if end
        else datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    )
    sizes: dict[int, list[int]] = defaultdict(list)
    with (DATA_DIR / "spike" / "head_sweep.csv").open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["status"] == "200" and r["hour"][8:10] == "15":
                sizes[int(r["hour"][:4])].append(int(r["bytes"]))
    total = 0.0
    for year in range(t0.year, t1.year + 1):
        a = max(t0, datetime(year, 1, 1, tzinfo=UTC))
        b = min(t1, datetime(year + 1, 1, 1, tzinfo=UTC))
        hours = (b - a).total_seconds() / 3600
        if hours > 0 and sizes[year]:
            total += sum(sizes[year]) / len(sizes[year]) * hours
    print(f"estimate {t0:%Y-%m-%d %Hh} to {t1:%Y-%m-%d %Hh}: {total / 1e12:.2f} TB gz")
    for mbps in (6.96, 2 * 6.96):
        print(f"  download at {mbps:.2f} MB/s: {total / (mbps * 1e6) / 86400:.1f} days")
    print(
        f"  serial DuckDB parse at 107 MB JSON/s, 5.36x JSON/gz: "
        f"{total * 5.36 / 107e6 / 86400:.1f} days"
    )


if __name__ == "__main__":
    main(*sys.argv[1:3])
