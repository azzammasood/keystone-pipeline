"""Phase 1 spike: HEAD-only size sweep of GH Archive hours (no downloads).

Monthly: day 15, hours 0/6/12/18, from 2011-03 to the last full month. Weekly: hour 12 every
7 days from 2025-06-01. Writes data/spike/head_sweep.csv (hour, status, bytes, last_modified).
"""

import csv
import time
from datetime import UTC, date, datetime, timedelta

import httpx

from keystone.paths import DATA_DIR
from keystone.policy import load_policy


def hours() -> list[str]:
    out: list[str] = []
    today = datetime.now(UTC).date()
    y, m = 2011, 3
    while date(y, m, 15) < today:
        out += [f"{y}-{m:02d}-15-{h}" for h in (0, 6, 12, 18)]
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    d = date(2025, 6, 1)
    while d < today:
        out.append(f"{d.isoformat()}-12")
        d += timedelta(days=7)
    return sorted(set(out))


def main() -> None:
    policy = load_policy()
    pause = 1 / policy.limits_for("data.gharchive.org").requests_per_second
    target = DATA_DIR / "spike" / "head_sweep.csv"
    with (
        httpx.Client(headers={"User-Agent": policy.user_agent}, timeout=60) as c,
        target.open("w", newline="", encoding="utf-8") as f,
    ):
        w = csv.writer(f)
        w.writerow(["hour", "status", "bytes", "last_modified"])
        for h in hours():
            r = c.head(f"https://data.gharchive.org/{h}.json.gz")
            w.writerow(
                [
                    h,
                    r.status_code,
                    r.headers.get("content-length", ""),
                    r.headers.get("last-modified", ""),
                ]
            )
            time.sleep(pause)
    print(f"wrote {target}")


if __name__ == "__main__":
    main()
