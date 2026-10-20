"""Phase 1 spike: HEAD probes that bracket the coverage boundaries (evidence for data-audit.md).

Writes data/spike/boundary_heads.csv (url, status, bytes, last_modified). No downloads.
"""

import csv
import time

import httpx

from keystone.paths import DATA_DIR
from keystone.policy import load_policy

GHA = "https://data.gharchive.org"
HF = "https://huggingface.co/datasets/open-index/open-github/resolve/main/data"


def urls() -> list[str]:
    out = [f"{GHA}/2011-02-11-{h}.json.gz" for h in (0, 12, 23)] + [f"{GHA}/2011-02-12-0.json.gz"]
    for d in ("15", "20", "21", "22", "23", "24", "25", "27", "29"):
        out += [f"{GHA}/2025-05-{d}-{h}.json.gz" for h in (6, 12, 18)]
    for d in ("08", "09", "10", "11", "12", "13", "14"):
        out += [f"{GHA}/2025-10-{d}-{h}.json.gz" for h in (0, 4, 8, 12, 16, 20)]
    out += [f"{GHA}/2025-10-15-{h}.json.gz" for h in (0, 2, 4, 6, 8, 10)]
    out += [
        f"{HF}/{p}.parquet"
        for p in (
            "releases/2015/06/15",
            "releases/2019/06/15",
            "releases/2024/06/15",
            "releases/2026/03/30",
            "pushes/2026/04/08",
        )
    ]
    return out


def main() -> None:
    policy = load_policy()
    pause = 1 / policy.limits_for("data.gharchive.org").requests_per_second
    target = DATA_DIR / "spike" / "boundary_heads.csv"
    with (
        httpx.Client(
            headers={"User-Agent": policy.user_agent}, timeout=60, follow_redirects=True
        ) as c,
        target.open("w", newline="", encoding="utf-8") as f,
    ):
        w = csv.writer(f)
        w.writerow(["url", "status", "bytes", "last_modified"])
        for u in urls():
            r = c.head(u)
            w.writerow(
                [
                    u,
                    r.status_code,
                    r.headers.get("content-length", ""),
                    r.headers.get("last-modified", ""),
                ]
            )
            time.sleep(pause)
    print(f"wrote {target}")


if __name__ == "__main__":
    main()
