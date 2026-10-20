"""Phase 1 spike: compare the Hugging Face open-index/open-github mirror with GH Archive.

For one UTC day and one event type, count events in the 24 GH Archive hourly files and rows in
the mirror's daily Parquet file. Raw files are deleted afterwards.

Usage: uv run python scripts/spike/mirror_compare.py 2011-02-12 CommitCommentEvent commit_comments
"""

import gzip
import json
import sys
import time

import duckdb
import httpx
import orjson

from keystone.paths import DATA_DIR
from keystone.policy import load_policy

RAW = DATA_DIR / "spike" / "mirror"
HF = "https://huggingface.co/datasets/open-index/open-github/resolve/main/data"


def main(day: str, event_type: str, table: str) -> None:
    RAW.mkdir(parents=True, exist_ok=True)
    policy = load_policy()
    pause = 1 / policy.limits_for("data.gharchive.org").requests_per_second
    gh_count, gh_ids, hours_ok = 0, set(), 0
    with httpx.Client(headers={"User-Agent": policy.user_agent}, timeout=300) as c:
        for h in range(24):
            r = c.get(f"https://data.gharchive.org/{day}-{h}.json.gz")
            time.sleep(pause)
            if r.status_code != 200:
                continue
            hours_ok += 1
            for line in gzip.decompress(r.content).splitlines():
                if line.strip():
                    e = orjson.loads(line)
                    if e.get("type") == event_type:
                        gh_count += 1
                        if e.get("id"):
                            gh_ids.add(str(e["id"]))
        y, m, d = day.split("-")
        dest = RAW / f"{table}-{day}.parquet"
        r = c.get(f"{HF}/{table}/{y}/{m}/{d}.parquet", follow_redirects=True)
    out: dict[str, object] = {
        "day": day,
        "event_type": event_type,
        "gharchive_hours": hours_ok,
        "gharchive_events": gh_count,
        "gharchive_ids": len(gh_ids),
        "mirror_http": r.status_code,
    }
    if r.status_code == 200:
        dest.write_bytes(r.content)
        con = duckdb.connect()
        p = dest.as_posix()
        out["mirror_rows"] = con.sql(f"select count(*) from '{p}'").fetchone()[0]  # type: ignore[index]
        cols = [row[0] for row in con.sql(f"describe select * from '{p}'").fetchall()]
        out["mirror_columns"] = cols
        id_col = next((x for x in ("event_id", "id") if x in cols), None)
        if id_col:
            ids = {str(v[0]) for v in con.sql(f"select {id_col} from '{p}'").fetchall()}
            out["mirror_ids"] = len(ids)
            out["ids_only_in_gharchive"] = len(gh_ids - ids)
            out["ids_only_in_mirror"] = len(ids - gh_ids)
        dest.unlink()
    target = DATA_DIR / "spike" / f"mirror-{table}-{day}.json"
    target.write_text(json.dumps(out, indent=1), encoding="utf-8")
    print({k: v for k, v in out.items() if k != "mirror_columns"})


if __name__ == "__main__":
    main(*sys.argv[1:4])
