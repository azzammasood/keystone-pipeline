"""Phase 1 spike: download sample GH Archive hours, measure them, delete the raw file.

Per hour: bytes, sha256, md5 vs ETag, download time, event count, event-type mix, parse
throughput (stdlib json, orjson, DuckDB), and the field paths present per event type.
Writes one JSON summary per hour to data/spike/hours/. Raw files are deleted afterwards.

Usage: uv run python scripts/spike/gharchive_sample.py 2011-02-12-0 2015-01-01-12 ...
"""

import gzip
import hashlib
import json
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

import duckdb
import httpx
import orjson

from keystone.paths import DATA_DIR
from keystone.policy import load_policy

OUT = DATA_DIR / "spike" / "hours"
RAW = DATA_DIR / "spike" / "raw"
MAX_DEPTH = 3


def paths(obj: object, prefix: str = "", depth: int = 0) -> set[str]:
    """Key paths of a JSON value; lists are marked [] and their first dict is inspected."""
    out: set[str] = set()
    if depth >= MAX_DEPTH:
        return out
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{prefix}.{k}" if prefix else k
            out.add(p)
            out |= paths(v, p, depth + 1)
    elif isinstance(obj, list) and obj and isinstance(obj[0], dict):
        out |= paths(obj[0], prefix + "[]", depth)
    return out


def download(client: httpx.Client, hour: str, dest: Path) -> dict[str, object]:
    url = f"https://data.gharchive.org/{hour}.json.gz"
    sha, md5, size = hashlib.sha256(), hashlib.md5(usedforsecurity=False), 0
    t0 = time.perf_counter()
    with client.stream("GET", url) as r:
        r.raise_for_status()
        etag = r.headers.get("etag", "").strip('"')
        with dest.open("wb") as f:
            for chunk in r.iter_bytes(1 << 20):
                f.write(chunk)
                sha.update(chunk)
                md5.update(chunk)
                size += len(chunk)
        headers = {k: r.headers.get(k) for k in ("last-modified", "server", "cf-cache-status")}
    secs = time.perf_counter() - t0
    return {
        "url": url,
        "bytes_gz": size,
        "sha256": sha.hexdigest(),
        "etag": etag,
        "etag_is_md5": etag == md5.hexdigest(),
        "download_s": round(secs, 2),
        "download_mb_s": round(size / 1e6 / secs, 2),
        "headers": headers,
    }


def measure(hour: str, client: httpx.Client) -> dict[str, object]:
    RAW.mkdir(parents=True, exist_ok=True)
    dest = RAW / f"{hour}.json.gz"
    info = download(client, hour, dest)
    try:
        # Stream line by line: recent hours are ~0.6 GB of JSON, too big to hold in memory.
        types: Counter[str] = Counter()
        field_counts: dict[str, Counter[str]] = defaultdict(Counter)
        t_json = t_orjson = 0.0
        n_bytes = n_lines = n_events = 0
        t_min = t_max = None
        with gzip.open(dest, "rb") as f:
            for line in f:
                n_bytes += len(line)
                n_lines += 1
                if not line.strip():
                    continue
                t0 = time.perf_counter()
                json.loads(line)
                t1 = time.perf_counter()
                e = orjson.loads(line)
                t_orjson += time.perf_counter() - t1
                t_json += t1 - t0
                n_events += 1
                t = str(e.get("type", "<none>"))
                types[t] += 1
                for path in paths(e):
                    field_counts[t][path] += 1
                ts = e.get("created_at")
                if isinstance(ts, str):
                    t_min = ts if t_min is None or ts < t_min else t_min
                    t_max = ts if t_max is None or ts > t_max else t_max
        info["bytes_json"] = n_bytes
        info["parse_json_s"] = round(t_json, 3)
        info["parse_orjson_s"] = round(t_orjson, 3)

        t0 = time.perf_counter()
        n_duck = duckdb.sql(
            f"select count(*) from read_json('{dest.as_posix()}', format='newline_delimited',"
            " records=false, maximum_object_size=104857600)"
        ).fetchone()
        info["parse_duckdb_s"] = round(time.perf_counter() - t0, 3)
        info["duckdb_count"] = n_duck[0] if n_duck else None
        info.update(
            events=n_events,
            lines=n_lines,
            events_per_s_orjson=round(n_events / max(t_orjson, 1e-9)),
            created_at_min=t_min,
            created_at_max=t_max,
            types=dict(types.most_common()),
            # share of events of that type carrying each path, rounded
            fields={
                t: {p: round(c / types[t], 3) for p, c in sorted(fc.items())}
                for t, fc in field_counts.items()
            },
        )
    finally:
        dest.unlink(missing_ok=True)
    return info


def main(hours: list[str]) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    policy = load_policy()
    with httpx.Client(headers={"User-Agent": policy.user_agent}, timeout=300) as client:
        for hour in hours:
            target = OUT / f"{hour}.json"
            if target.exists():
                print(f"{hour}: cached")
                continue
            info = measure(hour, client)
            target.write_text(json.dumps(info, indent=1), encoding="utf-8")
            print(
                f"{hour}: {info['events']} events, {int(info['bytes_gz']) / 1e6:.1f} MB gz, "  # type: ignore[call-overload]
                f"{info['download_mb_s']} MB/s, orjson {info['parse_orjson_s']} s, "
                f"duckdb {info['parse_duckdb_s']} s, etag=md5 {info['etag_is_md5']}"
            )
            time.sleep(1 / policy.limits_for("data.gharchive.org").requests_per_second)


if __name__ == "__main__":
    main(sys.argv[1:])
