"""Build small, scrubbed GH Archive fixtures from kept sample hours (data/raw/gharchive).

Usage: uv run python scripts/fixtures/make_gharchive_fixtures.py 2011-02-12-0 2013-06-15-12 ...

Keeps each record's shape (keys and JSON types) so the era parser and drift detector see real
structure, but no real values: every string becomes a placeholder from a counter (never derived
from the value, so nothing can be reversed), every id is renumbered, and only enum-like fields
(type, action, ref_type, ...) and timestamps are kept. Email fields get `redacted@example.invalid`
so tests can prove the parser drops them. Up to `--per-type` records per event type.
"""

import argparse
import gzip
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import orjson

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw" / "gharchive"
OUT = ROOT / "tests" / "fixtures" / "gharchive"
KEEP_STR = {
    "type",
    "action",
    "ref_type",
    "object",
    "state",
    "pusher_type",
    "mergeable_state",
    "role",
}
KEEP_INT = {
    "size",
    "distinct_size",
    "number",
    "commits",
    "additions",
    "deletions",
    "changed_files",
    "comments",
    "review_comments",
    "public",  # early E1 encodes it as 0
}
TIMESTAMP = re.compile(
    r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(Z|[+-]\d{2}:\d{2})"
    r"|\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} [+-]\d{4})$"  # early E1: 2012/03/10 22:36:55 -0800
)
HEX40 = re.compile(r"^[0-9a-f]{40}$")


class Scrubber:
    def __init__(self) -> None:
        self.maps: dict[str, dict[Any, Any]] = defaultdict(dict)

    def _token(self, kind: str, value: Any, make: Any) -> Any:
        table = self.maps[kind]
        if value not in table:
            table[value] = make(len(table) + 1)
        return table[value]

    def string(self, key: str, value: str) -> str:
        if key in KEEP_STR or TIMESTAMP.match(value):
            return value
        if "email" in key:
            return "redacted@example.invalid"
        if HEX40.match(value):
            return self._token("sha", value, lambda n: f"{n:040x}")
        if key == "ref":
            return self._token("ref", value, lambda n: f"refs/heads/branch-{n}")
        if value.count("/") == 1 and ":" not in value:
            owner, name = value.split("/")
            o = self._token("owner", owner, lambda n: f"owner-{n}")
            r = self._token("repo", name, lambda n: f"repo-{n}")
            return f"{o}/{r}"
        if key in ("owner", "login", "actor") or key.endswith("_login"):
            return self._token("owner", value, lambda n: f"owner-{n}")
        if key == "name" and value:
            return self._token("repo", value, lambda n: f"repo-{n}")
        if not value:
            return value
        return self._token("str", value, lambda n: f"s-{n}")

    def scrub(self, value: Any, key: str = "") -> Any:
        if isinstance(value, dict):
            return {k: self.scrub(v, k) for k, v in value.items()}
        if isinstance(value, list):
            return [self.scrub(v, key) for v in value]
        if isinstance(value, bool) or value is None or isinstance(value, float):
            return value
        if isinstance(value, int):
            if key in KEEP_INT:
                return value
            return self._token("int", value, lambda n: 1_000_000 + n)
        if isinstance(value, str):
            return self.string(key, value)
        raise TypeError(type(value))


def build(hour: str, per_type: int) -> Path:
    src = RAW / hour[:4] / hour[5:7] / f"{hour}.json.gz"
    picked: dict[str, int] = defaultdict(int)
    scrubber = Scrubber()
    lines = []
    with gzip.open(src, "rb") as fh:
        for raw in fh:
            rec = orjson.loads(raw)
            etype = str(rec.get("type"))
            if picked[etype] >= per_type:
                continue
            picked[etype] += 1
            lines.append(orjson.dumps(scrubber.scrub(rec)))
    OUT.mkdir(parents=True, exist_ok=True)
    dest = OUT / f"{hour}.json.gz"
    with gzip.GzipFile(dest, "wb", mtime=0) as out:
        out.write(b"\n".join(lines) + b"\n")
    print(f"{dest.name}: {len(lines)} records, {len(picked)} types, {dest.stat().st_size} B")
    return dest


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("hours", nargs="+")
    ap.add_argument("--per-type", type=int, default=3)
    args = ap.parse_args()
    for hour in args.hours:
        build(hour, args.per_type)


if __name__ == "__main__":
    main()
