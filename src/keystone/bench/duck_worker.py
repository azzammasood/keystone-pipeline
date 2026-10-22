"""DuckDB rung of the ladder, run in its own process so its peak memory is its own.

Usage: python -m keystone.bench.duck_worker <input dir> <output dir>
Prints one JSON line: {"engine_seconds": .., "peak_bytes": ..}.
"""

import json
import sys
import time
from pathlib import Path

import duckdb

from keystone.bench.ladder import peak_rss_bytes
from keystone.rollup.spec import duckdb_select, input_sql, rollups


def run(in_dir: Path, out_dir: Path) -> float:
    started = time.perf_counter()
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")
    con.execute("SET enable_progress_bar = false")
    src = (in_dir / "*.parquet").as_posix()
    con.execute(f"CREATE VIEW events AS SELECT * FROM read_parquet('{src}')")
    con.execute(f"CREATE VIEW rollup_input AS {input_sql()}")
    for r in rollups():
        target = out_dir / r.name
        target.mkdir(parents=True, exist_ok=True)
        con.execute(
            f"COPY ({duckdb_select(r)}) TO '{(target / 'part-0.parquet').as_posix()}' "
            "(FORMAT parquet, COMPRESSION zstd)"
        )
    con.close()
    return time.perf_counter() - started


def main() -> None:
    seconds = run(Path(sys.argv[1]), Path(sys.argv[2]))
    print(json.dumps({"engine_seconds": seconds, "peak_bytes": peak_rss_bytes()}))


if __name__ == "__main__":
    main()
