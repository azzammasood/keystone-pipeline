"""Run dbt (programmatic invocation) with paths derived from a data root.

Orchestrators call `keystone warehouse build`, never dbt directly, so paths, the target and
the docs-coverage gate are the same everywhere. Paths are absolute with forward slashes:
DuckDB globs and dbt Jinja both accept them on Windows and Linux.
"""

import os
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from keystone.paths import REPO_ROOT

DBT_DIR = REPO_ROOT / "dbt"


@dataclass(frozen=True)
class WarehousePaths:
    root: Path

    @property
    def warehouse_db(self) -> Path:
        return self.root / "warehouse" / "keystone.duckdb"

    @property
    def rollup_db(self) -> Path:
        return self.root / "rollup" / "rollup.duckdb"

    @property
    def target_dir(self) -> Path:
        return self.root / "warehouse" / "dbt-target"

    def env(self) -> dict[str, str]:
        return {
            "KEYSTONE_DATA_ROOT": self.root.resolve().as_posix(),
            "KEYSTONE_WAREHOUSE_PATH": self.warehouse_db.resolve().as_posix(),
            "KEYSTONE_ROLLUP_PATH": self.rollup_db.resolve().as_posix(),
            "DBT_TARGET_PATH": self.target_dir.resolve().as_posix(),
            "DBT_LOG_PATH": (self.target_dir / "logs").resolve().as_posix(),
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
        }


@contextmanager
def _environ(values: dict[str, str]) -> Iterator[None]:
    old = {k: os.environ.get(k) for k in values}
    os.environ.update(values)
    try:
        yield
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def run_dbt(args: list[str], paths: WarehousePaths, target: str = "duckdb") -> bool:
    """`dbt <args>` in the project directory; True when every node succeeded."""
    from dbt.cli.main import dbtRunner  # heavy import, only when dbt is used

    missing = [p for p in (paths.rollup_db,) if not p.exists()]
    if missing:
        raise FileNotFoundError(f"run `keystone rollup run` first: {missing[0]} is missing")
    paths.warehouse_db.parent.mkdir(parents=True, exist_ok=True)
    common = ["--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR), "--target", target]
    with _environ(paths.env()):
        result = dbtRunner().invoke([*args, *common])
    return bool(result.success)
