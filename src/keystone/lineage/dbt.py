"""dbt lineage with openlineage-dbt (`dbt-ol send-events`) over the last run's artifacts.

dbt-ol reads `manifest.json`, `run_results.json` and `catalog.json`, emits one job per model
and test (inputs, outputs, schema, data-quality assertions) and parses each model's compiled
SQL into column lineage (openlineage-sql). openlineage-sql ships no Windows wheel, so on
Windows dbt-ol runs in a pinned Linux container (uv image, the `lineage` dependency group of
the same lock file); on Linux (CI) it runs natively.

dbt-ol writes to a file transport; the events are then canonicalised (naming.py: the DuckDB
file path becomes `duckdb://keystone`, dbt sources over silver Parquet become the silver
datasets the parser writes) and appended to the Keystone event log. Each rewritten event
records how many identifiers were rewritten in a `keystone_canonical` run facet.
"""

import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

import orjson

from keystone import __version__
from keystone.lineage import naming
from keystone.lineage.emit import PRODUCER, write_events
from keystone.paths import REPO_ROOT
from keystone.warehouse.dbt import DBT_DIR, WarehousePaths

UV_IMAGE = (
    "ghcr.io/astral-sh/uv:0.11.25-python3.12-trixie-slim"
    "@sha256:1b9ee30381d729b07044688631ba4473c72db5c048a5f2a504c40186e65efb86"
)
CANONICAL_FACET_SCHEMA = (
    f"https://github.com/azzammasood/keystone-pipeline/blob/v{__version__}"
    "/src/keystone/lineage/dbt.py"
)


def _native() -> bool:
    return shutil.which("dbt-ol") is not None


def _command(
    paths: WarehousePaths, events_file: Path, target: str
) -> tuple[list[str], dict[str, str]]:
    args = ["send-events", "--target", target, "--project-dir", "{dbt}", "--profiles-dir", "{dbt}"]
    env_common = {
        "OPENLINEAGE_NAMESPACE": naming.JOB_NS,
        "OPENLINEAGE__TRANSPORT__TYPE": "file",
        "OPENLINEAGE__TRANSPORT__APPEND": "true",
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
    if _native():
        env = {**os.environ, **paths.env(), **env_common}
        env["OPENLINEAGE__TRANSPORT__LOG_FILE_PATH"] = str(events_file)
        env["DBT_PROFILES_DIR"] = str(DBT_DIR)  # send-events reads the profile from here
        dbt = str(DBT_DIR)
        return ["dbt-ol", *[a.format(dbt=dbt) for a in args]], env
    # Container: repo at /w (read-only), data root at /data, events in /out.
    inner = {
        **env_common,
        "OPENLINEAGE__TRANSPORT__LOG_FILE_PATH": f"/out/{events_file.name}",
        "DBT_PROFILES_DIR": "/w/dbt",
        "KEYSTONE_DATA_ROOT": "/data",
        "KEYSTONE_WAREHOUSE_PATH": "/data/warehouse/keystone.duckdb",
        "KEYSTONE_ROLLUP_PATH": "/data/rollup/rollup.duckdb",
        "DBT_TARGET_PATH": "/data/warehouse/dbt-target",
        "UV_PROJECT_ENVIRONMENT": "/venv",
        "UV_CACHE_DIR": "/uvcache",
        "UV_LINK_MODE": "copy",
    }
    cmd = ["docker", "run", "--rm", "-w", "/w"]
    cmd += ["-v", f"{REPO_ROOT.resolve()}:/w:ro", "-v", f"{paths.root.resolve()}:/data:ro"]
    cmd += ["-v", f"{events_file.parent.resolve()}:/out", "-v", "keystone-uv-cache:/uvcache"]
    cmd += ["-v", "keystone-lineage-venv:/venv"]
    for k, v in inner.items():
        cmd += ["-e", f"{k}={v}"]
    cmd += [UV_IMAGE, "uv", "run", "--frozen", "--no-dev", "--only-group", "lineage"]
    cmd += ["dbt-ol", *[a.format(dbt="/w/dbt") for a in args]]
    return cmd, dict(os.environ)


def _rewrite_ds(ds: dict[str, Any]) -> int:
    n = 0
    ns, name = naming.canonical(ds["namespace"], ds["name"])
    if (ns, name) != (ds["namespace"], ds["name"]):
        ds["namespace"], ds["name"] = ns, name
        n += 1
    cll = ds.get("facets", {}).get("columnLineage", {})
    for spec in cll.get("fields", {}).values():
        for f in spec.get("inputFields", []):
            ns, name = naming.canonical(f["namespace"], f["name"])
            if (ns, name) != (f["namespace"], f["name"]):
                f["namespace"], f["name"] = ns, name
                n += 1
    return n


def canonicalise(event: dict[str, Any]) -> dict[str, Any]:
    n = sum(_rewrite_ds(d) for d in event.get("inputs", []) + event.get("outputs", []))
    if n:
        event.setdefault("run", {}).setdefault("facets", {})["keystone_canonical"] = {
            "_producer": PRODUCER,
            "_schemaURL": CANONICAL_FACET_SCHEMA,
            "rewritten_identifiers": n,
        }
    return event


def send_events(root: Path, target: str = "duckdb") -> list[dict[str, Any]]:
    """Run dbt-ol over the artifacts of the last `warehouse build`; return the canonical events
    (already appended to the event log and forwarded to Marquez when configured)."""
    paths = WarehousePaths(root)
    if not (paths.target_dir / "run_results.json").exists():
        raise FileNotFoundError("run `keystone warehouse build` first (no run_results.json)")
    with tempfile.TemporaryDirectory(dir=root) as tmp:
        events_file = Path(tmp) / "dbt-ol.jsonl"
        cmd, env = _command(paths, events_file, target)
        proc = subprocess.run(cmd, env=env, capture_output=True, text=True, encoding="utf-8")
        if proc.returncode != 0 or not events_file.exists():
            tail = (proc.stdout + proc.stderr)[-3000:]
            raise RuntimeError(f"dbt-ol failed (exit {proc.returncode}):\n{tail}")
        raw = [orjson.loads(line) for line in events_file.read_bytes().splitlines() if line]
    events = [canonicalise(e) for e in raw]
    write_events(root, events)
    return events
