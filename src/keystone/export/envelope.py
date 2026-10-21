"""The versioned export envelope (ADR-204): every published dataset is rows wrapped in a header
that records the window, sources, privacy level and the profile that produced them.

`wrap` takes `produced_by` from the run log (the latest `profile` stage), so an export can only
claim a profile the pipeline recorded; with no recorded decision it refuses to wrap.
"""

import json
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import jsonschema

from keystone import __version__
from keystone.paths import CONTRACTS_DIR
from keystone.telemetry.metrics import produced_by
from keystone.telemetry.runlog import read

SCHEMA_PATH = CONTRACTS_DIR / "export_envelope.schema.json"
ENVELOPE_VERSION = "1.0"
CORRECTIONS_URL = "https://github.com/azzammasood/keystone-pipeline/issues"


class NoProfileRecorded(RuntimeError):
    """No `keystone profile check --run` in the run log: which profile produced the data is
    unknown, and an export never guesses it."""


def recorded_profile(root: Path) -> dict[str, Any]:
    found = produced_by(read(root))
    if found is None or found["status"] != "ok":
        raise NoProfileRecorded(f"no successful profile decision in {root / 'telemetry'}")
    return {
        "profile": found["profile"],
        "fallback": found["fallback"],
        "run_id": found["run_id"],
        "keystone_version": __version__,
    }


def wrap(
    dataset: str,
    dataset_version: str,
    rows: Sequence[dict[str, Any]],
    *,
    produced: dict[str, Any],
    sources: Sequence[dict[str, str]],
    window_start: datetime | None = None,
    window_end: datetime | None = None,
    periods: Sequence[str] = (),
    level: str = "repository",
    now: datetime | None = None,
) -> dict[str, Any]:
    doc = {
        "envelope_version": ENVELOPE_VERSION,
        "dataset": dataset,
        "dataset_version": dataset_version,
        "generated_at": (now or datetime.now(UTC)).isoformat(),
        "produced_by": produced,
        "window": {
            "start": window_start.isoformat() if window_start else None,
            "end": window_end.isoformat() if window_end else None,
            "periods": list(periods),
        },
        "sources": [dict(s) for s in sources],
        "privacy": {"level": level, "people": "aggregate-only"},
        "corrections": CORRECTIONS_URL,
        "row_count": len(rows),
        "rows": [dict(r) for r in rows],
    }
    validate(doc)
    return doc


def validate(doc: dict[str, Any]) -> None:
    schema = json.loads(SCHEMA_PATH.read_text("utf-8"))
    jsonschema.validate(doc, schema, format_checker=jsonschema.FormatChecker())
    if doc["row_count"] != len(doc["rows"]):
        raise jsonschema.ValidationError(f"row_count {doc['row_count']} != {len(doc['rows'])} rows")
