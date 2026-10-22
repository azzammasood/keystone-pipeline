"""Workflow hygiene (ADR-004, ADR-041, ADR-042): every action pinned to a commit SHA, every
schedule behind the off-by-default repository variable, no workflow above read-all by default."""

import re
from pathlib import Path
from typing import Any

import pytest
import yaml

from keystone.paths import REPO_ROOT

WORKFLOWS = sorted((REPO_ROOT / ".github" / "workflows").glob("*.yml"))
PINNED = re.compile(r"^[\w.-]+/[\w./-]+@[0-9a-f]{40}$")
# Weekly checks that predate the schedule switch and cost a few minutes a week.
UNGATED_SCHEDULES = {"parity.yml", "scorecard.yml"}


def _load(path: Path) -> dict[str, Any]:
    doc: dict[str, Any] = yaml.safe_load(path.read_text("utf-8"))
    doc["on"] = doc.pop(True, doc.get("on"))  # YAML 1.1 reads the `on` key as true
    return doc


def _uses(node: object) -> list[str]:
    if isinstance(node, dict):
        found = [v for k, v in node.items() if k == "uses" and isinstance(v, str)]
        return found + [u for v in node.values() for u in _uses(v)]
    if isinstance(node, list):
        return [u for v in node for u in _uses(v)]
    return []


@pytest.mark.parametrize("path", WORKFLOWS, ids=lambda p: p.name)
def test_actions_pinned_by_sha(path: Path) -> None:
    for ref in _uses(_load(path)):
        if not ref.startswith("./"):
            assert PINNED.match(ref), f"{path.name}: {ref} is not pinned to a commit SHA"


@pytest.mark.parametrize("path", WORKFLOWS, ids=lambda p: p.name)
def test_default_token_is_read_only(path: Path) -> None:
    assert _load(path).get("permissions") in ({"contents": "read"}, "read-all", {})


@pytest.mark.parametrize("path", WORKFLOWS, ids=lambda p: p.name)
def test_schedules_are_gated_by_the_switch(path: Path) -> None:
    doc = _load(path)
    if "schedule" not in (doc["on"] or {}) or path.name in UNGATED_SCHEDULES:
        return
    for name, job in doc["jobs"].items():
        assert "vars.KEYSTONE_SCHEDULES == 'true'" in str(job.get("if", "")), (
            f"{path.name}:{name} runs on a schedule without the KEYSTONE_SCHEDULES switch"
        )


def test_new_workflows_present() -> None:
    names = {p.name for p in WORKFLOWS}
    assert {"ingest.yml", "publish.yml", "release.yml"} <= names
