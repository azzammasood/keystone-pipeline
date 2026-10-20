from pathlib import Path

import pytest
from pydantic import ValidationError

from keystone.policy import load_policy


def test_gharchive_limits_are_polite() -> None:
    limits = load_policy().limits_for("data.gharchive.org")
    assert limits.max_concurrent <= 2
    assert limits.requests_per_second <= 2.0


def test_unknown_host_gets_default() -> None:
    policy = load_policy()
    assert policy.limits_for("example.org") == policy.default_limits


def test_disk_budget_capped_at_70_percent(tmp_path: Path) -> None:
    bad = tmp_path / "policy.yml"
    bad.write_text(
        "user_agent: x\ndisk_budget_fraction: 0.9\n"
        "default_limits: {max_concurrent: 1, requests_per_second: 1}\n",
        encoding="utf-8",
    )
    with pytest.raises(ValidationError):
        load_policy(bad)
