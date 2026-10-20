"""Phase 1 regression oracles for GH Archive sample hours (docs/data-audit.md)."""

import json
import os
import time

import httpx
import pytest

from keystone.paths import REPO_ROOT
from keystone.policy import load_policy

ORACLES = json.loads(
    (REPO_ROOT / "tests" / "fixtures" / "oracles" / "gharchive_hours.json").read_text(
        encoding="utf-8"
    )
)["hours"]


def test_oracles_are_self_consistent() -> None:
    assert len(ORACLES) >= 20
    for hour, o in ORACLES.items():
        assert sum(o["types"].values()) == o["events"], hour
        assert len(o["sha256"]) == 64 and len(o["etag_md5"]) == 32, hour


def test_first_hour_and_known_breaks() -> None:
    assert ORACLES["2011-02-12-0"]["events"] == 1552
    # Capture drop at 2025-05-23 ~18h (same weekday, same hour): about -36%.
    assert ORACLES["2025-05-29-12"]["events"] / ORACLES["2025-05-22-12"]["events"] < 0.7
    # Non-push types nearly vanish by mid-2026 (E3 coverage collapse).
    assert ORACLES["2026-08-23-12"]["types"]["PullRequestEvent"] < 100


@pytest.mark.network
@pytest.mark.skipif(os.environ.get("KEYSTONE_NETWORK_TESTS") != "1", reason="network test")
def test_files_unchanged_upstream() -> None:
    policy = load_policy()
    pause = 1 / policy.limits_for("data.gharchive.org").requests_per_second
    with httpx.Client(headers={"User-Agent": policy.user_agent}, timeout=60) as c:
        for hour, o in ORACLES.items():
            r = c.head(f"https://data.gharchive.org/{hour}.json.gz")
            assert r.status_code == 200, hour
            assert r.headers["etag"].strip('"') == o["etag_md5"], f"{hour} republished"
            time.sleep(pause)
