"""Client policy (config/policy.yml): user agent, rate limits, disk budget."""

from pathlib import Path

import yaml
from pydantic import BaseModel, Field

from keystone.paths import CONFIG_DIR


class HostLimits(BaseModel):
    max_concurrent: int = Field(ge=1)
    requests_per_second: float = Field(gt=0)


class Policy(BaseModel):
    user_agent: str
    default_limits: HostLimits
    hosts: dict[str, HostLimits] = {}
    disk_budget_fraction: float = Field(gt=0, le=0.7)

    def limits_for(self, host: str) -> HostLimits:
        return self.hosts.get(host.lower(), self.default_limits)


def load_policy(path: Path | None = None) -> Policy:
    path = path or CONFIG_DIR / "policy.yml"
    return Policy.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
