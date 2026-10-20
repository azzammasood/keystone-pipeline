"""Source definitions (config/sources.yml) and per-source ingestion: GH Archive, OSV."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import yaml
from pydantic import AfterValidator, BaseModel, Field, field_validator

from keystone.paths import CONFIG_DIR


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


UtcDatetime = Annotated[datetime, AfterValidator(_utc)]


class Period(BaseModel):
    name: str
    label: str
    start: UtcDatetime


class GhArchiveConfig(BaseModel):
    base_url: str
    first_hour: UtcDatetime
    publish_lag_hours: int = Field(ge=0)
    late_after_hours: int = Field(ge=0)
    near_empty_bytes: int = Field(ge=0)
    near_empty_from: UtcDatetime
    completeness_periods: list[Period]
    eras: list[Period]
    payload_trim_at: UtcDatetime

    @field_validator("completeness_periods", "eras")
    @classmethod
    def _sorted(cls, periods: list[Period]) -> list[Period]:
        if not periods or [p.start for p in periods] != sorted(p.start for p in periods):
            raise ValueError("periods and eras must be non-empty and sorted by start")
        return periods

    def period_for(self, hour: datetime) -> str:
        """Completeness period (spike-report section 4) that contains `hour`."""
        hour = _utc(hour)
        name = "PRE"
        for period in self.completeness_periods:
            if hour >= period.start:
                name = period.name
        return name

    def era_for(self, hour: datetime) -> str:
        """Schema era (docs/era-map.md) of the file for `hour`; `PRE` before the first era."""
        hour = _utc(hour)
        name = "PRE"
        for era in self.eras:
            if hour >= era.start:
                name = era.name
        return name


class OsvConfig(BaseModel):
    base_url: str
    head_chunk_bytes: int = Field(ge=1024)
    first_run_max_bytes: int = Field(ge=1024)
    ecosystems: list[str]


class Sources(BaseModel):
    gharchive: GhArchiveConfig
    osv: OsvConfig


def load_sources(path: Path | None = None) -> Sources:
    path = path or CONFIG_DIR / "sources.yml"
    return Sources.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
