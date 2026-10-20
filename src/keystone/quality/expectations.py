"""Great Expectations suites on the parsed GH Archive tables, validated one source file at a time.

The suites hold the era rules that a column-level contract cannot express on its own (keys and
ids per era, UTC offsets per era, trimmed pushes in E3). GX usage analytics are switched off
before GX is imported: no data or metadata leaves the machine.
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from keystone.parsers.parquet import EVENT_SCHEMA, HOUR_TYPE_SCHEMA, QUARANTINE_SCHEMA
from keystone.sources import load_sources

os.environ["GX_ANALYTICS_ENABLED"] = "False"

ERAS = ["E0", "E1", "E2", "E3"]
PERIODS = ["C1", "C2", "GAP", "C3", "C4", "C5"]
KEY = r"^[0-9a-f]{32}$"
TRIM_AT = load_sources().gharchive.payload_trim_at.isoformat()  # GX conditions need a string


def _events_suite(gx: Any) -> list[Any]:
    e = gx.expectations
    required = [
        "source_file",
        "source_sha256",
        "fetched_at",
        "source_line",
        "era",
        "period",
        "event_type",
        "created_at",
        "created_at_offset_min",
        "actor_is_bot",
    ]
    from great_expectations.expectations.row_conditions import Column

    era, etype = Column(name="era"), Column(name="event_type")
    created = Column(name="created_at")
    e1, not_e1 = era == "E1", era != "E1"
    push = etype == "PushEvent"

    def where(cond: Any) -> dict[str, Any]:
        return {"row_condition": cond}

    out = [e.ExpectTableColumnsToMatchOrderedList(column_list=EVENT_SCHEMA.names)]
    out += [e.ExpectColumnValuesToNotBeNull(column=c) for c in required]
    out += [
        e.ExpectCompoundColumnsToBeUnique(column_list=["source_file", "source_line"]),
        e.ExpectColumnValuesToBeInSet(column="era", value_set=ERAS),
        e.ExpectColumnValuesToBeInSet(column="period", value_set=PERIODS),
        e.ExpectColumnValuesToBeInSet(column="actor_key_kind", value_set=["id", "login"]),
        e.ExpectColumnValuesToMatchRegex(column="actor_key", regex=KEY),
        e.ExpectColumnValuesToMatchRegex(column="pr_author_key", regex=KEY),
        e.ExpectColumnValuesToMatchRegex(column="issue_author_key", regex=KEY),
        e.ExpectColumnValuesToMatchRegex(column="source_sha256", regex=r"^[0-9a-f]{64}$"),
        e.ExpectColumnValuesToMatchRegex(column="repo_name", regex=r"^[^/]+/[^/]+$"),
        e.ExpectColumnValuesToMatchRegex(column="event_type", regex=r"^[A-Za-z]+Event$"),
        # ADR-013: E1 keys are logins, every other era keys by account id.
        e.ExpectColumnValuesToBeInSet(column="actor_key_kind", value_set=["login"], **where(e1)),
        e.ExpectColumnValuesToBeInSet(column="actor_key_kind", value_set=["id"], **where(not_e1)),
        e.ExpectColumnValuesToBeNull(column="event_id", **where(e1)),
        e.ExpectColumnValuesToNotBeNull(column="event_id", **where(not_e1)),
        # UTC normalisation: only E1 sources carry offsets (Pacific, -07:00 or -08:00).
        e.ExpectColumnValuesToBeInSet(
            column="created_at_offset_min", value_set=[-420, -480], **where(e1)
        ),
        e.ExpectColumnValuesToBeInSet(
            column="created_at_offset_min", value_set=[0], **where(not_e1)
        ),
        # GitHub's payload trim by event time: pushes after it have no size, before it they do.
        e.ExpectColumnValuesToBeNull(column="push_size", **where((created > TRIM_AT) & push)),
        e.ExpectColumnValuesToNotBeNull(column="push_size", **where((created < TRIM_AT) & push)),
        e.ExpectColumnValuesToBeNull(column="pr_merged", **where(era == "E3")),
    ]
    return out


def _hour_types_suite(gx: Any) -> list[Any]:
    e = gx.expectations
    return [
        e.ExpectTableColumnsToMatchOrderedList(column_list=HOUR_TYPE_SCHEMA.names),
        e.ExpectCompoundColumnsToBeUnique(column_list=["source_file", "event_type"]),
        e.ExpectColumnValuesToBeBetween(column="events", min_value=1),
        e.ExpectColumnValuesToBeInSet(column="era", value_set=ERAS),
        e.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="created_at_max", column_B="created_at_min", or_equal=True
        ),
    ]


def _quarantine_suite(gx: Any) -> list[Any]:
    e = gx.expectations
    return [
        e.ExpectTableColumnsToMatchOrderedList(column_list=QUARANTINE_SCHEMA.names),
        e.ExpectColumnValuesToNotBeNull(column="reason"),
        e.ExpectColumnValuesToMatchRegex(column="line_sha256", regex=r"^[0-9a-f]{64}$"),
    ]


SUITES = {"events": _events_suite, "hour_types": _hour_types_suite, "quarantine": _quarantine_suite}


@dataclass(frozen=True)
class SuiteResult:
    table: str
    file: str
    rows: int
    failed: list[str]

    @property
    def ok(self) -> bool:
        return not self.failed


class Validator:
    """One ephemeral GX context with the three suites; validate a Parquet file at a time."""

    def __init__(self) -> None:
        import great_expectations as gx  # heavy; optional `quality` group

        self.gx = gx
        self.context = gx.get_context(mode="ephemeral")
        from great_expectations.data_context.types.base import ProgressBarsConfig

        self.context.variables.progress_bars = ProgressBarsConfig(globally=False)
        source = self.context.data_sources.add_pandas("parquet")
        self.validations: dict[str, Any] = {}
        for table, build in SUITES.items():
            batch = source.add_dataframe_asset(table).add_batch_definition_whole_dataframe("file")
            suite = self.context.suites.add(gx.ExpectationSuite(name=table))
            for expectation in build(gx):
                suite.add_expectation(expectation)
            self.validations[table] = self.context.validation_definitions.add(
                gx.ValidationDefinition(name=table, data=batch, suite=suite)
            )

    def validate(self, table: str, path: Path) -> SuiteResult:
        import pyarrow.parquet as pq

        frame = pq.read_table(path).to_pandas()
        run = self.validations[table].run(batch_parameters={"dataframe": frame})
        failed = [_describe(r.expectation_config) for r in run.results if not r.success]
        return SuiteResult(table, path.as_posix(), len(frame), failed)


def _describe(config: Any) -> str:
    kwargs = {k: v for k, v in config.kwargs.items() if k in ("column", "row_condition")}
    return f"{config.type} {kwargs}"
