"""The language-model rung of package-to-repository resolution (Phase 7).

A model may only choose among the candidates the rules listed for a package, or decline. Any
answer naming something else is refused here, before it can reach silver, and recorded as a
refusal. The model's stated confidence is stored with its choice; the warehouse (rule `llm`)
accepts a choice only at or above `MIN_CONFIDENCE`, otherwise it stays `suggested`.

The OpenRouter backend needs `OPENROUTER_API_KEY` (env only). Without it the rung is skipped
and logged (PROGRESS.md); conflicts stay `suggested` in the review CSV.
"""

import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, Protocol

from keystone.parsers.models import Utc, _Row
from keystone.parsers.parquet import arrow_schema, write_rows

MIN_CONFIDENCE = 0.8


@dataclass(frozen=True)
class Question:
    package_key: str
    candidates: tuple[str, ...]  # repository keys, e.g. github.com/owner/name
    context: str  # facts the rules saw (sources per candidate); no person data


@dataclass(frozen=True)
class Answer:
    candidate: str | None  # None: the model declines
    confidence: float
    rationale: str = ""


class Chooser(Protocol):
    model: str

    def ask(self, question: Question) -> Answer: ...


class NotConfigured(RuntimeError):
    """No backend credentials: the rung is skipped, never faked."""


class LlmChoiceRow(_Row):
    package_key: str
    candidate: str | None
    confidence: float | None
    outcome: Literal["chosen", "declined", "refused"]
    model: str
    candidates: str  # the listed set, `|`-separated, as asked
    decided_at: Utc


CHOICE_SCHEMA = arrow_schema(LlmChoiceRow)


def validate(question: Question, answer: Answer) -> Literal["chosen", "declined", "refused"]:
    """`refused` when the answer names a candidate that was not listed or a bad confidence."""
    if not 0.0 <= answer.confidence <= 1.0:
        return "refused"
    if answer.candidate is None:
        return "declined"
    return "chosen" if answer.candidate in question.candidates else "refused"


def decide(
    chooser: Chooser, questions: Sequence[Question], now: Callable[[], datetime] | None = None
) -> list[LlmChoiceRow]:
    clock = now or (lambda: datetime.now(UTC))
    rows = []
    for q in questions:
        answer = chooser.ask(q)
        outcome = validate(q, answer)
        rows.append(
            LlmChoiceRow(
                package_key=q.package_key,
                candidate=answer.candidate if outcome == "chosen" else None,
                confidence=answer.confidence if outcome == "chosen" else None,
                outcome=outcome,
                model=chooser.model,
                candidates="|".join(q.candidates),
                decided_at=clock(),
            )
        )
    return rows


def openrouter_chooser() -> Chooser:
    """The configured backend, or NotConfigured. No request is made without a key."""
    if not os.environ.get("OPENROUTER_API_KEY"):
        raise NotConfigured("OPENROUTER_API_KEY is not set: the language-model rung is skipped")
    raise NotConfigured("the OpenRouter backend is written when the owner provides a key")


def write_choices(rows: Sequence[LlmChoiceRow], dest: Path) -> int:
    """Atomic Parquet; an empty list writes a zero-row file (the rung ran, nothing chosen)."""
    return write_rows(rows, CHOICE_SCHEMA, dest)


def choices_dir(root: Path) -> Path:
    return root / "silver" / "resolve" / "llm_choices"


def ensure_choices(root: Path) -> None:
    """A zero-row file so the warehouse can read the rung's output before it ever ran."""
    dest = choices_dir(root) / "_empty.parquet"
    if not dest.exists():
        write_choices([], dest)


def questions(rows: Sequence[tuple[str, str, bool, bool, bool]]) -> list[Question]:
    """One question per tied package: (package_key, repo_key, by_eco, by_npm, by_git_range)."""
    by_package: dict[str, list[tuple[str, str]]] = {}
    for package, repo_key, eco, npm, git in rows:
        named = [s for s, on in (("ecosystems", eco), ("npm", npm), ("osv_git_range", git)) if on]
        by_package.setdefault(package, []).append((repo_key, ",".join(named)))
    return [
        Question(
            package_key=package,
            candidates=tuple(k for k, _ in cands),
            context="; ".join(f"{k}: named by {s}" for k, s in cands),
        )
        for package, cands in by_package.items()
    ]
