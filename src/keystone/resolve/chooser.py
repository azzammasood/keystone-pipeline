"""The language-model rung of package-to-repository resolution (Phase 7).

A model may only choose among the candidates the rules listed for a package, or decline. Any
answer naming something else is refused here, before it can reach silver, and recorded as a
refusal. The model's stated confidence is stored with its choice; the warehouse (rule `llm`)
accepts a choice only at or above `MIN_CONFIDENCE`, otherwise it stays `suggested`.

The OpenRouter backend needs `OPENROUTER_API_KEY` (env only). Without it the rung is skipped
and logged (PROGRESS.md); conflicts stay `suggested` in the review CSV. It is $0 by
construction (CLAUDE.md): only a `:free` model id, its listed prices checked to be zero before
the first request, and every response's reported cost checked to be zero, else it stops.
"""

import json
import os
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol

import httpx

from keystone.parsers.models import Utc, _Row
from keystone.parsers.parquet import arrow_schema, write_rows

MIN_CONFIDENCE = 0.8
OPENROUTER = "https://openrouter.ai/api/v1"
DEFAULT_MODEL = "nvidia/nemotron-3-ultra-550b-a55b:free"  # structured output works, $0 (2026-10-22)
UNPARSEABLE = "\x00unparseable"  # never a listed candidate: an unreadable answer is refused


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


class DailyQuota(RuntimeError):
    """The free daily request quota is used up: stop, keep the answers so far, resume later."""


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
        try:
            answer = chooser.ask(q)
        except DailyQuota:
            break  # the rows so far are kept; the rest is asked on a later run
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


class WouldCost(RuntimeError):
    """The model or a response is not free: a hard stop (CLAUDE.md, $0 only)."""


SYSTEM = (
    "You map a software package to the source repository where it is developed. Choose only "
    "one of the listed candidate keys, or null when the facts do not settle it. Answer with "
    "JSON: candidate (a listed key or null), confidence (0 to 1), rationale (one sentence)."
)


class OpenRouterChooser:
    """Chat completions on a free OpenRouter model, paced by policy.yml (`openrouter.ai`)."""

    def __init__(
        self,
        api_key: str,
        model: str = DEFAULT_MODEL,
        client: httpx.Client | None = None,
        min_interval: float = 4.0,
        max_retries: int = 3,
    ) -> None:
        if not model.endswith(":free"):
            raise WouldCost(f"{model} is not a :free model")
        self.model = model
        self.min_interval = min_interval
        self.max_retries = max_retries
        self._next = 0.0
        self._http = client or httpx.Client(timeout=120.0)
        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "X-Title": "keystone-pipeline",
        }
        self.requests = 0

    def check_free(self) -> None:
        """The model is listed and every price is zero."""
        got = self._http.get(f"{OPENROUTER}/models", headers=self._headers)
        got.raise_for_status()
        listed = {m["id"]: m for m in got.json().get("data", [])}
        if self.model not in listed:
            raise WouldCost(f"{self.model} is not listed by OpenRouter")
        prices = listed[self.model].get("pricing") or {}
        if any(float(v or 0) != 0 for v in prices.values()):
            raise WouldCost(f"{self.model} has a non-zero price: {prices}")

    def _post(self, body: dict[str, Any]) -> dict[str, Any]:
        for attempt in range(self.max_retries + 1):
            wait = self._next - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._next = time.monotonic() + self.min_interval
            self.requests += 1
            resp = self._http.post(
                f"{OPENROUTER}/chat/completions", headers=self._headers, json=body
            )
            if resp.status_code == 402:
                raise WouldCost("OpenRouter answered 402 (payment required)")
            if resp.status_code == 429 and "per-day" in resp.text:
                raise DailyQuota("OpenRouter free-model requests for today are used up")
            if resp.status_code in (429, 500, 502, 503) and attempt < self.max_retries:
                retry = resp.headers.get("retry-after", "")
                self._next = time.monotonic() + (float(retry) if retry.isdigit() else 20.0)
                continue
            resp.raise_for_status()
            data: dict[str, Any] = resp.json()
            cost = (data.get("usage") or {}).get("cost")
            if cost is not None and float(cost) != 0:
                raise WouldCost(f"a response reported cost {cost}")
            return data
        raise RuntimeError("unreachable")

    def ask(self, question: Question) -> Answer:
        schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["candidate", "confidence", "rationale"],
            "properties": {
                "candidate": {"type": ["string", "null"], "enum": [*question.candidates, None]},
                "confidence": {"type": "number", "minimum": 0, "maximum": 1},
                "rationale": {"type": "string"},
            },
        }
        body = {
            "model": self.model,
            "temperature": 0,
            # reasoning models think before the JSON; a low cap truncates it (free either way)
            "max_tokens": 4000,
            "reasoning": {"effort": "low"},
            "usage": {"include": True},
            "messages": [
                {"role": "system", "content": SYSTEM},
                {
                    "role": "user",
                    "content": f"Package: {question.package_key}\nCandidates: {question.context}",
                },
            ],
            "response_format": {
                "type": "json_schema",
                "json_schema": {"name": "choice", "strict": True, "schema": schema},
            },
        }
        data = self._post(body)
        try:
            text = data["choices"][0]["message"]["content"] or ""
            start, end = text.find("{"), text.rfind("}")
            parsed = json.loads(text[start : end + 1])
            candidate = parsed.get("candidate")
            confidence = float(parsed.get("confidence"))
        except (KeyError, IndexError, TypeError, ValueError):
            return Answer(UNPARSEABLE, 0.0, "unparseable answer")
        if candidate is not None and not isinstance(candidate, str):
            return Answer(UNPARSEABLE, 0.0, "candidate is not text")
        return Answer(candidate, confidence, str(parsed.get("rationale", ""))[:300])


def openrouter_chooser(model: str | None = None) -> OpenRouterChooser:
    """The configured backend, or NotConfigured. No request is made without a key."""
    key = os.environ.get("OPENROUTER_API_KEY")
    if not key:
        raise NotConfigured("OPENROUTER_API_KEY is not set: the language-model rung is skipped")
    from keystone.policy import load_policy

    rps = load_policy().limits_for("openrouter.ai").requests_per_second
    backend = OpenRouterChooser(
        key,
        model or os.environ.get("KEYSTONE_LLM_MODEL", DEFAULT_MODEL),
        min_interval=1.0 / rps,
    )
    backend.check_free()
    return backend


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


def unanswered(questions: Sequence[Question], answered: set[tuple[str, str]]) -> list[Question]:
    """Questions without a valid choice for exactly the same candidates (saves the quota)."""
    return [q for q in questions if (q.package_key, "|".join(q.candidates)) not in answered]


def questions(rows: Sequence[tuple[str, str, str, bool, bool, bool]]) -> list[Question]:
    """One question per tied package, from rows (package_key, repo_key, repo_full_name, by_eco,
    by_npm, by_git_range). The context names repositories and sources only."""
    by_package: dict[str, list[tuple[str, str]]] = {}
    for package, repo_key, full_name, eco, npm, git in rows:
        named = [s for s, on in (("ecosystems", eco), ("npm", npm), ("osv_git_range", git)) if on]
        facts = f"repository {full_name}, named by {', '.join(named) or 'no registry'}"
        by_package.setdefault(package, []).append((repo_key, facts))
    return [
        Question(
            package_key=package,
            candidates=tuple(k for k, _ in cands),
            context="; ".join(f"{k} ({facts})" for k, facts in cands),
        )
        for package, cands in by_package.items()
    ]
