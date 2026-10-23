"""The weekly risk brief (Phase 10, ADR-061): prose by a free OpenRouter model over facts read
from the site exports, never numbers of its own.

- Facts: the radar's status and top rows, backtest outcomes, freshness and the producing
  profile, all read from the exported JSON (`keystone export site`).
- Cache: the answer is stored under the SHA-256 of the facts, the model and the prompt
  version, so the same input never asks twice.
- Checks: every number in the text must appear in the facts, the text must avoid predictive
  or shaming wording and must not name people (no `@`, no URLs). A failing text is not
  published (`rejected`, with the reason).
- $0: only a `:free` model whose listed prices are zero, every response's reported cost zero
  (the Phase 7 client, `resolve.chooser`), and at most `MAX_REQUESTS` requests per run.
  Without `OPENROUTER_API_KEY` the brief is `skipped` and says so.
"""

import hashlib
import json
import re
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from keystone.export import envelope

PROMPT_VERSION = "brief-3"
MAX_REQUESTS = 2
MAX_CHARS = 1500
TOP = 5
SOURCES = [
    {"name": "Keystone site exports", "url": "https://github.com/azzammasood/keystone-pipeline"}
]

SYSTEM = (
    "You write a short weekly brief for an open-source supply-chain risk radar. Use only the "
    "facts given as JSON. Write 3 to 6 plain sentences, no lists, no headings. Copy every "
    "number exactly as it appears in the facts, as digits; do not compute, round or add "
    "numbers, and never write a quantity in words. The "
    "score components are 'signals' (say so once); describe data status in plain words. "
    "Never predict what will happen, never say a project will be abandoned or "
    "compromised, never use 'likely', never blame, name or mention people or maintainers. "
    "If the data is partial or pending, say so plainly."
)
BANNED = [
    # predictive or speculative
    r"\b(?:will|would|could|may|might|shall)\b(?:\s+\w+){0,3}\s+"
    r"(?:abandon|compromis|fail|break|collapse|die|disappear|stop)",
    r"\babandon",
    r"\blikely\b",
    r"\bprobabl",
    r"\bexpected to\b",
    r"\bpredict",
    r"\bforecast",
    r"\bgoing to\b",
    r"\bdoomed\b",
    r"\bat risk of\b",
    # shaming or about people
    r"\bnegligen",
    r"\bfault\b",
    r"\bblame",
    r"\blazy\b",
    r"\birresponsib",
    r"\bcareless",
    r"\bincompeten",
    r"\b(?:he|she|him|his|her|hers|maintainers?|authors?|owners?)\b",
    r"https?://",
    r"@",
]
# Numbers in words: the facts hold digits only, so any spelled-out quantity is invented.
NUMBER_WORDS = re.compile(
    r"\b(?:zero|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|"
    r"fourteen|fifteen|sixteen|seventeen|eighteen|nineteen|twenty|thirty|forty|fifty|sixty|"
    r"seventy|eighty|ninety|hundred|thousand|million|billion|dozen|half|double[sd]?|"
    r"triple[sd]?|twice|thrice|quadruple[sd]?|single|percent|first|second|third)\b",
    re.IGNORECASE,
)
_NUM = re.compile(r"-?(?:\d[\d,]*(?:\.\d+)?|\.\d+)")


def iso_week(now: datetime) -> str:
    y, w, _ = now.isocalendar()
    return f"{y}-W{w:02d}"


def facts(site: Path, now: datetime) -> dict[str, Any]:
    radar = json.loads((site / "radar.json").read_text("utf-8"))
    bt = json.loads((site / "backtests.json").read_text("utf-8"))
    pm = json.loads((site / "pipeline_metrics.json").read_text("utf-8"))
    status = radar.get("status") or {"state": "ok", "detail": ""}
    return {
        "week": iso_week(now),
        "radar": {
            "state": status["state"],
            "detail": status["detail"],
            "repositories": radar["row_count"],
            "top": [
                {
                    "repository": r["repo_name"] or f"repository id {r['repo_id']}",
                    "risk": r["risk"],
                    "fragility": r["fragility"],
                    "strongest_signals": [s["name"] for s in r["top_signals"]],
                }
                for r in radar["rows"][:TOP]
            ],
        },
        "backtest": [{"incident": r["id"], "outcome": r["outcome"]} for r in bt["rows"]],
        "data_freshness_target": {"ok": "met", "breach": "missed"}.get(
            pm["freshness"]["status"], "unknown"
        ),
        "engine_profile": radar["produced_by"]["profile"],
        "fell_back_from_full": radar["produced_by"]["fallback"],
    }


def input_sha256(f: dict[str, Any], model: str) -> str:
    blob = json.dumps({"facts": f, "model": model, "prompt": PROMPT_VERSION}, sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()


def _walk(value: Any) -> Iterator[str]:
    if isinstance(value, dict):
        for k, v in value.items():
            yield str(k)
            yield from _walk(v)
    elif isinstance(value, list):
        for v in value:
            yield from _walk(v)
    elif value is not None and not isinstance(value, bool):
        yield str(value)


def _strings(f: dict[str, Any]) -> list[str]:
    """Fact strings with digits that are not plain numbers (2026-W43, a repository name)."""
    found = {s for s in _walk(f) if re.search(r"\d", s) and not _NUM.fullmatch(s)}
    return sorted(found, key=len, reverse=True)


def allowed_numbers(f: dict[str, Any]) -> set[float]:
    """Every number the facts hold, as values or inside their strings (e.g. 0.1 in a detail)."""
    out: set[float] = set()
    for text in _walk(f):
        for m in _NUM.findall(text):
            out.add(float(m.replace(",", "")))
    return out


def check(text: str, f: dict[str, Any]) -> str | None:
    """None when the text passes; else the first reason it fails."""
    if not text.strip():
        return "empty text"
    if len(text) > MAX_CHARS:
        return f"longer than {MAX_CHARS} characters"
    for pat in BANNED:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return f"banned wording: {m.group(0)!r}"
    m = NUMBER_WORDS.search(text)
    if m:
        return f"number in words: {m.group(0)!r}"
    # Fact strings that hold digits (2026-W43, repository names) are fine verbatim; any other
    # digit must stand alone as a number from the facts, never glued to letters (5x, 44.28k).
    rest = text
    for s in _strings(f):
        rest = rest.replace(s, " ")
    glued = re.search(r"[A-Za-z]\d|\d[A-Za-z]", rest)
    if glued:
        return f"digit glued to letters: {glued.group(0)!r}"
    allowed = allowed_numbers(f)
    for m2 in _NUM.findall(rest):
        if float(m2.replace(",", "")) not in allowed:
            return f"number not in the facts: {m2}"
    return None


Ask = Callable[[str, str], str]  # (system, user) -> text


def openrouter_ask(api_key: str, model: str) -> tuple[Ask, Callable[[], int]]:
    from keystone.resolve.chooser import OpenRouterChooser

    # max_retries=0: every request counts against MAX_REQUESTS (refuses a non-:free id)
    client = OpenRouterChooser(api_key, model, max_retries=0)
    client.check_free()

    def ask(system: str, user: str) -> str:
        if client.requests >= MAX_REQUESTS:
            raise RuntimeError(f"request cap {MAX_REQUESTS} reached")
        data = client.post(
            {
                "model": model,
                "temperature": 0,
                "max_tokens": 4000,  # reasoning models think first; free either way
                "reasoning": {"effort": "low"},
                "usage": {"include": True},
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
            }
        )
        return str(data["choices"][0]["message"].get("content") or "").strip()

    return ask, lambda: client.requests


def write_brief(
    site: Path,
    cache_dir: Path,
    *,
    root: Path,
    model: str,
    ask: Ask | None,
    skip_reason: str = "",
    now: datetime | None = None,
) -> dict[str, Any]:
    """Write `site/brief.json` (one row) and return the row."""
    now = now or datetime.now(UTC)
    f = facts(site, now)
    sha = input_sha256(f, model)
    cached = cache_dir / f"{sha}.json"
    row: dict[str, Any]
    hit = json.loads(cached.read_text("utf-8")) if cached.exists() else None
    if hit is not None and hit.get("text") and check(hit["text"], f) is not None:
        hit = None  # the checks tightened since it was cached: never republish unchecked
    if hit is not None:
        row = {**hit, "outcome": "cached"}
    elif ask is None:
        row = _row(f, None, None, sha, "skipped", skip_reason or "no language model configured")
    else:
        user = "Facts (JSON):\n" + json.dumps(f, indent=1, sort_keys=True)
        reason = "no answer"
        text = ""
        error = ""
        for _ in range(MAX_REQUESTS):
            try:
                text = ask(SYSTEM, user)
            except Exception as exc:  # quota, cost guard, network: the exports still publish
                error = f"model error: {type(exc).__name__}: {str(exc)[:200]}"
                break
            found = check(text, f)
            if found is None:
                break
            reason = found
            user += f"\n\nYour last text was refused ({found}). Write it again."
        if error:
            row = _row(f, None, model, sha, "error", error)
        elif check(text, f) is None:
            row = _row(f, text, model, sha, "written", "every number checked against the facts")
            cached.parent.mkdir(parents=True, exist_ok=True)
            cached.write_bytes((json.dumps(row, indent=2) + "\n").encode())
        else:
            row = _row(f, None, model, sha, "rejected", reason)
    state = {"written": "ok", "cached": "ok"}.get(row["outcome"], "skipped")
    doc = envelope.wrap(
        "brief",
        "v1",
        [row],
        produced=envelope.recorded_profile(root),
        sources=SOURCES,
        status=(state, row["detail"]),
        now=now,
    )
    (site / "brief.json").write_bytes((json.dumps(doc, indent=2) + "\n").encode())
    return row


def _row(
    f: dict[str, Any], text: str | None, model: str | None, sha: str, outcome: str, detail: str
) -> dict[str, Any]:
    return {
        "week": f["week"],
        "text": text,
        "model": model,
        "input_sha256": sha,
        "outcome": outcome,
        "detail": detail,
    }
