"""robots.txt rules per RFC 9309, with `*` and `$` path patterns.

`urllib.robotparser` matches rule paths as plain prefixes, so `Disallow: /pypi/*/json`
(pypi.org) would never match `/pypi/requests/json`. Here the group for our product token is
used when one exists, else the `*` group; the longest matching rule wins and `Allow` wins a
tie (RFC 9309, 2.2.2). Anything that is not a rule line (comments, JSON) is ignored.
"""

import re
from dataclasses import dataclass, field
from urllib.parse import unquote, urlsplit


@dataclass(frozen=True)
class _Rule:
    allow: bool
    path: str
    pattern: re.Pattern[str]


def _compile(path: str) -> re.Pattern[str]:
    anchored = path.endswith("$")
    body = path[:-1] if anchored else path
    regex = ".*".join(re.escape(part) for part in body.split("*"))
    return re.compile(regex + ("$" if anchored else ""))


@dataclass
class Robots:
    groups: dict[str, list[_Rule]] = field(default_factory=dict)

    @classmethod
    def parse(cls, text: str) -> "Robots":
        robots = cls()
        agents: list[str] = []
        in_rules = False
        for raw in text.splitlines():
            line = raw.split("#", 1)[0].strip()
            if ":" not in line:
                continue
            key, value = (s.strip() for s in line.split(":", 1))
            key = key.lower()
            if key == "user-agent":
                if in_rules:  # a user-agent line after rules starts a new group
                    agents, in_rules = [], False
                agents.append(value.lower())
                for agent in agents:
                    robots.groups.setdefault(agent, [])
            elif key in ("allow", "disallow") and agents:
                in_rules = True
                if value:  # an empty Disallow allows everything
                    rule = _Rule(key == "allow", value, _compile(unquote(value)))
                    for agent in agents:
                        robots.groups[agent].append(rule)
        return robots

    def can_fetch(self, user_agent: str, url: str) -> bool:
        token = user_agent.split("/", 1)[0].strip().lower()
        rules = self.groups.get(token, self.groups.get("*", []))
        parts = urlsplit(url)
        target = unquote(parts.path or "/") + (f"?{parts.query}" if parts.query else "")
        best: _Rule | None = None
        for rule in rules:
            if rule.pattern.match(target) and (
                best is None
                or len(rule.path) > len(best.path)
                or (len(rule.path) == len(best.path) and rule.allow)
            ):
                best = rule
        return best is None or best.allow
