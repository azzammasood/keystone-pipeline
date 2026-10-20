"""Privacy at read time (ADR-013, CLAUDE.md): salted actor keys and bot flags.

Logins, emails, names and profile fields never leave the parser. A person is represented only
by `actor_key`, an HMAC-SHA256 of the GitHub account id (E0, E2, E3) or the lower-cased login
(E1, which has no ids), keyed with `KEYSTONE_ACTOR_SALT` from the environment.
"""

import hashlib
import hmac
import os
from functools import lru_cache
from pathlib import Path
from typing import Literal

import yaml

from keystone.paths import CONFIG_DIR

SALT_ENV = "KEYSTONE_ACTOR_SALT"
MIN_SALT_LENGTH = 16
KeyKind = Literal["id", "login"]


class SaltMissing(RuntimeError):
    """The salt is required; there is no default, so keys cannot be computed without it."""


def salt_from_env() -> bytes:
    salt = os.environ.get(SALT_ENV, "")
    if len(salt) < MIN_SALT_LENGTH:
        raise SaltMissing(f"{SALT_ENV} must be set (at least {MIN_SALT_LENGTH} characters)")
    return salt.encode()


class ActorHasher:
    """Keyed hash of an account. Cached: the same accounts recur many times per hour."""

    def __init__(self, salt: bytes) -> None:
        self._salt = salt
        self._cache: dict[str, str] = {}

    def key(self, kind: KeyKind, value: int | str) -> str:
        token = f"{kind}:{value if kind == 'id' else str(value).lower()}"
        found = self._cache.get(token)
        if found is None:
            found = hmac.new(self._salt, token.encode(), hashlib.sha256).hexdigest()[:32]
            if len(self._cache) < 1_000_000:
                self._cache[token] = found
        return found


@lru_cache(maxsize=1)
def bot_logins(path: Path | None = None) -> frozenset[str]:
    path = path or CONFIG_DIR / "bots.yml"
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return frozenset(str(x).lower() for x in data.get("logins", []))


def is_bot(login: str | None) -> bool:
    if not login:
        return False
    low = login.lower()
    return low.endswith("[bot]") or low in bot_logins()
