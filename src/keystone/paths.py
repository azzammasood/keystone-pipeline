"""Repository paths. Always use pathlib; never hardcode separators."""

import os
from pathlib import Path

REPO_ROOT = Path(os.environ.get("KEYSTONE_ROOT", Path(__file__).resolve().parents[2]))
DATA_DIR = REPO_ROOT / "data"
CONFIG_DIR = REPO_ROOT / "config"
DOCS_DIR = REPO_ROOT / "docs"
