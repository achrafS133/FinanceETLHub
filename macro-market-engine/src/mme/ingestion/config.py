"""Load the declarative source catalog (config/sources.yml)."""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

import yaml


def _find_config() -> Path:
    # Allow override (e.g. Fabric mounts config to a known path).
    env = os.getenv("MME_SOURCES_YML")
    if env:
        return Path(env)
    # Walk up from this file to the project root that holds config/sources.yml.
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "config" / "sources.yml"
        if candidate.exists():
            return candidate
    raise FileNotFoundError("config/sources.yml not found; set MME_SOURCES_YML.")


@lru_cache(maxsize=1)
def load_sources() -> Dict[str, Any]:
    """Return the parsed source catalog as a dict."""
    with open(_find_config(), "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)
