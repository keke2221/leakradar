"""Entity normalization helpers (nice-to-have)."""

from __future__ import annotations

from typing import Dict

ALIAS_MAP: Dict[str, str] = {
    "openai": "OpenAI",
    "open-ai": "OpenAI",
    "tesla": "Tesla",
    "hugging face": "HuggingFace",
}


def normalize(name: str) -> str:
    key = name.strip().lower()
    return ALIAS_MAP.get(key, name.strip())
