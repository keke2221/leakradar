"""Founder brief generation."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional

import requests

from . import config
from .db import get_connection
from .news import latest_topics


@dataclass
class Brief:
    ts: datetime
    sector: str
    title: str
    summary: str
    sources: List[str]


def _load_comparisons():
    with get_connection() as conn:
        df = conn.execute("SELECT ts, sector, hype_index, reality_index, gap FROM comparisons").fetchall()
    if not df:
        return []
    rows = []
    for row in df:
        ts = datetime.fromisoformat(row["ts"])
        rows.append((ts, row["sector"], row["hype_index"], row["reality_index"], row["gap"]))
    return rows


def _top_components() -> Dict[str, List[str]]:
    with get_connection() as conn:
        rows = conn.execute("SELECT ts, sector, components FROM scores").fetchall()
    if not rows:
        return {}
    latest_ts = max(datetime.fromisoformat(r["ts"]) for r in rows)
    picks: Dict[str, List[str]] = {}
    for row in rows:
        if datetime.fromisoformat(row["ts"]) != latest_ts:
            continue
        try:
            comps = json.loads(row["components"])
        except json.JSONDecodeError:
            continue
        sorted_metrics = sorted(comps.items(), key=lambda item: abs(item[1]), reverse=True)
        picks[row["sector"]] = [f"{name}: {value:+.2f}" for name, value in sorted_metrics[:3]]
    return picks


def _perplexity_prompt(sector: str, hype: float, reality: float, components: List[str], sources: List[str]) -> str:
    return (
        f"You are generating a daily founder brief for the {sector} sector. "
        f"Hype index is {hype:.1f} (media + social buzz) while reality index is {reality:.1f} "
        "based on hiring, repos, papers, and grants. "
        "Summarize the gap and what actions founders should consider in 6-8 sentences. "
        f"Key metrics: {', '.join(components)}. "
        f"Sources to cite: {', '.join(sources[:5])}. Return concise prose with actionable tone."
    )


def _call_perplexity(prompt: str) -> Optional[str]:
    if not (config.PERPLEXITY_API_KEY and config.USE_PERPLEXITY):
        return None
    try:
        resp = requests.post(
            "https://api.perplexity.ai/chat/completions",
            headers={"Authorization": f"Bearer {config.PERPLEXITY_API_KEY}"},
            json={
                "model": "pplx-7b-chat",
                "messages": [{"role": "system", "content": "You are a concise market analyst."}, {"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"]
    except Exception:
        return None


def _fallback_summary(sector: str, hype: float, reality: float, components: List[str], sources: List[str]) -> str:
    direction = "outpacing" if hype > reality else "lagging"
    lines = [
        f"Hype is {direction} fundamentals in {sector}: hype {hype:.1f} vs reality {reality:.1f}.",
        f"Reality highlights: {', '.join(components) if components else 'steady signals'}.",
    ]
    if sources:
        lines.append(f"Sources to review: {', '.join(sources[:3])}.")
    lines.append("Action: reconcile narrative vs execution by double-checking hiring, shipping, and capital plans.")
    return " ".join(lines)


def generate_briefs() -> List[Brief]:
    comparisons = _load_comparisons()
    if not comparisons:
        return []
    latest_ts = max(row[0] for row in comparisons)
    comparisons = [row for row in comparisons if row[0] == latest_ts]
    topics = latest_topics()
    top_components = _top_components()
    briefs: List[Brief] = []
    for ts, sector, hype, reality, _ in comparisons:
        sector_sources = topics.get(sector, {}).get("sources", [])
        sector_components = top_components.get(sector, [])
        prompt = _perplexity_prompt(sector, hype, reality, sector_components, sector_sources)
        summary = _call_perplexity(prompt) or _fallback_summary(sector, hype, reality, sector_components, sector_sources)
        title = f"{sector.title()} Founder Brief"
        briefs.append(Brief(ts, sector, title, summary, sector_sources[:5]))
    return briefs
