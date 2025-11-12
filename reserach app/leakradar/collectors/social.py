"""Social / sentiment collector (SerpAPI optional)."""

from __future__ import annotations

from datetime import datetime, timezone

import requests

from core import config
from core.db import get_connection


def _serp_count(query: str) -> int:
    if not config.SERPAPI_KEY:
        return 0
    try:
        resp = requests.get(
            "https://serpapi.com/search.json",
            params={"engine": "google", "q": query, "api_key": config.SERPAPI_KEY},
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
        results = data.get("organic_results", [])
        return len(results)
    except Exception:
        return 0


def collect():
    if not config.SERPAPI_KEY:
        return {"inserted": 0, "quarantined": 0, "skipped": "serpapi_key_missing"}
    rows = []
    now = datetime.now(timezone.utc).isoformat()
    for sector, queries in config.NARRATIVE_QUERIES.items():
        mentions = sum(_serp_count(q) for q in queries)
        rows.append((now, "serpapi", sector, "social_mentions", float(mentions), "{}", "", 0.6))
    with get_connection() as conn:
        conn.executemany(
            """
            INSERT INTO narrative_events (ts, source, sector, metric, value, payload, source_url, confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return {"inserted": len(rows), "quarantined": 0}
