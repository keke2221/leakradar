"""News / narrative collector."""

from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

import requests

from core import config
from core.db import get_connection


def _perplexity_payload(sector: str) -> Optional[Dict]:
    if not (config.PERPLEXITY_API_KEY and config.USE_PERPLEXITY):
        return None
    prompt = (
        f"Summarize today’s top headlines and developments for the {sector} industry in 5 bullets. "
        "List source URLs. Return JSON with fields: media_hits (int, approximate), "
        "top_topics (array of strings), sources (array of URLs)."
    )
    try:
        resp = requests.post(
            "https://api.perplexity.ai/chat/completions",
            headers={"Authorization": f"Bearer {config.PERPLEXITY_API_KEY}"},
            json={
                "model": "pplx-7b-chat",
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        content = data["choices"][0]["message"]["content"]
        return json.loads(content)
    except Exception:
        return None


def _newsapi_payload(sector: str) -> Optional[Dict]:
    if not config.NEWSAPI_KEY:
        return None
    queries = config.NARRATIVE_QUERIES.get(sector, [sector])
    query = " OR ".join(queries)
    since = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
    params = {
        "q": query,
        "from": since,
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": 100,
    }
    try:
        resp = requests.get(
            "https://newsapi.org/v2/everything",
            params=params,
            headers={"X-Api-Key": config.NEWSAPI_KEY},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        articles = data.get("articles", [])[:5]
        sources = [article.get("url") for article in articles if article.get("url")]
        return {
            "media_hits": min(int(data.get("totalResults", 0) or 0), 100),
            "top_topics": [article.get("title") for article in articles if article.get("title")],
            "sources": sources,
        }
    except Exception:
        return None


def collect():
    rows = []
    now = datetime.now(timezone.utc).isoformat()
    for sector in config.SECTORS:
        payload = _perplexity_payload(sector)
        confidence = 0.8 if payload else 0.0
        source = "perplexity"
        if payload is None:
            payload = _newsapi_payload(sector)
            confidence = 0.7 if payload else 0.0
            source = "newsapi"
        if payload is None:
            continue
        media_hits = payload.get("media_hits")
        sources = payload.get("sources", [])
        if media_hits is None:
            media_hits = max(len(sources), random.randint(5, 15))
        row_payload = json.dumps(payload)
        rows.append(
            (now, source, sector, "media_hits", float(media_hits), row_payload, ",".join(sources), confidence)
        )
    if not rows:
        return {"inserted": 0, "quarantined": 0, "skipped": len(config.SECTORS)}
    with get_connection() as conn:
        conn.executemany(
            """
            INSERT INTO narrative_events (ts, source, sector, metric, value, payload, source_url, confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return {"inserted": len(rows), "quarantined": 0}
