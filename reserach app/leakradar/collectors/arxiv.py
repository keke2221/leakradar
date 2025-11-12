"""ArXiv RSS collector for AI signals."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from time import sleep

import feedparser

from collectors.base import checksum_payload, persist_rows

FEEDS = [
    "https://export.arxiv.org/rss/cs.AI",
    "https://export.arxiv.org/rss/cs.LG",
    "https://export.arxiv.org/rss/cs.CL",
]

CONFIDENCE = 0.9
PARSE_VERSION = "arxiv_rss_v1"


def _parse_feed(url: str) -> dict:
    return feedparser.parse(url)


def collect():
    rows = []
    for url in FEEDS:
        feed = _parse_feed(url)
        entries = feed.get("entries", [])
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=1)
        recent = []
        for entry in entries:
            published = entry.get("published_parsed")
            if not published:
                continue
            published_dt = datetime(*published[:6], tzinfo=timezone.utc)
            if published_dt >= cutoff:
                recent.append(
                    {
                        "title": entry.get("title"),
                        "link": entry.get("link"),
                        "published": published_dt.isoformat(),
                    }
                )
        payload = {"feed_url": url, "recent_samples": recent[:5], "total_recent": len(recent)}
        rows.append(
            {
                "ts": now.isoformat(),
                "sector": "ai",
                "entity": url,
                "metric": "new_papers",
                "value": float(len(recent)),
                "payload": payload,
                "source_url": url,
                "parse_version": PARSE_VERSION,
                "checksum": checksum_payload(payload),
                "license": "arXiv",
                "confidence": CONFIDENCE,
            }
        )
        sleep(0.5)
    inserted, quarantined = persist_rows("arxiv", rows)
    return {"inserted": inserted, "quarantined": quarantined}
