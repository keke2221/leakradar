"""Job board collector for tracked careers."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

from collectors.base import checksum_payload, persist_rows
from core.config import BASE_DIR

CAREERS_PATH = BASE_DIR / "tracked" / "careers.json"
PARSE_VERSION = "jobs_v1"


def _load_careers():
    if not CAREERS_PATH.exists():
        return []
    return json.loads(CAREERS_PATH.read_text(encoding="utf-8-sig"))


def _confidence(kind: str) -> float:
    if kind == "html":
        return 0.6
    return 0.8


def _fetch(url: str):
    try:
        resp = requests.get(url, timeout=20)
        return resp.status_code, resp.text
    except Exception as exc:
        return None, str(exc)


def _count_keywords(html: str, keywords):
    text = html.lower()
    return sum(text.count(k.lower()) for k in keywords)


def collect():
    rows = []
    for entry in _load_careers():
        url = entry["url"]
        status, body = _fetch(url)
        now = datetime.now(timezone.utc).isoformat()
        if status != 200:
            payload = {"error": body, "discovered_postings": []}
            value = 0.0
        else:
            soup = BeautifulSoup(body, "lxml")
            titles = [el.get_text(strip=True) for el in soup.find_all(["h1", "h2", "a"])][:5]
            value = float(_count_keywords(body, entry.get("keywords", [])))
            payload = {
                "titles": titles,
                "keyword_hits": value,
                "dom_checksum": hashlib.sha1(body.encode("utf-8")).hexdigest(),
                "discovered_postings": titles,
            }
        rows.append(
            {
                "ts": now,
                "sector": entry["sector"],
                "entity": url,
                "metric": "job_count",
                "value": value,
                "payload": payload,
                "source_url": url,
                "parse_version": PARSE_VERSION,
                "checksum": checksum_payload(payload),
                "license": "N/A",
                "confidence": _confidence(entry.get("type", "html")),
                "http_status": status,
            }
        )
    inserted, quarantined = persist_rows("jobs", rows)
    return {"inserted": inserted, "quarantined": quarantined}
