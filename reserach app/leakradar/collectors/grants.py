"""Grant collector using sample JSON (swap with USAspending/NIH when ready)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from collectors.base import checksum_payload, persist_rows
from core.config import BASE_DIR

SAMPLE_PATH = BASE_DIR / "tracked" / "samples" / "grants.json"
PARSE_VERSION = "grants_v1"


def _load_samples():
    if SAMPLE_PATH.exists():
        return json.loads(SAMPLE_PATH.read_text(encoding="utf-8-sig"))
    return []


def collect():
    rows = []
    now = datetime.now(timezone.utc).isoformat()
    for grant in _load_samples():
        payload = grant
        rows.append(
            {
                "ts": now,
                "sector": grant.get("sector", "ai"),
                "entity": grant.get("program"),
                "metric": "grants",
                "value": float(grant.get("amount", 0)),
                "payload": payload,
                "source_url": grant.get("url"),
                "parse_version": PARSE_VERSION,
                "checksum": checksum_payload(payload),
                "license": grant.get("license"),
                "confidence": 0.6,
            }
        )
    inserted, quarantined = persist_rows("grants", rows)
    return {"inserted": inserted, "quarantined": quarantined}
