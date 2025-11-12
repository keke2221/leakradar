"""Shared helpers for collectors."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Dict, Iterable, Tuple

from core import db
from core.validate import validate_event


def checksum_payload(payload: Dict) -> str:
    blob = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha1(blob).hexdigest()


def persist_rows(source: str, rows: Iterable[Dict]) -> Tuple[int, int]:
    inserted = 0
    quarantined = 0
    now = datetime.now(timezone.utc).isoformat()
    with db.get_connection() as conn:
        for row in rows:
            row = dict(row)
            row.setdefault("source", source)
            row.setdefault("fetched_at", now)
            if isinstance(row.get("payload"), (dict, list)):
                row["payload"] = json.dumps(row["payload"])
            ok, error = validate_event(row)
            target = "events" if ok else "events_quarantine"
            if not ok:
                row["error"] = error
                quarantined += 1
            else:
                inserted += 1
            columns = [
                "ts",
                "source",
                "sector",
                "entity",
                "metric",
                "value",
                "payload",
                "source_url",
                "fetched_at",
                "parse_version",
                "checksum",
                "license",
                "confidence",
            ]
            params = [row.get(col) for col in columns]
            if target == "events":
                conn.execute(
                    """
                    INSERT INTO events (ts, source, sector, entity, metric, value, payload,
                    source_url, fetched_at, parse_version, checksum, license, confidence)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    params,
                )
            else:
                conn.execute(
                    """
                    INSERT INTO events_quarantine (ts, source, sector, entity, metric, value, payload,
                    source_url, fetched_at, parse_version, checksum, license, confidence, error)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    params + [row.get("error")],
                )
    return inserted, quarantined
