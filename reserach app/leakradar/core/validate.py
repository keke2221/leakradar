"""Schema validation for ingested events."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

ALLOWED_METRICS = {
    "new_papers",
    "recruiting_trials",
    "job_count",
    "stars",
    "releases",
    "grants",
}


def _parse_ts(value: str) -> Optional[datetime]:
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value)
    except Exception:
        return None


def validate_event(row: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Return (ok, error) for an event row."""
    ts_str = row.get("ts")
    if not ts_str:
        return False, "missing ts"
    ts = _parse_ts(ts_str)
    if not ts:
        return False, "invalid ts"
    if ts < datetime.now(timezone.utc) - timedelta(days=365):
        return False, "ts too old"

    metric = row.get("metric")
    if metric not in ALLOWED_METRICS:
        return False, f"invalid metric {metric}"

    value = row.get("value")
    if value is None or value < 0:
        return False, "value negative"

    confidence = row.get("confidence")
    if confidence is not None and not (0 <= confidence <= 1):
        return False, "confidence out of range"

    http_status = row.get("http_status")
    if http_status is not None and http_status != 200:
        return False, f"http {http_status}"

    return True, None
