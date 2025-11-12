"""Monitoring utilities for collectors and anomalies."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Sequence

import pandas as pd

from .config import SEVERE_Z, SOURCE_SILENCE_HOURS


@dataclass
class CollectorStatus:
    source: str
    last_seen: datetime | None
    stale: bool


def collector_health(conn) -> List[CollectorStatus]:
    cur = conn.execute(
        "SELECT source, MAX(fetched_at) as fetched_at FROM events GROUP BY source"
    )
    rows = cur.fetchall()
    now = datetime.now(timezone.utc)
    statuses: List[CollectorStatus] = []
    for row in rows:
        fetched_at = row["fetched_at"]
        last_seen = None
        if fetched_at:
            last_seen = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
        stale = True
        if last_seen:
            stale = now - last_seen > timedelta(hours=SOURCE_SILENCE_HOURS)
        statuses.append(CollectorStatus(source=row["source"], last_seen=last_seen, stale=stale))
    return statuses


def summarize_collector_health(statuses: Sequence[CollectorStatus]) -> str:
    parts = []
    for status in statuses:
        ts = status.last_seen.isoformat() if status.last_seen else "never"
        flag = "STALE" if status.stale else "OK"
        parts.append(f"{status.source}:{flag}({ts})")
    return ", ".join(parts)


def severe_spike_budget(anomalies_df: pd.DataFrame) -> bool:
    """Return True if spike budget exceeded (>=5 severe anomalies per day)."""
    if anomalies_df.empty:
        return False
    severe = anomalies_df[anomalies_df["zscore"].abs() >= SEVERE_Z]
    if severe.empty:
        return False
    counts = severe.groupby(severe["ts"].dt.date).size()
    return counts.max() >= 5
