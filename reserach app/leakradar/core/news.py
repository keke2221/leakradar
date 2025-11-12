"""Narrative signal helpers."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional

import pandas as pd

from . import config
from .db import get_connection


def _read_narratives(conn) -> pd.DataFrame:
    df = pd.read_sql_query(
        "SELECT ts, source, sector, metric, value, payload, source_url, confidence FROM narrative_events",
        conn,
    )
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df


def _load_events_df(conn=None) -> pd.DataFrame:
    if conn is not None:
        return _read_narratives(conn)
    with get_connection() as conn_obj:
        return _read_narratives(conn_obj)


def media_density(window_days: int = 30) -> pd.DataFrame:
    """Return per-sector media hit z-scores over the given window."""
    df = _load_events_df()
    if df.empty:
        return pd.DataFrame(columns=["ts", "sector", "media_hits", "media_z"])
    df = df[df["metric"] == "media_hits"].copy()
    df["ts"] = df["ts"].dt.floor("D")
    cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
    df = df[df["ts"] >= cutoff]
    grouped = df.groupby(["ts", "sector"])["value"].sum().reset_index(name="media_hits")
    frames = []
    for sector, sdf in grouped.groupby("sector"):
        mean = sdf["media_hits"].mean()
        std = sdf["media_hits"].std(ddof=0) or 0.0
        if std == 0:
            sdf["media_z"] = 0.0
        else:
            sdf["media_z"] = (sdf["media_hits"] - mean) / std
        frames.append(sdf.assign(sector=sector))
    return pd.concat(frames) if frames else pd.DataFrame(columns=["ts", "sector", "media_hits", "media_z"])


def social_pulse(window_days: int = 30) -> pd.DataFrame:
    df = _load_events_df()
    if df.empty:
        return pd.DataFrame(columns=["ts", "sector", "social_mentions", "social_z"])
    df = df[df["metric"] == "social_mentions"].copy()
    if df.empty:
        return pd.DataFrame(columns=["ts", "sector", "social_mentions", "social_z"])
    df["ts"] = df["ts"].dt.floor("D")
    cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
    df = df[df["ts"] >= cutoff]
    grouped = df.groupby(["ts", "sector"])["value"].sum().reset_index(name="social_mentions")
    frames = []
    for sector, sdf in grouped.groupby("sector"):
        mean = sdf["social_mentions"].mean()
        std = sdf["social_mentions"].std(ddof=0) or 0.0
        if std == 0:
            sdf["social_z"] = 0.0
        else:
            sdf["social_z"] = (sdf["social_mentions"] - mean) / std
        frames.append(sdf.assign(sector=sector))
    return pd.concat(frames) if frames else pd.DataFrame(columns=["ts", "sector", "social_mentions", "social_z"])


def latest_topics() -> Dict[str, Dict[str, list]]:
    """Return latest payload topics and sources per sector."""
    df = _load_events_df()
    if df.empty:
        return {}
    df = df.sort_values("ts", ascending=False)
    result: Dict[str, Dict[str, list]] = {}
    for sector in config.SECTORS:
        sector_rows = df[df["sector"] == sector]
        if sector_rows.empty:
            continue
        payload_raw = sector_rows.iloc[0]["payload"]
        try:
            payload = json.loads(payload_raw) if payload_raw else {}
        except json.JSONDecodeError:
            payload = {}
        result[sector] = {
            "top_topics": payload.get("top_topics", []),
            "sources": payload.get("sources", []),
        }
    return result
