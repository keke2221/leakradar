"""Hype vs Reality comparator."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List

import pandas as pd

from . import config
from .db import get_connection
from .news import media_density, social_pulse

REALITY_METRIC_MAP = {
    "jobs": "jobs_keyword_count",
    "github": "github_stars_30d",
    "papers": "new_papers_7d",
    "grants": "grants_90d",
}


def _normalize_to_scale(z_value: float) -> float:
    """Map z-score to 0-100 scale."""
    return max(0.0, min(100.0, 50.0 + z_value * 15.0))


def _weighted_score(values: Dict[str, float], weights: Dict[str, float]) -> float:
    total = 0.0
    weight_sum = 0.0
    for key, weight in weights.items():
        if key in values:
            total += values[key] * weight
            weight_sum += weight
    return total / weight_sum if weight_sum else 0.0


def _metric_zscores(df: pd.DataFrame, column: str) -> Dict[str, float]:
    values = {}
    for sector, sdf in df.groupby("sector"):
        series = sdf[column].astype(float)
        std = series.std(ddof=0)
        if std == 0 or pd.isna(std):
            z = 0.0
        else:
            z = (series.iloc[-1] - series.mean()) / std
        values[sector] = z
    return values


def _reality_components(features_df: pd.DataFrame) -> Dict[str, Dict[str, float]]:
    components = {sector: {} for sector in config.SECTORS}
    if features_df.empty:
        return components
    for reality_key, column in REALITY_METRIC_MAP.items():
        zscores = _metric_zscores(features_df, column)
        for sector, z in zscores.items():
            components.setdefault(sector, {})[reality_key] = z
    return components


@dataclass
class CompareRow:
    ts: datetime
    sector: str
    hype_index: float
    reality_index: float
    gap: float


def build_indices() -> List[CompareRow]:
    media_df = media_density()
    social_df = social_pulse()
    with get_connection() as conn:
        features = pd.read_sql_query("SELECT * FROM features", conn)
    if features.empty:
        return []
    features["ts"] = pd.to_datetime(features["ts"])
    latest_ts = features["ts"].max()
    features = features.sort_values("ts")
    reality_components = _reality_components(features)

    latest_media = (
        media_df.sort_values("ts").groupby("sector").tail(1).set_index("sector")
        if not media_df.empty
        else None
    )
    latest_social = (
        social_df.sort_values("ts").groupby("sector").tail(1).set_index("sector")
        if not social_df.empty
        else None
    )
    media_lookup = latest_media["media_z"].to_dict() if latest_media is not None else {}
    social_lookup = latest_social["social_z"].to_dict() if latest_social is not None else {}

    rows: List[CompareRow] = []
    insert_rows = []
    ts_iso = latest_ts.isoformat()
    for sector in config.SECTORS:
        hype_inputs = {}
        media_z = media_lookup.get(sector)
        if media_z is not None:
            hype_inputs["media_density"] = media_z
        social_z = social_lookup.get(sector)
        if social_z is not None:
            hype_inputs["social_pulse"] = social_z
        hype_score = _normalize_to_scale(_weighted_score(hype_inputs, config.HYPE_WEIGHTS))

        reality_inputs = reality_components.get(sector, {})
        reality_score = _normalize_to_scale(_weighted_score(reality_inputs, config.REALITY_WEIGHTS))
        gap = hype_score - reality_score
        row = CompareRow(latest_ts, sector, hype_score, reality_score, gap)
        rows.append(row)
        insert_rows.append((ts_iso, sector, hype_score, reality_score, gap))

    with get_connection() as conn:
        conn.execute("DELETE FROM comparisons WHERE ts = ?", (ts_iso,))
        conn.executemany(
            "INSERT INTO comparisons (ts, sector, hype_index, reality_index, gap) VALUES (?, ?, ?, ?, ?)",
            insert_rows,
        )
    return rows
