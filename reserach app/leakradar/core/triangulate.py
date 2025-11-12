"""Triangulation utilities across sources."""

from __future__ import annotations

import pandas as pd

from .config import TRIANGULATION_MIN_SOURCES

EPS = 1e-6


def _trimmed_mean(values: pd.Series) -> float:
    if len(values) < 3:
        return float(values.mean())
    k = max(1, int(len(values) * 0.1))
    trimmed = values.sort_values().iloc[k:-k] if len(values) - 2 * k > 0 else values
    return float(trimmed.mean())


def compute_consensus(events: pd.DataFrame) -> pd.DataFrame:
    """Return consensus value per day/sector/metric with disagreement."""
    if events.empty:
        return pd.DataFrame(
            columns=["ts", "sector", "metric", "consensus_value", "disagreement", "source_count"]
        )

    events = events.copy()
    events["ts"] = pd.to_datetime(events["ts"], utc=True).dt.floor("D")

    grouped = (
        events.groupby(["ts", "sector", "metric", "source"])
        .agg({"value": "mean"})
        .reset_index()
    )

    records = []
    for (ts, sector, metric), metric_df in grouped.groupby(["ts", "sector", "metric"]):
        values = metric_df["value"]
        source_count = len(values)
        if source_count < TRIANGULATION_MIN_SOURCES:
            continue
        consensus_value = _trimmed_mean(values)
        mean_val = values.mean() or EPS
        disagreement = max(values.max() - values.min(), 0.0) / (abs(mean_val) + EPS)
        disagreement = max(0.0, min(1.0, disagreement))
        records.append(
            {
                "ts": ts,
                "sector": sector,
                "metric": metric,
                "consensus_value": consensus_value,
                "disagreement": disagreement,
                "source_count": source_count,
            }
        )

    return pd.DataFrame.from_records(records)


def disagreement_by_sector(consensus_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate disagreement per day+sector."""
    if consensus_df.empty:
        return pd.DataFrame(columns=["ts", "sector", "consensus_disagreement"])
    agg = (
        consensus_df.groupby(["ts", "sector"])["disagreement"]
        .mean()
        .reset_index()
        .rename(columns={"disagreement": "consensus_disagreement"})
    )
    return agg
