"""Score computation helpers."""

from __future__ import annotations

import json
from typing import Dict

import numpy as np
import pandas as pd

from .config import METRIC_WEIGHTS


def _safe_z(values: pd.Series) -> pd.Series:
    mean = values.mean()
    std = values.std(ddof=0)
    if std == 0 or np.isnan(std):
        return pd.Series(0.0, index=values.index)
    return (values - mean) / (std or 1.0)


def compute_scores(features: pd.DataFrame) -> pd.DataFrame:
    """Return per-day sector scores with component z-scores and mean confidence."""
    metric_cols = list(METRIC_WEIGHTS.keys())
    out_rows = []
    for (sector), sector_df in features.groupby("sector"):
        sector_df = sector_df.sort_values("ts")
        z_df = pd.DataFrame(index=sector_df.index)
        for metric in metric_cols:
            z_df[metric] = _safe_z(sector_df[metric].fillna(0.0))
        weighted = z_df.mul(pd.Series(METRIC_WEIGHTS))
        sector_df = sector_df.copy()
        sector_df["score"] = weighted.sum(axis=1)
        sector_df["components"] = z_df.apply(
            lambda row: json.dumps({metric: row[metric] for metric in metric_cols}),
            axis=1,
        )
        out_rows.append(sector_df)

    result = pd.concat(out_rows).sort_values(["ts", "sector"]).reset_index(drop=True)
    result.rename(columns={"confidence_mean": "mean_confidence"}, inplace=True)
    return result[["ts", "sector", "score", "components", "mean_confidence"]]
