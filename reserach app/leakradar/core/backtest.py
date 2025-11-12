"""Simple backtesting utilities."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import Dict

import pandas as pd

from .config import DATA_DIR


def _load_anomalies(conn) -> pd.DataFrame:
    return pd.read_sql_query("SELECT ts, sector, metric, zscore, verified_status FROM anomalies", conn)


def run_backtest(conn, output_path: Path | None = None) -> Dict[str, float]:
    """Compute persistence + false-spike rate summary."""
    df = _load_anomalies(conn)
    if df.empty:
        summary = {"anomaly_count": 0, "persist_pct": 0.0, "false_spike_rate": 0.0}
    else:
        df["ts"] = pd.to_datetime(df["ts"], utc=True)
        df.sort_values("ts", inplace=True)
        persist = 0
        for i, row in df.iterrows():
            mask = (
                (df["sector"] == row["sector"])
                & (df["metric"] == row["metric"])
                & (df["ts"] > row["ts"])
                & (df["ts"] <= row["ts"] + timedelta(days=7))
            )
            if mask.any():
                persist += 1
        persist_pct = persist / len(df)
        false_mask = df["verified_status"].isin(["noise", "bug"])
        false_spike_rate = false_mask.mean() if len(df) else 0.0
        summary = {
            "anomaly_count": int(len(df)),
            "persist_pct": round(float(persist_pct), 3),
            "false_spike_rate": round(float(false_spike_rate), 3),
        }

    dest = output_path or DATA_DIR / "backtest_summary.csv"
    df = pd.DataFrame([summary])
    try:
        df.to_csv(dest, index=False)
    except PermissionError:
        df.to_csv(dest.with_suffix(".tmp"), index=False)
    return summary
