"""Market signal helpers."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, List

import pandas as pd

from . import config
from .db import get_connection


def _read_market(conn) -> pd.DataFrame:
    df = pd.read_sql_query(
        "SELECT ts, sector, symbol, kind, metric, value, payload, confidence FROM market_events",
        conn,
    )
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df


def _load_market_df(conn=None) -> pd.DataFrame:
    if conn is not None:
        return _read_market(conn)
    with get_connection() as conn_obj:
        return _read_market(conn_obj)


def sector_pulse(window_days: int = 10) -> Dict[str, Dict[str, float]]:
    df = _load_market_df()
    if df.empty:
        return {}
    cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
    df = df[df["ts"] >= cutoff]
    if df.empty:
        return {}
    result: Dict[str, Dict[str, float]] = {}
    for sector, sdf in df.groupby("sector"):
        medians = sdf.groupby("metric")["value"].median().to_dict()
        result[sector] = {
            "price_change_7d": float(medians.get("price_change_7d", 0.0)),
            "volume_7d": float(medians.get("volume_7d", 0.0)),
        }
    return result


def top_movers(limit: int = 3) -> Dict[str, List[Dict[str, float]]]:
    df = _load_market_df()
    if df.empty:
        return {}
    latest_ts = df["ts"].max()
    latest = df[(df["ts"] == latest_ts) & (df["metric"] == "price_change_7d")]
    movers: Dict[str, List[Dict[str, float]]] = {}
    for sector, sdf in latest.groupby("sector"):
        sorted_df = sdf.reindex(sdf["value"].abs().sort_values(ascending=False).index)
        movers[sector] = [
            {"symbol": row["symbol"], "value": float(row["value"])}
            for _, row in sorted_df.head(limit).iterrows()
        ]
    return movers
