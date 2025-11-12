"""Market data collector."""

from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import requests
import yfinance as yf

from core import config
from core.db import get_connection


def _load_tickers() -> List[Dict[str, str]]:
    path = Path(config.TICKER_DEFAULTS)
    if not path.exists():
        return []
    with path.open() as fh:
        reader = csv.DictReader(fh)
        return [row for row in reader if row.get("symbol")]


def _fetch_alphavantage(symbol: str) -> List[Dict]:
    params = {
        "function": "TIME_SERIES_DAILY_ADJUSTED",
        "symbol": symbol,
        "apikey": config.ALPHAVANTAGE_KEY,
    }
    try:
        resp = requests.get("https://www.alphavantage.co/query", params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json().get("Time Series (Daily)", {})
        history = []
        for date_str, values in data.items():
            history.append(
                {
                    "date": datetime.fromisoformat(date_str),
                    "close": float(values.get("4. close", 0.0)),
                    "volume": float(values.get("6. volume", 0.0)),
                }
            )
        return sorted(history, key=lambda x: x["date"], reverse=True)
    except Exception:
        return []


def _fetch_yfinance(symbol: str) -> List[Dict]:
    try:
        hist = yf.Ticker(symbol).history(period="15d", interval="1d")
        if hist.empty:
            return []
        history = []
        for idx, row in hist.iterrows():
            history.append(
                {
                    "date": idx.to_pydatetime(),
                    "close": float(row["Close"]),
                    "volume": float(row.get("Volume", 0.0)),
                }
            )
        return sorted(history, key=lambda x: x["date"], reverse=True)
    except Exception:
        return []


def _compute_metrics(history: List[Dict]) -> Dict[str, float]:
    if not history:
        return {"price_change_7d": 0.0, "volume_7d": 0.0}
    latest = history[0]
    lookback_index = min(6, len(history) - 1)
    base = history[lookback_index]["close"] or 1e-6
    price_change = ((latest["close"] - base) / base) * 100.0
    volume = sum(item["volume"] for item in history[:7])
    return {"price_change_7d": price_change, "volume_7d": volume}


def collect():
    tickers = _load_tickers()
    if not tickers:
        return {"inserted": 0, "quarantined": 0, "skipped": "no_tickers"}
    rows = []
    now = datetime.now(timezone.utc).isoformat()
    use_alpha = bool(config.ALPHAVANTAGE_KEY) and not config.USE_YFINANCE
    for entry in tickers:
        symbol = entry["symbol"]
        history = _fetch_alphavantage(symbol) if use_alpha else _fetch_yfinance(symbol)
        metrics = _compute_metrics(history)
        confidence = 0.9 if use_alpha else 0.8
        for metric, value in metrics.items():
            rows.append(
                (
                    now,
                    entry.get("sector", "ai"),
                    symbol,
                    entry.get("kind", "ticker"),
                    metric,
                    float(value),
                    json.dumps(history[:7], default=str) if history else "{}",
                    confidence,
                )
            )
    if not rows:
        return {"inserted": 0, "quarantined": 0}
    with get_connection() as conn:
        conn.executemany(
            """
            INSERT INTO market_events (ts, sector, symbol, kind, metric, value, payload, confidence)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    return {"inserted": len(rows), "quarantined": 0}
