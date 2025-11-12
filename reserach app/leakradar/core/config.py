"""Central configuration for LeakSearcher."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = os.getenv("DB_PATH", str(DATA_DIR / "leakradar.sqlite"))
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
NEWSAPI_KEY = os.getenv("NEWSAPI_KEY")
PERPLEXITY_API_KEY = os.getenv("PERPLEXITY_API_KEY")
SERPAPI_KEY = os.getenv("SERPAPI_KEY")
ALPHAVANTAGE_KEY = os.getenv("ALPHAVANTAGE_KEY")


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


USE_PERPLEXITY = _env_bool("USE_PERPLEXITY", True)
USE_YFINANCE = _env_bool("USE_YFINANCE", True)

SECTORS: List[str] = ["ai", "biotech", "climate", "creator"]
METRIC_WEIGHTS: Dict[str, float] = {
    "new_papers_7d": 0.25,
    "recruiting_trials_30d": 0.25,
    "jobs_keyword_count": 0.20,
    "github_stars_30d": 0.20,
    "grants_90d": 0.10,
}

Z_SCORE_WINDOW_DAYS = 90
ALERT_SCORE = 2.0
ANOMALY_Z = 2.0
SEVERE_Z = 3.0
TRIANGULATION_MIN_SOURCES = 2
SOURCE_SILENCE_HOURS = 36

NARRATIVE_QUERIES = {
    "ai": ["AI", "GPU", "LLM"],
    "biotech": ["clinical trial", "oncology"],
    "climate": ["carbon", "grid", "renewable"],
    "creator": ["creator economy", "UGC", "OnlyFans"],
}

TICKER_DEFAULTS = BASE_DIR / "tracked" / "tickers.csv"
NEWS_SOURCES_PATH = BASE_DIR / "tracked" / "news_sources.json"

HYPE_WEIGHTS = {"media_density": 0.6, "social_pulse": 0.4}
REALITY_WEIGHTS = {"jobs": 0.35, "github": 0.25, "papers": 0.25, "grants": 0.15}


def hash_config() -> str:
    """Return a stable hash of critical runtime config for run tracking."""
    payload = {
        "sectors": SECTORS,
        "weights": METRIC_WEIGHTS,
        "z_window": Z_SCORE_WINDOW_DAYS,
        "alert_score": ALERT_SCORE,
        "anomaly_z": ANOMALY_Z,
        "severe_z": SEVERE_Z,
        "triangulation_min": TRIANGULATION_MIN_SOURCES,
        "source_silence_hours": SOURCE_SILENCE_HOURS,
        "use_perplexity": USE_PERPLEXITY,
        "use_yfinance": USE_YFINANCE,
        "narrative_queries": NARRATIVE_QUERIES,
    }
    blob = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha1(blob).hexdigest()


@dataclass
class RateLimitPolicy:
    polite_sleep_secs: float = 1.0
    retries: int = 3


POLICY = RateLimitPolicy()
