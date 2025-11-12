"""Feature aggregation, scoring, triangulation pipeline."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict

import pandas as pd

from core import config
from core import db
from core.scoring import compute_scores
from core.triangulate import compute_consensus, disagreement_by_sector


def _load_events(conn) -> pd.DataFrame:
    query = "SELECT ts, source, sector, entity, metric, value, confidence FROM events"
    df = pd.read_sql_query(query, conn)
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"], utc=True)
    return df


def build_features(events_df: pd.DataFrame) -> pd.DataFrame:
    if events_df.empty:
        end = datetime.now(timezone.utc)
        dates = pd.date_range(end=end, periods=config.Z_SCORE_WINDOW_DAYS, freq="D")
        idx = pd.MultiIndex.from_product([dates, config.SECTORS], names=["ts", "sector"])
        frame = pd.DataFrame(index=idx)
        for col in [
            "new_papers_7d",
            "new_papers_30d",
            "recruiting_trials_30d",
            "jobs_keyword_count",
            "github_stars_30d",
            "grants_90d",
            "consensus_disagreement",
            "confidence_mean",
        ]:
            frame[col] = 0.0
        return frame.reset_index()

    end = events_df["ts"].max()
    start = end - timedelta(days=config.Z_SCORE_WINDOW_DAYS - 1)
    dates = pd.date_range(start=start, end=end, freq="D")
    idx = pd.MultiIndex.from_product([dates, config.SECTORS], names=["ts", "sector"])
    frame = pd.DataFrame(index=idx)
    frame = frame.assign(
        new_papers_7d=0.0,
        new_papers_30d=0.0,
        recruiting_trials_30d=0.0,
        jobs_keyword_count=0.0,
        github_stars_30d=0.0,
        grants_90d=0.0,
        consensus_disagreement=0.0,
        confidence_mean=0.0,
    )

    daily = (
        events_df.groupby(["ts", "sector", "metric"])
        .agg(value=("value", "sum"), confidence=("confidence", "mean"))
        .reset_index()
    )

    # Confidence per day/sector
    conf = (
        daily.groupby(["ts", "sector"])["confidence"]
        .mean()
        .reset_index()
        .rename(columns={"confidence": "confidence_mean"})
    )
    conf_series = conf.set_index(["ts", "sector"])["confidence_mean"].reindex(idx, fill_value=0.0)
    frame["confidence_mean"] = conf_series.values

    def rolling_metric(metric_name: str, window: int, out_col: str):
        subset = daily[daily["metric"] == metric_name]
        if subset.empty:
            return
        pivot = subset.pivot_table(
            index="ts", columns="sector", values="value", aggfunc="sum"
        ).reindex(dates, fill_value=0.0)
        rolled = pivot.rolling(window=window, min_periods=1).sum()
        frame[out_col] = rolled.stack().reindex(idx, fill_value=0.0).values

    rolling_metric("new_papers", 7, "new_papers_7d")
    rolling_metric("new_papers", 30, "new_papers_30d")
    rolling_metric("recruiting_trials", 30, "recruiting_trials_30d")
    rolling_metric("grants", 90, "grants_90d")

    # Jobs daily count
    jobs = daily[daily["metric"] == "job_count"]
    if not jobs.empty:
        jobs_pivot = jobs.pivot_table(
            index="ts", columns="sector", values="value", aggfunc="sum"
        ).reindex(dates, fill_value=0.0)
        frame["jobs_keyword_count"] = jobs_pivot.stack().reindex(idx, fill_value=0.0).values

    # GitHub stars delta 30d
    stars = events_df[events_df["metric"] == "stars"]
    if not stars.empty:
        stars_daily = stars.groupby(["ts", "sector"])["value"].mean().reset_index()
        stars_pivot = stars_daily.pivot_table(
            index="ts", columns="sector", values="value", aggfunc="mean"
        ).reindex(dates, fill_value=0.0)
        delta = stars_pivot.diff(periods=30).fillna(0.0)
        frame["github_stars_30d"] = delta.stack().reindex(idx, fill_value=0.0).values

    # Triangulation disagreement
    consensus = compute_consensus(events_df)
    disagreement = disagreement_by_sector(consensus)
    disagreement = disagreement.set_index(["ts", "sector"]).reindex(idx, fill_value=0.0)
    frame["consensus_disagreement"] = disagreement["consensus_disagreement"].values

    return frame.reset_index()


def persist_features(conn, features_df: pd.DataFrame) -> None:
    conn.execute("DELETE FROM features")
    rows = [
        (
            row["ts"].isoformat(),
            row["sector"],
            float(row["new_papers_7d"]),
            float(row["new_papers_30d"]),
            float(row["recruiting_trials_30d"]),
            float(row["jobs_keyword_count"]),
            float(row["github_stars_30d"]),
            float(row["grants_90d"]),
            float(row["consensus_disagreement"]),
        )
        for _, row in features_df.iterrows()
    ]
    conn.executemany(
        """
        INSERT INTO features (
            ts, sector, new_papers_7d, new_papers_30d, recruiting_trials_30d,
            jobs_keyword_count, github_stars_30d, grants_90d, consensus_disagreement
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


def persist_scores(conn, scores_df: pd.DataFrame) -> None:
    conn.execute("DELETE FROM scores")
    rows = [
        (
            row["ts"].isoformat(),
            row["sector"],
            float(row["score"]),
            row["components"],
            float(row["mean_confidence"]),
        )
        for _, row in scores_df.iterrows()
    ]
    conn.executemany(
        "INSERT INTO scores (ts, sector, score, components, mean_confidence) VALUES (?, ?, ?, ?, ?)",
        rows,
    )


def run_compute() -> Dict[str, int]:
    with db.get_connection() as conn:
        events_df = _load_events(conn)
        features = build_features(events_df)
        persist_features(conn, features)
        scores = compute_scores(features)
        persist_scores(conn, scores)
        return {"features": len(features), "scores": len(scores)}
