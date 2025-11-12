"""Streamlit UI for LeakSearcher."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone

import pandas as pd
import streamlit as st

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from app.tabs import brief as brief_tab
from app.tabs import markets as markets_tab
from app.tabs import narrative as narrative_tab
from core import config

st.set_page_config(page_title="LeakSearcher", layout="wide")


def _connect():
    return sqlite3.connect(config.DB_PATH)


@st.cache_data(ttl=60)
def load_scores():
    with _connect() as conn:
        df = pd.read_sql_query("SELECT * FROM scores", conn)
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"])
    return df


@st.cache_data(ttl=60)
def load_features():
    with _connect() as conn:
        df = pd.read_sql_query("SELECT * FROM features", conn)
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"])
    return df


@st.cache_data(ttl=30)
def load_events(limit: int = 500):
    with _connect() as conn:
        query = f"SELECT * FROM events ORDER BY ts DESC LIMIT {limit}"
        df = pd.read_sql_query(query, conn)
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"])
    return df


@st.cache_data(ttl=30)
def load_anomalies():
    with _connect() as conn:
        df = pd.read_sql_query("SELECT rowid as id, * FROM anomalies ORDER BY ts DESC", conn)
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"])
    return df


@st.cache_data(ttl=60)
def load_comparisons():
    with _connect() as conn:
        df = pd.read_sql_query("SELECT * FROM comparisons", conn)
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"])
    return df


def _confidence_chip(value: float) -> str:
    if value >= 0.75:
        return f"High ({value:.2f})"
    if value >= 0.5:
        return f"Med ({value:.2f})"
    return f"Low ({value:.2f})"


def _coverage(df_events: pd.DataFrame) -> pd.DataFrame:
    if df_events.empty:
        return pd.DataFrame(columns=["sector", "coverage"])
    cutoff = datetime.now(timezone.utc) - timedelta(hours=48)
    recent = df_events[df_events["ts"] >= cutoff]
    denom = df_events.groupby("sector")["source"].nunique().replace(0, 1)
    numer = recent.groupby("sector")["source"].nunique()
    coverage = (numer / denom).fillna(0.0).rename("coverage")
    return coverage.reset_index()


def _update_anomaly(row_id: int, status: str):
    with _connect() as conn:
        conn.execute(
            "UPDATE anomalies SET verified_status = ? WHERE rowid = ?",
            (status, row_id),
        )
        conn.commit()
    load_anomalies.clear()


def _add_note(sector: str, text: str):
    if not text.strip():
        return
    with _connect() as conn:
        conn.execute(
            "INSERT INTO notes (ts, sector, text) VALUES (?, ?, ?)",
            (datetime.now(timezone.utc).isoformat(), sector, text.strip()),
        )
        conn.commit()


scores = load_scores()
features = load_features()
events = load_events()
anomalies = load_anomalies()
comparisons = load_comparisons()

st.title("LeakSearcher Dashboard")
st.caption("Tracking AI / Biotech / Climate / Creator economy signals with provenance and confidence.")

if not comparisons.empty:
    latest = comparisons[comparisons["ts"] == comparisons["ts"].max()]
    cols = st.columns(len(config.SECTORS))
    for idx, sector in enumerate(config.SECTORS):
        sector_row = latest[latest["sector"] == sector]
        if sector_row.empty:
            continue
        gap = sector_row.iloc[0]["gap"]
        color = "green" if gap < 0 else "red"
        cols[idx].markdown(f"**{sector.title()}**  \n<span style='color:{color};'>Gap: {gap:+.1f}</span>", unsafe_allow_html=True)

tabs = st.tabs(
    [
        "Leaderboard",
        "Leak Feed",
        "Narrative",
        "Markets",
        "Sector Detail",
        "Coverage",
        "Founder Briefs",
    ]
)

# Leaderboard tab
with tabs[0]:
    st.subheader("Sector Leaderboard")
    if scores.empty:
        st.info("No scores yet. Run `python run_all.py`.")
    else:
        latest_ts = scores["ts"].max()
        latest_scores = scores[scores["ts"] == latest_ts].copy()
        baseline = (
            scores[scores["ts"] >= latest_ts - timedelta(days=30)]
            .groupby("sector")["score"]
            .mean()
            .reset_index()
            .rename(columns={"score": "score_mean_30d"})
        )
        latest_scores = latest_scores.merge(baseline, on="sector", how="left")
        latest_scores["delta_vs_30d"] = latest_scores["score"] - latest_scores["score_mean_30d"]
        coverage_df = _coverage(events)
        latest_scores = latest_scores.merge(coverage_df, on="sector", how="left")
        if "coverage" not in latest_scores.columns:
            latest_scores["coverage"] = 0.0
        latest_scores["coverage"] = latest_scores["coverage"].fillna(0.0)
        latest_scores["coverage_status"] = latest_scores["coverage"].apply(
            lambda x: "Low" if x < 0.7 else "OK"
        )
        latest_scores["confidence_chip"] = latest_scores["mean_confidence"].apply(_confidence_chip)
        latest_scores["disagreement_pct"] = (
            features[features["ts"] == latest_ts]
            .set_index("sector")["consensus_disagreement"]
            .reindex(latest_scores["sector"])
            .fillna(0.0)
            .values
        )
        leaderboard_cols = latest_scores[
            [
                "sector",
                "score",
                "delta_vs_30d",
                "confidence_chip",
                "coverage",
                "coverage_status",
                "disagreement_pct",
            ]
        ].rename(
            columns={
                "delta_vs_30d": "? vs 30d mean",
                "confidence_chip": "confidence",
                "disagreement_pct": "disagreement",
            }
        )
        st.dataframe(
            leaderboard_cols.style.bar(subset=["score"], color="#00a5cf").background_gradient(
                subset=["coverage"], cmap="Reds_r"
            ),
            use_container_width=True,
        )
        st.bar_chart(latest_scores.set_index("sector")["score"])

# Leak Feed tab
with tabs[1]:
    st.subheader("Leak Feed")
    if anomalies.empty:
        st.success("No anomalies breaching thresholds.")
    else:
        for _, row in anomalies.iterrows():
            if abs(row["zscore"]) < config.ANOMALY_Z:
                continue
            cols = st.columns([2, 2, 2, 2, 1, 1])
            cols[0].markdown(f"**{row['sector']} · {row['metric']}**")
            cols[1].markdown(f"z-score: `{row['zscore']:.2f}`")
            cols[2].markdown(_confidence_chip(row["confidence"]))
            disagreement = (
                features[
                    (features["sector"] == row["sector"]) & (features["ts"] == row["ts"])
                ]["consensus_disagreement"]
                .mean()
            )
            cols[3].markdown(f"Disagreement: {disagreement:.0%}" if not pd.isna(disagreement) else "Disagreement: n/a")
            if cols[4].button("Confirm", key=f"confirm_{row['id']}"):
                _update_anomaly(row["id"], "confirm")
            if cols[5].button("Noise", key=f"noise_{row['id']}"):
                _update_anomaly(row["id"], "noise")
            st.divider()

# Narrative tab
with tabs[2]:
    narrative_tab.render()

# Markets tab
with tabs[3]:
    markets_tab.render()

# Sector detail
with tabs[4]:
    st.subheader("Sector Detail")
    sector = st.selectbox("Sector", config.SECTORS)
    sector_feat = features[features["sector"] == sector]
    if sector_feat.empty:
        st.warning("No data.")
    else:
        metric_cols = [
            "new_papers_7d",
            "new_papers_30d",
            "recruiting_trials_30d",
            "jobs_keyword_count",
            "github_stars_30d",
            "grants_90d",
        ]
        st.line_chart(sector_feat.set_index("ts")[metric_cols])
        latest_row = scores[(scores["sector"] == sector) & (scores["ts"] == scores["ts"].max())]
        if not latest_row.empty:
            components = latest_row.iloc[0]["components"]
            comp = pd.DataFrame(
                list(json.loads(components).items()), columns=["metric", "zscore"]
            )
            st.table(comp)
        sector_events = events[events["sector"] == sector].head(50)
        st.dataframe(
            sector_events[
                ["ts", "source", "entity", "metric", "value", "confidence", "source_url"]
            ],
            use_container_width=True,
        )
        note = st.text_area("Add note", placeholder="Hypothesis / explain anomaly")
        if st.button("Save note"):
            _add_note(sector, note)
            st.success("Note saved.")

# Coverage tab
with tabs[5]:
    st.subheader("Coverage & Health")
    if events.empty:
        st.info("No events yet.")
    else:
        coverage = events.groupby(["source", "sector"])["fetched_at"].max().reset_index()
        coverage["fetched_at"] = pd.to_datetime(coverage["fetched_at"])
        coverage["hours_old"] = (
            datetime.now(timezone.utc) - coverage["fetched_at"]
        ).dt.total_seconds() / 3600
        st.dataframe(coverage, use_container_width=True)
    with _connect() as conn:
        quarantine = pd.read_sql_query(
            "SELECT error, COUNT(*) as count FROM events_quarantine GROUP BY error", conn
        )
    st.subheader("Quarantine breakdown")
    st.dataframe(quarantine, use_container_width=True)

# Brief tab
with tabs[6]:
    brief_tab.render()
