"""Narrative tab components."""

from __future__ import annotations

import sqlite3

import pandas as pd
import streamlit as st

from core import config
from core.news import latest_topics, media_density, social_pulse


def _load_comparisons():
    with sqlite3.connect(config.DB_PATH) as conn:
        df = pd.read_sql_query("SELECT * FROM comparisons", conn)
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"])
    return df


def render():
    st.subheader("Narrative Signals")
    media_df = media_density()
    social_df = social_pulse()
    comparisons = _load_comparisons()

    if media_df.empty and social_df.empty:
        st.info("No narrative data yet. Run `python run_all.py` after configuring NewsAPI or Perplexity keys.")
        return

    if not media_df.empty:
        pivot = media_df.pivot(index="ts", columns="sector", values="media_hits")
        st.line_chart(pivot, height=250)

    if not social_df.empty:
        pivot_social = social_df.pivot(index="ts", columns="sector", values="social_mentions")
        st.line_chart(pivot_social, height=250)

    if not comparisons.empty:
        latest_ts = comparisons["ts"].max()
        latest = comparisons[comparisons["ts"] == latest_ts]
        st.dataframe(
            latest[["sector", "hype_index", "reality_index", "gap"]].set_index("sector"),
            use_container_width=True,
        )

    topics = latest_topics()
    if topics:
        st.markdown("### Top Topics & Sources")
        for sector, data in topics.items():
            st.markdown(f"**{sector.title()}**")
            if data.get("top_topics"):
                st.write(", ".join(data["top_topics"]))
            if data.get("sources"):
                for url in data["sources"][:5]:
                    st.markdown(f"- [{url}]({url})")
