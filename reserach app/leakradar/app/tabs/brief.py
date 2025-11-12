"""Founder brief tab."""

from __future__ import annotations

import sqlite3

import pandas as pd
import streamlit as st

from core import config


def _load_briefs():
    with sqlite3.connect(config.DB_PATH) as conn:
        df = pd.read_sql_query("SELECT * FROM briefs", conn)
    if df.empty:
        return df
    df["ts"] = pd.to_datetime(df["ts"])
    return df


def render():
    st.subheader("Founder Briefs")
    briefs = _load_briefs()
    if briefs.empty:
        st.info("No briefs yet. Run `python scripts/run_brief.py` after compute.")
        return
    latest_ts = briefs["ts"].max()
    latest = briefs[briefs["ts"] == latest_ts]
    md_lines = [f"# Briefs {latest_ts.date()}"]
    for _, row in latest.iterrows():
        st.markdown(f"### {row['sector'].title()}")
        st.write(row["summary"])
        if row.get("sources"):
            st.caption(f"Sources: {row['sources']}")
        md_lines.append(f"## {row['sector'].title()}")
        md_lines.append(row["summary"])
        md_lines.append("")
    st.download_button(
        "Export Markdown",
        "\n".join(md_lines).encode("utf-8"),
        file_name=f"brief_{latest_ts.date()}.md",
    )
