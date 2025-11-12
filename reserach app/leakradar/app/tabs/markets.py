"""Markets tab."""

from __future__ import annotations

import sqlite3

import pandas as pd
import streamlit as st

from core import config
from core.markets import sector_pulse, top_movers


def _sparkline_data():
    with sqlite3.connect(config.DB_PATH) as conn:
        df = pd.read_sql_query("SELECT ts, sector, symbol, metric, value FROM market_events", conn)
    if df.empty:
        return pd.DataFrame()
    df = df[df["metric"] == "price_change_7d"].copy()
    df["ts"] = pd.to_datetime(df["ts"])
    pivot = df.pivot_table(index="ts", columns="sector", values="value", aggfunc="mean")
    return pivot


def render():
    st.subheader("Market Attention Signals")
    pulse = sector_pulse()
    if not pulse:
        st.info("No market data yet. Populate tracked/tickers.csv and run collectors.")
        return
    pulse_rows = []
    movers = top_movers()
    for sector, values in pulse.items():
        sector_movers = ", ".join(
            f"{item['symbol']} ({item['value']:+.2f}%)" for item in movers.get(sector, [])
        )
        pulse_rows.append(
            {
                "sector": sector,
                "median_price_change_7d": values["price_change_7d"],
                "median_volume_7d": values["volume_7d"],
                "top_movers": sector_movers or "—",
            }
        )
    st.dataframe(pd.DataFrame(pulse_rows).set_index("sector"), use_container_width=True)

    spark_data = _sparkline_data()
    if not spark_data.empty:
        st.line_chart(spark_data, height=200)

    st.caption("Disclaimer: markets are noisy and used here only as an attention proxy.")
