"""End-to-end orchestration for LeakSearcher."""

from __future__ import annotations

import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from random import randint

import pandas as pd

from collectors import (
    arxiv,
    clinicaltrials,
    github,
    grants,
    jobs,
    markets as markets_collector,
    news as news_collector,
    social as social_collector,
)
from compute.aggregate import run_compute
from core import config
from core.backtest import run_backtest
from core.compare import build_indices
from core.db import get_connection, init_db
from core.log import get_logger
from core.monitor import collector_health, severe_spike_budget, summarize_collector_health
from core.news import latest_topics
from scripts import run_brief as run_brief_script

LOG = get_logger()


def _new_run_id() -> str:
    now = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"{now}-{randint(1000, 9999)}"


def _git_sha() -> str:
    try:
        return (
            subprocess.check_output(["git", "-C", str(Path(__file__).parent), "rev-parse", "HEAD"])
            .decode()
            .strip()
        )
    except Exception:
        return "unknown"


def _record_run(run_id: str, status: str, started_at: str, finished_at: str | None = None):
    with get_connection() as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO runs (run_id, started_at, finished_at, code_sha, config_sha, status)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                started_at,
                finished_at,
                _git_sha(),
                config.hash_config(),
                status,
            ),
        )


def _run_collectors():
    summary = {}
    collector_modules = [
        arxiv,
        clinicaltrials,
        jobs,
        github,
        grants,
        news_collector,
        social_collector,
        markets_collector,
    ]
    for module in collector_modules:
        name = module.__name__.split(".")[-1]
        try:
            result = module.collect()
        except Exception as exc:
            result = {"error": str(exc), "inserted": 0, "quarantined": 0}
        summary[name] = result
        LOG.info("collector %s => %s", name, result)
    return summary


def _insert_anomalies(run_id: str) -> pd.DataFrame:
    with get_connection() as conn:
        df = pd.read_sql_query(
            "SELECT ts, sector, score, components, mean_confidence FROM scores", conn
        )
        if df.empty:
            return df
        df["ts"] = pd.to_datetime(df["ts"])
        latest_ts = df["ts"].max()
        latest = df[df["ts"] == latest_ts]
        conn.execute("DELETE FROM anomalies WHERE run_id = ?", (run_id,))
        rows = []
        for _, row in latest.iterrows():
            components = json.loads(row["components"])
            for metric, zscore in components.items():
                if abs(zscore) >= config.ANOMALY_Z:
                    conn.execute(
                        """
                        INSERT INTO anomalies (ts, run_id, sector, metric, zscore, confidence, verified_status)
                        VALUES (?, ?, ?, ?, ?, ?, NULL)
                        """,
                        (
                            latest_ts.isoformat(),
                            run_id,
                            row["sector"],
                            metric,
                            float(zscore),
                            float(row["mean_confidence"]),
                        ),
                    )
                    rows.append(
                        {
                            "ts": latest_ts,
                            "sector": row["sector"],
                            "metric": metric,
                            "zscore": zscore,
                            "confidence": row["mean_confidence"],
                        }
                    )
        return pd.DataFrame(rows)


def _send_alerts(anomalies: pd.DataFrame, scores: pd.DataFrame):
    if not (config.TELEGRAM_BOT_TOKEN and config.TELEGRAM_CHAT_ID):
        return
    import requests

    high_scores = scores[scores["score"] >= config.ALERT_SCORE]
    lines = []
    for _, row in high_scores.iterrows():
        lines.append(
            f"{row['sector']} score {row['score']:.2f} (conf {row['mean_confidence']:.2f})"
        )
    severe = anomalies[anomalies["zscore"].abs() >= config.SEVERE_Z]
    for _, row in severe.iterrows():
        lines.append(
            f"{row['sector']} {row['metric']} z={row['zscore']:.2f} (conf {row['confidence']:.2f})"
        )
    if not lines:
        return
    message = "LeakRadar alerts:\n" + "\n".join(lines[:10])
    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    requests.post(url, json={"chat_id": config.TELEGRAM_CHAT_ID, "text": message})


def main():
    init_db()
    run_id = _new_run_id()
    started_at = datetime.now(timezone.utc).isoformat()
    _record_run(run_id, "running", started_at)

    collectors_summary = _run_collectors()
    compute_summary = run_compute()
    anomalies = _insert_anomalies(run_id)
    comparison_rows = build_indices()
    brief_result = run_brief_script.main()

    with get_connection() as conn:
        scores_df = pd.read_sql_query("SELECT ts, sector, score, mean_confidence FROM scores", conn)
        scores_df["ts"] = pd.to_datetime(scores_df["ts"])
        status = "ok"
        severe_flag = False
        if not anomalies.empty:
            severe_flag = (anomalies["zscore"].abs() >= config.SEVERE_Z).any()
        health = collector_health(conn)
        stale = [s.source for s in health if s.stale]
        if not anomalies.empty:
            anomalies["ts"] = pd.to_datetime(anomalies["ts"])
        if stale or severe_flag or severe_spike_budget(anomalies if not anomalies.empty else pd.DataFrame()):
            status = "warn"
        _record_run(run_id, status, started_at, datetime.now(timezone.utc).isoformat())
        run_backtest(conn)
        LOG.info("collector health: %s", summarize_collector_health(health))
        LOG.info("compute summary: %s", compute_summary)
        LOG.info("anomalies: %s", anomalies.to_dict(orient="records") if not anomalies.empty else "none")
        _send_alerts(anomalies, scores_df[scores_df["ts"] == scores_df["ts"].max()])

    inserted_total = sum(v.get("inserted", 0) for v in collectors_summary.values())
    quarantined_total = sum(v.get("quarantined", 0) for v in collectors_summary.values())
    high_scores = []
    if not scores_df.empty:
        latest = scores_df[scores_df["ts"] == scores_df["ts"].max()]
        high_scores = latest[latest["score"] >= config.ALERT_SCORE]["sector"].tolist()

    print("=== LeakSearcher run ===")
    print(f"run_id: {run_id} status:{status}")
    print(f"events inserted: {inserted_total} | quarantined: {quarantined_total}")
    print(f"high scoring sectors: {', '.join(high_scores) if high_scores else 'none'}")
    print(f"stale sources: {', '.join(stale) if stale else 'none'}")
    severe = anomalies[anomalies['zscore'].abs() >= config.SEVERE_Z] if not anomalies.empty else pd.DataFrame()
    print(f"severe anomalies: {severe.shape[0]}")
    if comparison_rows:
        print("Hype vs Reality:")
        for row in comparison_rows:
            print(f"  {row.sector}: hype {row.hype_index:.1f}, reality {row.reality_index:.1f}, gap {row.gap:+.1f}")
    topics = latest_topics()
    for sector, data in topics.items():
        srcs = data.get("sources", [])[:3]
        if srcs:
            print(f"{sector.title()} sources: {', '.join(srcs)}")
    if isinstance(brief_result, dict):
        print(f"briefs generated: {brief_result.get('generated', 0)}")


if __name__ == "__main__":
    main()
