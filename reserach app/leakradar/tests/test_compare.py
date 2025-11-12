import sqlite3
from pathlib import Path

import pandas as pd

from core import compare


def test_build_indices_handles_missing_social(monkeypatch, tmp_path):
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE features (
            ts TEXT,
            sector TEXT,
            new_papers_7d REAL,
            new_papers_30d REAL,
            recruiting_trials_30d REAL,
            jobs_keyword_count REAL,
            github_stars_30d REAL,
            grants_90d REAL,
            consensus_disagreement REAL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE comparisons (
            ts TEXT,
            sector TEXT,
            hype_index REAL,
            reality_index REAL,
            gap REAL
        )
        """
    )
    rows = [
        (
            "2024-01-01T00:00:00+00:00",
            "ai",
            5,
            10,
            1,
            3,
            2,
            0.5,
            0,
        ),
        (
            "2024-01-02T00:00:00+00:00",
            "ai",
            8,
            13,
            2,
            4,
            3,
            0.8,
            0,
        ),
    ]
    conn.executemany(
        """
        INSERT INTO features VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )

    def fake_conn():
        class _Ctx:
            def __enter__(self_inner):
                return conn

            def __exit__(self_inner, exc_type, exc, tb):
                return False

        return _Ctx()

    media_df = pd.DataFrame(
        {"ts": pd.to_datetime(["2024-01-02"]), "sector": ["ai"], "media_hits": [10], "media_z": [1.0]}
    )
    social_df = pd.DataFrame(columns=["ts", "sector", "social_mentions", "social_z"])

    monkeypatch.setattr(compare, "get_connection", fake_conn)
    monkeypatch.setattr(compare, "media_density", lambda: media_df)
    monkeypatch.setattr(compare, "social_pulse", lambda: social_df)

    rows = compare.build_indices()
    assert rows
    assert rows[0].sector == "ai"
    assert rows[0].gap is not None
