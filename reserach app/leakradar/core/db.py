"""SQLite helpers with lightweight migrations."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, Iterable, Iterator, List

from .config import DB_PATH


def _connect() -> sqlite3.Connection:
    path = Path(DB_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


@contextmanager
def get_connection() -> Iterator[sqlite3.Connection]:
    conn = _connect()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: Dict[str, str]) -> None:
    cur = conn.execute(f"PRAGMA table_info({table});")
    existing = {row["name"] for row in cur.fetchall()}
    for name, ddl in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl};")


def _create_table(conn: sqlite3.Connection, name: str, columns: Iterable[str]) -> None:
    ddl = ", ".join(columns)
    conn.execute(f"CREATE TABLE IF NOT EXISTS {name} ({ddl});")


def init_db() -> None:
    with get_connection() as conn:
        _create_table(
            conn,
            "events",
            [
                "id INTEGER PRIMARY KEY",
                "ts TEXT",
                "source TEXT",
                "sector TEXT",
                "entity TEXT",
                "metric TEXT",
                "value REAL",
                "payload TEXT",
                "source_url TEXT",
                "fetched_at TEXT",
                "parse_version TEXT",
                "checksum TEXT",
                "license TEXT",
                "confidence REAL",
            ],
        )
        _ensure_columns(
            conn,
            "events",
            {
                "source_url": "TEXT",
                "fetched_at": "TEXT",
                "parse_version": "TEXT",
                "checksum": "TEXT",
                "license": "TEXT",
                "confidence": "REAL",
            },
        )

        _create_table(
            conn,
            "events_quarantine",
            [
                "id INTEGER PRIMARY KEY",
                "ts TEXT",
                "source TEXT",
                "sector TEXT",
                "entity TEXT",
                "metric TEXT",
                "value REAL",
                "payload TEXT",
                "source_url TEXT",
                "fetched_at TEXT",
                "parse_version TEXT",
                "checksum TEXT",
                "license TEXT",
                "confidence REAL",
                "error TEXT",
            ],
        )
        _ensure_columns(
            conn,
            "events_quarantine",
            {
                "source_url": "TEXT",
                "fetched_at": "TEXT",
                "parse_version": "TEXT",
                "checksum": "TEXT",
                "license": "TEXT",
                "confidence": "REAL",
                "error": "TEXT",
            },
        )

        _create_table(
            conn,
            "features",
            [
                "ts TEXT",
                "sector TEXT",
                "new_papers_7d REAL",
                "new_papers_30d REAL",
                "recruiting_trials_30d REAL",
                "jobs_keyword_count REAL",
                "github_stars_30d REAL",
                "grants_90d REAL",
                "consensus_disagreement REAL",
            ],
        )

        _create_table(
            conn,
            "scores",
            [
                "ts TEXT",
                "sector TEXT",
                "score REAL",
                "components TEXT",
                "mean_confidence REAL",
            ],
        )
        _ensure_columns(conn, "scores", {"mean_confidence": "REAL"})

        _create_table(
            conn,
            "narrative_events",
            [
                "id INTEGER PRIMARY KEY",
                "ts TEXT",
                "source TEXT",
                "sector TEXT",
                "metric TEXT",
                "value REAL",
                "payload TEXT",
                "source_url TEXT",
                "confidence REAL",
            ],
        )

        _create_table(
            conn,
            "market_events",
            [
                "id INTEGER PRIMARY KEY",
                "ts TEXT",
                "sector TEXT",
                "symbol TEXT",
                "kind TEXT",
                "metric TEXT",
                "value REAL",
                "payload TEXT",
                "confidence REAL",
            ],
        )

        _create_table(
            conn,
            "comparisons",
            [
                "ts TEXT",
                "sector TEXT",
                "hype_index REAL",
                "reality_index REAL",
                "gap REAL",
            ],
        )

        _create_table(
            conn,
            "briefs",
            [
                "ts TEXT",
                "sector TEXT",
                "title TEXT",
                "summary TEXT",
                "sources TEXT",
            ],
        )

        _create_table(
            conn,
            "runs",
            [
                "run_id TEXT PRIMARY KEY",
                "started_at TEXT",
                "finished_at TEXT",
                "code_sha TEXT",
                "config_sha TEXT",
                "status TEXT",
            ],
        )

        _create_table(
            conn,
            "anomalies",
            [
                "ts TEXT",
                "run_id TEXT",
                "sector TEXT",
                "metric TEXT",
                "zscore REAL",
                "confidence REAL",
                "verified_status TEXT",
            ],
        )
        _ensure_columns(conn, "anomalies", {"ts": "TEXT"})

        _create_table(
            conn,
            "entities",
            [
                "id INTEGER PRIMARY KEY",
                "kind TEXT",
                "canonical TEXT",
                "aliases TEXT",
            ],
        )

        _create_table(
            conn,
            "notes",
            [
                "ts TEXT",
                "sector TEXT",
                "text TEXT",
            ],
        )


__all__ = [
    "get_connection",
    "init_db",
]
