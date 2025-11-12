"""Run all collectors sequentially."""

from __future__ import annotations

from core.db import init_db
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

COLLECTORS = [
    ("arxiv", arxiv.collect),
    ("clinicaltrials", clinicaltrials.collect),
    ("jobs", jobs.collect),
    ("github", github.collect),
    ("grants", grants.collect),
    ("news", news_collector.collect),
    ("social", social_collector.collect),
    ("markets", markets_collector.collect),
]


def main():
    init_db()
    summary = {}
    for name, fn in COLLECTORS:
        try:
            result = fn()
        except Exception as exc:
            result = {"error": str(exc)}
        summary[name] = result
    return summary


if __name__ == "__main__":
    print(main())
