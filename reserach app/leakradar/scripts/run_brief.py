"""Generate and persist founder briefs."""

from __future__ import annotations

from pathlib import Path

import requests

from core import config
from core.db import get_connection
from core.insights import Brief, generate_briefs
from core.log import get_logger

LOG = get_logger()
BRIEF_DIR = config.DATA_DIR / "briefs"
BRIEF_DIR.mkdir(parents=True, exist_ok=True)


def _persist(briefs: list[Brief]):
    with get_connection() as conn:
        conn.executemany(
            """
            INSERT INTO briefs (ts, sector, title, summary, sources)
            VALUES (?, ?, ?, ?, ?)
            """,
            [(b.ts.isoformat(), b.sector, b.title, b.summary, ",".join(b.sources)) for b in briefs],
        )


def _send_telegram(message: str):
    if not (config.TELEGRAM_BOT_TOKEN and config.TELEGRAM_CHAT_ID):
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": config.TELEGRAM_CHAT_ID, "text": message},
            timeout=15,
        )
    except Exception as exc:
        LOG.error("Failed to send Telegram brief: %s", exc)


def _archive_md(briefs: list[Brief]):
    if not briefs:
        return
    date_str = briefs[0].ts.date().isoformat()
    path = BRIEF_DIR / f"brief_{date_str}.md"
    lines = [f"# Founder Briefs ({date_str})", ""]
    for brief in briefs:
        lines.append(f"## {brief.sector.title()}")
        lines.append(brief.summary)
        if brief.sources:
            lines.append("")
            lines.append("Sources:")
            for src in brief.sources:
                lines.append(f"- {src}")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main():
    briefs = generate_briefs()
    if not briefs:
        LOG.warning("No comparisons available; skipping briefs.")
        return {"generated": 0}
    _persist(briefs)
    _archive_md(briefs)
    for brief in briefs:
        line = f"{brief.sector.title()}: {brief.summary.split('.')[0]}."
        _send_telegram(line[:350])
    LOG.info("Generated %d briefs", len(briefs))
    return {"generated": len(briefs)}


if __name__ == "__main__":
    print(main())
