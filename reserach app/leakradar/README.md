# LeakSearcher / LeakRadar

LeakSearcher is a single-user dashboard that tracks early market signals across AI, Biotech, Climate/Energy, and the Creator Economy. The stack is pure Python 3.11 + Streamlit with SQLite/WAL for $0 infra cost.

## Quickstart

```bash
python -m venv .venv
. .venv/Scripts/activate  # PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
cp .env.example .env  # fill tokens: GitHub, NewsAPI, etc.
python run_all.py      # run collectors + analytics once
streamlit run app/streamlit_app.py
```

Optional cron (daily 07:00):
```
0 7 * * * /path/to/.venv/bin/python /path/to/leakradar/run_all.py
```

## Repo layout

- `core/`: config, database/migrations, validation, scoring, triangulation, monitoring, hype-vs-reality comparator, founder briefs helpers
- `collectors/`: ArXiv, ClinicalTrials, jobs, GitHub, grants + new narrative (NewsAPI/Perplexity), social (SerpAPI), and markets (yfinance or AlphaVantage)
- `compute/`: feature aggregation & scoring logic
- `app/`: Streamlit app plus tab components (Leaderboard, Leak Feed, Narrative, Markets, Sector Detail, Coverage, Founder Briefs)
- `scripts/`: helpers such as `run_collectors.py`, `run_compute.py`, `run_brief.py`
- `data/`: SQLite DB (`data/leakradar.sqlite`) plus derived outputs (`backtest_summary.csv`, `data/briefs/*.md`)
- `tracked/`: CSV/JSON definitions for repos, careers, tickers, and news queries
- `tests/`: lightweight unit tests (validation, collectors, scoring)

## Features

- Provenance + confidence per event, schema validation, quarantine on invalid rows
- Narrative/news ingestion (Perplexity or NewsAPI) plus optional Reddit/SerpAPI social pulse
- Market pulse from configurable tickers/ETFs (yfinance or AlphaVantage)
- Hype vs Reality comparator (media + social vs. jobs/github/papers/grants)
- Founder Brief generation (Perplexity optional, markdown export + Telegram snippet)
- Triangulation with disagreement metrics, anomaly verification, and coverage health
- Backtests with persistence/false-spike stats feeding a notebook for visualization
- Streamlit UI cues for confidence, coverage, disagreement, hype gap, and verify buttons

## Sample data

- `tracked/repos.csv`: sample OSS repos across sectors
- `tracked/careers.json`: mock postings across ATS types
- `tracked/queries.json`: RSS/API seeds for grants/jobs
- `tracked/samples/grants.json`: small grant payload for offline demo
- `tracked/tickers.csv`: starter list of equities/ETFs for each sector
- `tracked/news_sources.json`: optional seed phrases for narrative collectors

## Tests

```bash
pytest
```

## API keys / env

Copy `.env.example` and fill as needed:

- `GITHUB_TOKEN` (optional, boosts GitHub collector confidence)
- `NEWSAPI_KEY` or `PERPLEXITY_API_KEY` (narrative); set `USE_PERPLEXITY` / `USE_YFINANCE` flags
- `SERPAPI_KEY` (optional social mentions)
- `ALPHAVANTAGE_KEY` (optional markets; otherwise yfinance)
- `TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID` for alerts and brief snippets

## Notes

- Database schema auto-migrates on startup via `core/db.py`.
- API calls respect polite rate limits; provide tokens for higher confidence/quotas.
- `python run_all.py` now runs: core collectors → news/social/markets → compute/compare → founder briefs.
- Telegram alerts and briefs are optional.
- No PII is stored; payloads are trimmed to public metadata.
