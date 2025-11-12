"""GitHub repo collector."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import requests

from collectors.base import checksum_payload, persist_rows
from core.config import BASE_DIR, GITHUB_TOKEN

REPOS_PATH = BASE_DIR / "tracked" / "repos.csv"
PARSE_VERSION = "github_api_v1"
API_BASE = "https://api.github.com"


def _headers():
    headers = {"Accept": "application/vnd.github+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    return headers


def _confidence() -> float:
    return 0.9 if GITHUB_TOKEN else 0.7


def _load_repos() -> List[Dict[str, str]]:
    if not REPOS_PATH.exists():
        return []
    with REPOS_PATH.open(encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        return list(reader)


def collect():
    rows = []
    confidence = _confidence()
    headers = _headers()
    for row in _load_repos():
        owner = row.get("owner", "")
        repo_name = row.get("repo")
        if "/" in owner and not repo_name:
            owner, repo_name = owner.split("/", 1)
        url = f"{API_BASE}/repos/{owner}/{repo_name}"
        now = datetime.now(timezone.utc).isoformat()
        releases_count = 0.0
        try:
            resp = requests.get(url, headers=headers, timeout=30)
            status = resp.status_code
            data = resp.json() if status == 200 else {}
            rel_resp = requests.get(
                f"{url}/releases", headers=headers, params={"per_page": 100}, timeout=30
            )
            if rel_resp.status_code == 200:
                releases_count = float(len(rel_resp.json()))
        except Exception as exc:
            status = None
            data = {"error": str(exc)}
            releases_count = 0.0
        payload = data if isinstance(data, dict) else {"note": "non-json"}
        stars = float(payload.get("stargazers_count", 0) or 0)
        base_payload = {
            "repo": payload.get("full_name"),
            "stars": stars,
            "forks": payload.get("forks_count"),
            "open_issues": payload.get("open_issues_count"),
        }
        rows.append(
            {
                "ts": now,
                "sector": row.get("sector", "ai"),
                "entity": payload.get("full_name", f"{owner}/{repo_name}"),
                "metric": "stars",
                "value": stars,
                "payload": base_payload,
                "source_url": url,
                "parse_version": PARSE_VERSION,
                "checksum": checksum_payload(base_payload),
                "license": payload.get("license", {}).get("name") if isinstance(payload.get("license"), dict) else None,
                "confidence": confidence,
                "http_status": status,
            }
        )
        release_payload = {"repo": payload.get("full_name"), "releases_estimate": releases_count}
        rows.append(
            {
                "ts": now,
                "sector": row.get("sector", "ai"),
                "entity": payload.get("full_name", f"{owner}/{repo_name}"),
                "metric": "releases",
                "value": releases_count,
                "payload": release_payload,
                "source_url": url,
                "parse_version": PARSE_VERSION,
                "checksum": checksum_payload(release_payload),
                "license": payload.get("license", {}).get("name") if isinstance(payload.get("license"), dict) else None,
                "confidence": confidence,
                "http_status": status,
            }
        )
    inserted, quarantined = persist_rows("github", rows)
    return {"inserted": inserted, "quarantined": quarantined}
