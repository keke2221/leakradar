"""ClinicalTrials.gov recruiting oncology collector."""

from __future__ import annotations

from datetime import datetime, timezone

import requests

from collectors.base import checksum_payload, persist_rows

API_URL = (
    "https://clinicaltrials.gov/api/query/study_fields?"
    "expr=oncology&recr=Recruiting&min_rnk=1&max_rnk=100"
    "&fields=NCTId,BriefTitle,Phase,EnrollmentCount,LastUpdateSubmitDate"
)

CONFIDENCE = 0.9
PARSE_VERSION = "ctgov_v1"


def collect():
    now = datetime.now(timezone.utc).isoformat()
    try:
        resp = requests.get(API_URL, timeout=30)
        status = resp.status_code
        data = resp.json() if status == 200 else {}
    except Exception as exc:
        status = None
        data = {"error": str(exc)}

    studies = (
        data.get("StudyFieldsResponse", {}).get("StudyFields", []) if isinstance(data, dict) else []
    )
    payload = {"sample": studies[:5], "count": len(studies)}

    row = {
        "ts": now,
        "sector": "biotech",
        "entity": "clinicaltrials.gov",
        "metric": "recruiting_trials",
        "value": float(len(studies)),
        "payload": payload,
        "source_url": API_URL,
        "parse_version": PARSE_VERSION,
        "checksum": checksum_payload(payload),
        "license": "ClinicalTrials.gov",
        "confidence": CONFIDENCE,
        "http_status": status,
    }
    inserted, quarantined = persist_rows("clinicaltrials", [row])
    return {"inserted": inserted, "quarantined": quarantined}
