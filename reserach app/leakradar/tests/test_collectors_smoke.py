from datetime import datetime, timezone

from collectors import arxiv, grants


def test_arxiv_collector_builds_rows(monkeypatch):
    sample_feed = {
        "entries": [
            {
                "title": "AI paper",
                "link": "https://arxiv.org/abs/1234",
                "published_parsed": datetime.now(timezone.utc).timetuple(),
            }
        ]
    }
    monkeypatch.setattr(arxiv, "_parse_feed", lambda url: sample_feed)
    captured = {}

    def fake_persist(source, rows):
        captured["rows"] = rows
        return len(rows), 0

    monkeypatch.setattr(arxiv, "persist_rows", fake_persist)
    arxiv.collect()
    assert captured["rows"]
    assert captured["rows"][0]["metric"] == "new_papers"


def test_grants_collector_uses_samples(monkeypatch):
    captured = {}

    def fake_persist(source, rows):
        captured["rows"] = rows
        return len(rows), 0

    monkeypatch.setattr(grants, "persist_rows", fake_persist)
    grants.collect()
    assert captured["rows"]
    assert captured["rows"][0]["metric"] == "grants"
