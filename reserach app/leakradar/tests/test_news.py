from types import SimpleNamespace

from collectors import news


def test_newsapi_payload_returns_media_hits(monkeypatch):
    monkeypatch.setattr(news.config, "NEWSAPI_KEY", "test-key", raising=False)

    def fake_get(url, params=None, headers=None, timeout=None):
        data = {
            "totalResults": 42,
            "articles": [
                {"title": "AI funding boom", "url": "https://example.com/a1"},
                {"title": "GPU supply", "url": "https://example.com/a2"},
            ],
        }
        return SimpleNamespace(json=lambda: data, raise_for_status=lambda: None)

    monkeypatch.setattr(news.requests, "get", fake_get)
    payload = news._newsapi_payload("ai")
    assert isinstance(payload["media_hits"], int)
    assert payload["media_hits"] == 42
