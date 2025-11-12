import pandas as pd

from core import markets


def test_sector_pulse_uses_medians(monkeypatch):
    df = pd.DataFrame(
        {
            "ts": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-02"], utc=True),
            "sector": ["ai", "ai", "biotech"],
            "symbol": ["NVDA", "MSFT", "MRNA"],
            "kind": ["ticker", "ticker", "ticker"],
            "metric": ["price_change_7d", "volume_7d", "price_change_7d"],
            "value": [5.0, 1000.0, -2.0],
            "payload": ["{}", "{}", "{}"],
            "confidence": [0.8, 0.8, 0.8],
        }
    )

    monkeypatch.setattr(markets, "_load_market_df", lambda conn=None: df)
    pulse = markets.sector_pulse(window_days=1000)
    assert "ai" in pulse
    assert pulse["ai"]["price_change_7d"] == 5.0
