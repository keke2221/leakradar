from datetime import datetime, timezone

from core.validate import validate_event


def test_validate_event_metric_and_value():
    ok, err = validate_event(
        {
            "ts": datetime.now(timezone.utc).isoformat(),
            "metric": "new_papers",
            "value": 1.0,
            "confidence": 0.5,
            "http_status": 200,
        }
    )
    assert ok
    assert err is None


def test_validate_event_rejects_negative_value():
    ok, err = validate_event(
        {
            "ts": datetime.now(timezone.utc).isoformat(),
            "metric": "new_papers",
            "value": -1,
        }
    )
    assert not ok
    assert "value" in err
