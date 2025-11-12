import pandas as pd

from core.scoring import compute_scores


def test_compute_scores_zero_std_returns_zero_z():
    data = pd.DataFrame(
        [
            {
                "ts": pd.Timestamp("2024-01-01"),
                "sector": "ai",
                "new_papers_7d": 1.0,
                "new_papers_30d": 2.0,
                "recruiting_trials_30d": 3.0,
                "jobs_keyword_count": 4.0,
                "github_stars_30d": 5.0,
                "grants_90d": 6.0,
                "consensus_disagreement": 0.1,
                "confidence_mean": 0.8,
            },
            {
                "ts": pd.Timestamp("2024-01-02"),
                "sector": "ai",
                "new_papers_7d": 1.0,
                "new_papers_30d": 2.0,
                "recruiting_trials_30d": 3.0,
                "jobs_keyword_count": 4.0,
                "github_stars_30d": 5.0,
                "grants_90d": 6.0,
                "consensus_disagreement": 0.1,
                "confidence_mean": 0.8,
            },
        ]
    )
    scores = compute_scores(data)
    assert (scores["score"] == 0).all()
    assert (scores["mean_confidence"] == 0.8).all()
