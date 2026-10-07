import pandas as pd
import pytest

from src.validation.event_validator import validate_events

def create_valid_events():
    return pd.DataFrame({
        "event_id": [1, 2],
        "sport": ["football", "football"],
        "league": ["Premiere League", "La Liga"],
        "home_team": ["Arsenal", "Barcelona"],
        "away_team": ["Chelsea", "Valencia"],
        "event_time": pd.to_datetime([
            "2026-09-21 18:00:00",
            "2026-09-21 20:00:00"
        ]),
        "home_odds": [1.85, 1.50],
        "draw_odds": [3.40, 4.10],
        "away_odds": [4.20, 6.50]
    })

def test_valid_events_pass_validation():
    df = create_valid_events()

    validate_events(df)

def test_negative_home_odds_are_rejected():
    df = create_valid_events()

    df.loc[0, "home_odds"] = -1.85

    with pytest.raises(
        ValueError,
        match="home_odds must be positive"
    ):

        validate_events(df)

