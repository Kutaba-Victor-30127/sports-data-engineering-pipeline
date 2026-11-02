import pandas as pd
import pytest

from src.validation.event_validator import (
    partition_events,
    validate_events,
)

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
        "away_odds": [4.20, 6.50],
        "source_updated_at": pd.to_datetime([
        "2026-10-26T10:00:00Z",
        "2026-10-26T10:00:00Z"
        ], utc=True)
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

def test_missing_source_updated_at_is_rejected():
    df = create_valid_events()

    df.loc[0, "source_updated_at"] = pd.NaT

    with pytest.raises(
        ValueError,
        match="source_updated_at cannot be null"
    ):
        validate_events(df)


def test_non_numeric_home_odds_are_rejected():
    df = create_valid_events()

    df["home_odds"] = df["home_odds"].astype("object")
    df.loc[0, "home_odds"] = "not-a-number"

    valid_df, rejected_df = partition_events(df)

    assert len(valid_df) == 1
    assert len(rejected_df) == 1

    assert (
        rejected_df.iloc[0]["rejection_reason"]
        == "home_odds must be positive"
    )


def test_invalid_source_updated_at_is_rejected():
    df = create_valid_events()

    df["source_updated_at"] = df["source_updated_at"].astype("object")
    df.loc[0, "source_updated_at"] = "not-a-timestamp"

    valid_df, rejected_df = partition_events(df)

    assert len(valid_df) == 1
    assert len(rejected_df) == 1

    assert (
        rejected_df.iloc[0]["rejection_reason"]
        == "source_updated_at cannot be null or invalid"
    )
