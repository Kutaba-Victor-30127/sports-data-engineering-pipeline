import pandas as pd
import pytest

from src.validation.batch_parity import (
    validate_batch_parity,
)


def create_events():
    return pd.DataFrame({
        "event_id": [1, 2],
        "sport": [
            "football",
            "football",
        ],
        "league": [
            "Premier League",
            "La Liga",
        ],
        "home_team": [
            "Arsenal",
            "Barcelona",
        ],
        "away_team": [
            "Chelsea",
            "Valencia",
        ],
        "event_time": pd.to_datetime([
            "2026-09-21 18:00:00",
            "2026-09-21 20:00:00",
        ]),
        "home_odds": [1.95, 1.50],
        "draw_odds": [3.40, 4.10],
        "away_odds": [4.20, 6.50],
        "source_updated_at": pd.to_datetime(
            [
                "2026-10-26T11:00:00Z",
                "2026-10-26T10:00:00Z",
            ],
            utc=True,
        ),
    })


def test_matching_csv_and_json_pass():
    csv_df = create_events()

    json_df = (
        create_events()
        .iloc[::-1]
        .reset_index(drop=True)
    )

    validate_batch_parity(
        csv_df=csv_df,
        json_df=json_df,
    )


def test_csv_json_mismatch_is_rejected():
    csv_df = create_events()
    json_df = create_events()

    json_df.loc[
        json_df["event_id"] == 1,
        "home_odds",
    ] = 2.25

    with pytest.raises(
        ValueError,
        match=(
            "CSV/JSON batch parity "
            "check failed"
        ),
    ):
        validate_batch_parity(
            csv_df=csv_df,
            json_df=json_df,
        )