import pandas as pd
import pytest
from sqlalchemy import text

from src.database.connection import (
    get_engine,
)
from src.ingestion.load_events import (
    load_to_history,
)


TEST_EVENT_MIN = 900001
TEST_EVENT_MAX = 900010


@pytest.fixture(autouse=True)
def clean_history_test_rows():
    engine = get_engine()

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                DELETE FROM staging.event_odds_history
                WHERE event_id BETWEEN
                    :min_id AND :max_id
                """
            ),
            {
                "min_id": TEST_EVENT_MIN,
                "max_id": TEST_EVENT_MAX,
            },
        )

    yield

    with engine.begin() as connection:
        connection.execute(
            text(
                """
                DELETE FROM staging.event_odds_history
                WHERE event_id BETWEEN
                    :min_id AND :max_id
                """
            ),
            {
                "min_id": TEST_EVENT_MIN,
                "max_id": TEST_EVENT_MAX,
            },
        )


def create_history_events():
    return pd.DataFrame({
        "event_id": [
            900001,
            900002,
        ],
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
        "home_odds": [
            1.95,
            1.50,
        ],
        "draw_odds": [
            3.40,
            4.10,
        ],
        "away_odds": [
            4.20,
            6.50,
        ],
        "source_file": [
            "test/events.csv",
            "test/events.csv",
        ],
        "batch_id": [
            "history-test-batch",
            "history-test-batch",
        ],
        "ingested_at": pd.to_datetime(
            [
                "2026-11-16T10:00:00Z",
                "2026-11-16T10:00:00Z",
            ],
            utc=True,
        ),
        "source_updated_at": pd.to_datetime(
            [
                "2026-11-16T09:00:00Z",
                "2026-11-16T09:00:00Z",
            ],
            utc=True,
        ),
    })


def test_history_replay_is_idempotent():
    df = create_history_events()

    first_load = load_to_history(df)

    second_load = load_to_history(df)

    assert (
        first_load[
            "history_inserted_count"
        ]
        == 2
    )

    assert (
        first_load[
            "history_ignored_count"
        ]
        == 0
    )

    assert (
        second_load[
            "history_inserted_count"
        ]
        == 0
    )

    assert (
        second_load[
            "history_ignored_count"
        ]
        == 2
    )


def test_new_source_version_is_added_to_history():
    initial_df = (
        create_history_events()
        .iloc[[0]]
        .copy()
    )

    updated_df = initial_df.copy()

    updated_df.loc[
        updated_df.index[0],
        "home_odds",
    ] = 1.85

    updated_df.loc[
        updated_df.index[0],
        "source_updated_at",
    ] = pd.Timestamp(
        "2026-11-16T11:00:00Z"
    )

    first_load = load_to_history(
        initial_df
    )

    second_load = load_to_history(
        updated_df
    )

    assert (
        first_load[
            "history_inserted_count"
        ]
        == 1
    )

    assert (
        second_load[
            "history_inserted_count"
        ]
        == 1
    )

    engine = get_engine()

    with engine.connect() as connection:
        version_count = (
            connection.execute(
                text(
                    """
                    SELECT COUNT(*)
                    FROM staging.event_odds_history
                    WHERE event_id = 900001
                    """
                )
            )
            .scalar_one()
        )

    assert version_count == 2