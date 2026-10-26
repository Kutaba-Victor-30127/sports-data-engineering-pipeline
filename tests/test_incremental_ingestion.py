from decimal import Decimal

import pandas as pd
import pytest
from sqlalchemy import text

from src.database.connection import get_engine
from src.ingestion.load_events import load_to_staging

def create_batch(
    batch_id,
    event_1_odds,
    source_updated_at
):
    return pd.DataFrame({
        "event_id": [1, 2, 3, 4, 5],

        "sport": [
            "football",
            "football",
            "football",
            "football",
            "football"
        ],

        "league": [
            "Premier League",
            "La Liga",
            "Serie A",
            "Bundesliga",
            "Premier League"
        ],

        "home_team": [
            "Arsenal",
            "Barcelona",
            "Inter",
            "Bayern Munich",
            "Liverpool"
        ],

        "away_team": [
            "Chelsea",
            "Valencia",
            "Milan",
            "Dortmund",
            "Manchester City"
        ],

        "event_time": pd.to_datetime([
            "2026-09-21 18:00:00",
            "2026-09-21 20:00:00",
            "2026-09-22 19:45:00",
            "2026-09-22 18:30:00",
            "2026-09-23 20:00:00"
        ]),

        "home_odds": [
            event_1_odds,
            1.50,
            2.10,
            1.65,
            2.45
        ],

        "draw_odds": [
            3.40,
            4.10,
            3.20,
            4.00,
            3.30
        ],

        "away_odds": [
            4.20,
            6.50,
            3.50,
            5.10,
            2.80
        ],

        "source_file": [
            f"raw/test/batch_id={batch_id}/events.csv"
        ] * 5,

        "batch_id": [batch_id] * 5,

        "ingested_at": [
            pd.Timestamp.now(tz="UTC")
        ] * 5,

        "source_updated_at": [
            pd.Timestamp(source_updated_at)
        ] * 5
    })

@pytest.fixture
def clean_staging():
    engine = get_engine()

    with engine.begin() as connection:
        connection.execute(
            text("TRUNCATE TABLE staging.raw_events")
        )

    yield

    with engine.begin() as connection:
        connection.execute(
            text("TRUNCATE TABLE staging.raw_events")
        )


def test_incremental_ingestion_is_idempotent_and_replay_safe(
    clean_staging
):
    engine = get_engine()

    batch_a = create_batch(
        batch_id="batch-a",
        event_1_odds=1.90,
        source_updated_at="2026-10-26T10:00:00Z"
    )

    load_to_staging(batch_a)

    with engine.begin() as connection:
        count = connection.execute(
            text(
                "SELECT COUNT(*) "
                "FROM staging.raw_events"
            )
        ).scalar_one()


    assert count == 5

    load_to_staging(batch_a)

    with engine.connect() as connection:
        count = connection.execute(
            text(
                "SELECT COUNT(*) "
                "FROM staging.raw_events"
            )
        ).scalar_one()

    assert count == 5


    batch_b = create_batch(
        batch_id="batch-b",
        event_1_odds=1.95,
        source_updated_at="2026-10-26T11:00:00Z"
    )

    load_to_staging(batch_b)

    with engine.connect() as connection:
        event = connection.execute(
            text("""
                SELECT 
                    home_odds,
                    source_updated_at,
                    batch_id
                FROM staging.raw_events
                WHERE event_id = 1
            """)
        ).mappings().one()

    assert event["home_odds"] == Decimal("1.95")
    assert event["batch_id"] == "batch-b"

    load_to_staging(batch_a)

    with engine.connect() as connection:
        event = connection.execute(
            text("""
                SELECT
                    home_odds,
                    source_updated_at,
                    batch_id
                FROM staging.raw_events
                WHERE event_id = 1
            """)
        ).mappings().one()

        count = connection.execute(
            text(
                "SELECT COUNT(*) "
                "FROM staging.raw_events"
            )
        ).scalar_one()

    assert event["home_odds"] == Decimal("1.95")
    assert event["batch_id"] == "batch-b"
    assert count == 5

        