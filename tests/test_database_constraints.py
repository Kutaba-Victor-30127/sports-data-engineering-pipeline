import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from src.database.connection import get_engine


def test_database_rejects_null_source_updated_at():
    engine = get_engine()

    with pytest.raises(IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                text(
                    """
                    INSERT INTO staging.raw_events (
                        event_id,
                        sport,
                        league,
                        home_team,
                        away_team,
                        event_time,
                        home_odds,
                        draw_odds,
                        away_odds,
                        source_updated_at
                    )
                    VALUES (
                        :event_id,
                        :sport,
                        :league,
                        :home_team,
                        :away_team,
                        :event_time,
                        :home_odds,
                        :draw_odds,
                        :away_odds,
                        NULL
                    )
                    """
                ),
                {
                    "event_id": 999999999,
                    "sport": "football",
                    "league": "Premier League",
                    "home_team": "Arsenal",
                    "away_team": "Chelsea",
                    "event_time": "2026-11-09 18:00:00",
                    "home_odds": 1.95,
                    "draw_odds": 3.40,
                    "away_odds": 4.20,
                },
            )