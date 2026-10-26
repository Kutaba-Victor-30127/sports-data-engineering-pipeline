import logging
import json

import pandas as pd

from src.aws.s3_client import read_file_from_s3
from src.validation.event_validator import validate_events


logger = logging.getLogger(__name__)


def load_json_events(s3_key):
    file_content = read_file_from_s3(s3_key)

    data = json.loads(
        file_content.decode("utf-8")
    )

    df = pd.json_normalize(data)

    df = df.rename(
        columns={
            "teams.home": "home_team",
            "teams.away": "away_team",
            "odds.home": "home_odds",
            "odds.draw": "draw_odds",
            "odds.away": "away_odds"
        }
    )

    df["source_updated_at"] = pd.to_datetime(
        df["source_updated_at"],
        utc=True
    )

    df["source_updated_at"] = pd.to_datetime(
        df["source_updated_at"],
        utc=True
    )

    validate_events(df)

    logger.info(
        "Loaded and normalized %s JSON events from S3.",
        len(df)
    )

    return df
    