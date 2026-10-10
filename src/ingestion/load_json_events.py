import logging
import json

import pandas as pd

from src.aws.s3_client import read_file_from_s3
from src.validation.event_validator import validate_events


logger = logging.getLogger(__name__)

S3_JSON_KEY = "raw/events/events.json"


def load_json_events():
    file_content = read_file_from_s3(S3_JSON_KEY)

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

    df["event_time"] = pd.to_datetime(df["event_time"])

    validate_events(df)

    logger.info(
        "Loaded and normalized %s JSON events from S3.",
        len(df)
    )

    return df


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    load_json_events()
    