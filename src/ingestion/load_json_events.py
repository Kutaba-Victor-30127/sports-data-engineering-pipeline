import json

import pandas as pd

from src.aws.s3_client import read_file_from_s3
from src.validation.event_validator import validate_events


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

    print("JSON events loaded and normalized successfully.")
    print(df)

    return df


if __name__ == "__main__":
    load_json_events()
    