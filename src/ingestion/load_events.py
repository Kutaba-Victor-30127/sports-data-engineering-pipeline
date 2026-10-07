from io import BytesIO

import pandas as pd
from sqlalchemy import text

from src.aws.s3_client import read_file_from_s3
from src.database.connection import get_engine
from src.validation.event_validator import validate_events 


S3_EVENTS_KEY = "raw/events/events.csv"


def load_events():
    file_content = read_file_from_s3(S3_EVENTS_KEY)

    df = pd.read_csv(
        BytesIO(file_content),
        parse_dates=["event_time"]
    )

    print("Events loaded successfully from S3.")

    validate_events(df)

    return df


def load_to_staging(df):
    engine = get_engine()

    with engine.begin() as connection:
        connection.execute(
            text("TRUNCATE TABLE staging.raw_events")
            )

        inserted_rows = df.to_sql(
            name="raw_events",
            con=connection,
            schema="staging",
            if_exists="append",
            index=False
        )

        count = connection.execute(
            text("SELECT COUNT(*) FROM staging.raw_events")
        ).scalar()

        print(f"Pandas reports inserted rows: {inserted_rows}")
        print(f"Rows inside transaction: {count}")

    print(f"{len(df)} events loaded into staging.raw_events.")



if __name__ == "__main__":
    events = load_events()
    load_to_staging(events)  