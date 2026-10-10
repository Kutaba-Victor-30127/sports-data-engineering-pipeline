import logging
from io import BytesIO

import pandas as pd
from sqlalchemy import text

from src.aws.s3_client import read_file_from_s3
from src.database.connection import get_engine
from src.validation.event_validator import validate_events 

logger = logging.getLogger(__name__)

S3_EVENTS_KEY = "raw/events/events.csv"


def load_events():
    file_content = read_file_from_s3(S3_EVENTS_KEY)

    df = pd.read_csv(
        BytesIO(file_content),
        parse_dates=["event_time"]
    )

    logger.info(
        "Loaded %s events from S3.",
        len(df)
    )

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

        logger.info(
            "Pandas inserted %s rows.",
            inserted_rows
        )

        logger.info(
            "Rows in staging.raw_events: %s",
            count
        )

    logger.info(
        "%s events loaded into staging.raw_events.",
        len(df)
    )



if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    events = load_events()
    load_to_staging(events)